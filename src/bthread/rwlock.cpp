#include <pthread.h>
#include <execinfo.h>
#include <dlfcn.h> // dlsym
#include <fcntl.h> // O_RDONLY
#include "butil/atomicops.h"
#include "bvar/bvar.h"
#include "bvar/collector.h"
#include "butil/macros.h" // BAIDU_CASSERT
#include "butil/containers/flat_map.h"
#include "butil/iobuf.h"
#include "butil/fd_guard.h"
#include "butil/files/file.h"
#include "butil/files/file_path.h"
#include "butil/file_util.h"
#include "butil/unique_ptr.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "butil/logging.h"
#include "butil/object_pool.h"
#include "bthread/butex.h"     // butex_*
#include "bthread/processor.h" // cpu_relax, barrier
#include "bthread/bthread.h"
#include "bthread/sys_futex.h"
#include "bthread/log.h"

/*
这是一个写优先和优化读性能的rwlock实现
1，读请求发现当前有写请求的时候，不管是否已经获得写锁，读请求都要等待；
2，没有写请求竞争的时候，读锁的效率是很高的，读锁之间基本上没有太多竞争，
   只需要对一个读计数的原子变量进行加减，这样在读锁获得和释放的路径上
   连一个CAS操作都没有。
Author: hetaofirst@163.com
2020-01-17
*/

namespace bthread {
//写锁标记
const unsigned RWLOCK_WLOCKED = 1;
//读等标记
const unsigned RWLOCK_RWAIT = 2;
//由于使用unsigned原子变量的低2位作为标记，那么计数就需要偏移
const unsigned RWLOCK_SHIFT = 2;

inline int rwlock_wlock(bthread_rwlock_t* rwlock) {
    butil::atomic<unsigned>* wc_rwait = 
        (butil::atomic<unsigned>*)rwlock->wc_rwait;
    butil::atomic<unsigned>* whole = 
        (butil::atomic<unsigned>*)rwlock->rc_wlock;
    //增加写计数
    wc_rwait->fetch_add(1 << RWLOCK_SHIFT, butil::memory_order_acquire);
    for(;;) {
        unsigned r = whole->load(butil::memory_order_relaxed);
        if(r != 0) {
            // 说明此时有读锁或者写锁
            if(bthread::butex_wait(whole, r, NULL) < 0 && 
                errno != EWOULDBLOCK && errno != EINTR) {
                wc_rwait->fetch_sub(1 << RWLOCK_SHIFT, butil::memory_order_relaxed);
                LOG(ERROR) << "wlock wait error, " << r;
                return errno;
            }
        }
        //尝试获得写锁
        else if(whole->compare_exchange_weak(r, r | RWLOCK_WLOCKED, butil::memory_order_acquire)) {
            return 0;
        }
    }
}


inline int rwlock_rlock(bthread_rwlock_t* rwlock) {
    butil::atomic<unsigned>* wc_rwait = 
        (butil::atomic<unsigned>*)rwlock->wc_rwait;
    for(;;) {
        //写优先处理，如果发现当前有写请求，则等待
        unsigned w = wc_rwait->load(butil::memory_order_relaxed);
        if( (w >> RWLOCK_SHIFT) > 0) {
            //设置标记后等待
            w = wc_rwait->fetch_or(RWLOCK_RWAIT, butil::memory_order_acquire) | RWLOCK_RWAIT;
            if((w >> RWLOCK_SHIFT) > 0) {
                if(bthread::butex_wait(wc_rwait, w, NULL) &&
                    errno != EWOULDBLOCK && errno != EINTR) {
                    LOG(ERROR) << "rlock wait error1, " << w;
                    return errno;
                }
            }
        }
        else {
            break;
        }
    }
    butil::atomic<unsigned>* whole = 
        (butil::atomic<unsigned>*)rwlock->rc_wlock;
    //不考虑在读锁中使用CAS, 直接加读计数，最大化读锁性能
    //所以下面需要判断写锁标记并等待
    unsigned r = whole->fetch_add(1 << RWLOCK_SHIFT, butil::memory_order_acquire);
    if((r & RWLOCK_WLOCKED) == 0) {
        return 0;
    }
    for(;;) {
        r = whole->load(butil::memory_order_relaxed);
        if((r & RWLOCK_WLOCKED)==0) {
            return 0;
        }
        else {
            if(bthread::butex_wait(whole, r, NULL) < 0 && 
                errno != EWOULDBLOCK && errno != EINTR) {
                whole->fetch_sub(1 << RWLOCK_SHIFT, butil::memory_order_relaxed);
                LOG(ERROR) << "rlock wait error2, " << r;
                return errno;
            }
        }
    }

}

inline int rwlock_unrlock(bthread_rwlock_t* rwlock) {
    butil::atomic<unsigned>* whole = 
        (butil::atomic<unsigned>*)rwlock->rc_wlock;
    //没有写竞争的时候，fetch_sub效率严重落后load！
    //减少读锁计数
    unsigned r = whole->fetch_sub(1 << RWLOCK_SHIFT, butil::memory_order_release)
        - (1 << RWLOCK_SHIFT);
    if((r >> RWLOCK_SHIFT) == 0) {
        butil::atomic<unsigned>* wc_rwait = 
            (butil::atomic<unsigned>*)rwlock->wc_rwait;
        if(wc_rwait->load(butil::memory_order_relaxed) > 0) {
            //写计数大于0，才可能是有写等待
            //unlock读锁只需要考虑唤醒写等
            bthread::butex_wake(whole);
        }
    }
    return 0;
}


inline int rwlock_unwlock(bthread_rwlock_t* rwlock) {
    butil::atomic<unsigned>* whole = 
        (butil::atomic<unsigned>*)rwlock->rc_wlock;
    //清除写锁标记
    unsigned r = whole->fetch_and(~RWLOCK_WLOCKED, butil::memory_order_release);

    butil::atomic<unsigned>* wc_rwait = 
        (butil::atomic<unsigned>*)rwlock->wc_rwait;
    //减少写计数
    unsigned w = wc_rwait->fetch_sub(1 << RWLOCK_SHIFT, butil::memory_order_release)
        - (1 << RWLOCK_SHIFT);
    //是否需要唤醒whole
    //有读等或者写等
    if((r >> RWLOCK_SHIFT) != 0) {
        //此处的读等优先唤醒，会卡住其他读写
        bthread::butex_wake_all(whole);
    }
    else if((w >> RWLOCK_SHIFT) != 0) {
        //还有其他写等，唤醒一个即可
        bthread::butex_wake(whole);
    }

    //check是否需要唤醒第一处的读等
    if((w >> RWLOCK_SHIFT) == 0 && (w & RWLOCK_RWAIT) != 0) {
        //有读等,清空标记后唤醒
        wc_rwait->fetch_and(~RWLOCK_RWAIT, butil::memory_order_relaxed);
        bthread::butex_wake_all(wc_rwait);
    }
    return 0;
}


inline int rwlock_unlock(bthread_rwlock_t* rwlock) {
    //判断写标记即可
    butil::atomic<unsigned>* whole = 
        (butil::atomic<unsigned>*)rwlock->rc_wlock;
    if ((whole->load(butil::memory_order_relaxed) & RWLOCK_WLOCKED) != 0) {
        return rwlock_unwlock(rwlock);
    } else {
        return rwlock_unrlock(rwlock);
    }
}


} // namespace bthread

extern "C" {

int bthread_rwlock_init(bthread_rwlock_t* __restrict rwlock,
                        const bthread_rwlockattr_t* __restrict attr) {
    rwlock->wc_rwait = bthread::butex_create_checked<unsigned>();
    rwlock->rc_wlock = bthread::butex_create_checked<unsigned>();
    if (!rwlock->wc_rwait || !rwlock->rc_wlock) {
        LOG(ERROR) << "no memory";
        return ENOMEM;
    }
    *rwlock->wc_rwait = 0;
    *rwlock->rc_wlock = 0;
    return 0;
}

int bthread_rwlock_destroy(bthread_rwlock_t* rwlock) {
    bthread::butex_destroy(rwlock->wc_rwait);
    bthread::butex_destroy(rwlock->rc_wlock);
    return 0;
}

int bthread_rwlock_rdlock(bthread_rwlock_t* rwlock) { return bthread::rwlock_rlock(rwlock); }

int bthread_rwlock_wrlock(bthread_rwlock_t* rwlock) { return bthread::rwlock_wlock(rwlock); }

int bthread_rwlock_unrlock(bthread_rwlock_t* rwlock) { return bthread::rwlock_unrlock(rwlock); }

int bthread_rwlock_unwlock(bthread_rwlock_t* rwlock) { return bthread::rwlock_unwlock(rwlock); }

int bthread_rwlock_unlock(bthread_rwlock_t* rwlock) { return bthread::rwlock_unlock(rwlock); }
}