#ifndef BTHREAD_RW_MUTEX_H
#define BTHREAD_RW_MUTEX_H

#include "bthread/types.h"
#include "butil/scoped_lock.h"
#include "bvar/utils/lock_timer.h"
#include "bthread/bthread.h"

__BEGIN_DECLS
// -------------------------------------------
// Functions for handling read-write locks.
// -------------------------------------------

// Initialize read-write lock `rwlock' using attributes `attr', or use
// the default values if later is NULL.
extern int bthread_rwlock_init(
    bthread_rwlock_t* __restrict rwlock, const bthread_rwlockattr_t* __restrict attr);

// Destroy read-write lock `rwlock'.
extern int bthread_rwlock_destroy(bthread_rwlock_t* rwlock);

// Acquire read lock for `rwlock'.
extern int bthread_rwlock_rdlock(bthread_rwlock_t* rwlock);

// Try to acquire read lock for `rwlock'.
extern int bthread_rwlock_tryrdlock(bthread_rwlock_t* rwlock);

// Try to acquire read lock for `rwlock' or return after specfied time.
extern int bthread_rwlock_timedrdlock(
    bthread_rwlock_t* __restrict rwlock, const struct timespec* __restrict abstime);

// Acquire write lock for `rwlock'.
extern int bthread_rwlock_wrlock(bthread_rwlock_t* rwlock);

// Try to acquire write lock for `rwlock'.
extern int bthread_rwlock_trywrlock(bthread_rwlock_t* rwlock);

// Try to acquire write lock for `rwlock' or return after specfied time.
extern int bthread_rwlock_timedwrlock(
    bthread_rwlock_t* __restrict rwlock, const struct timespec* __restrict abstime);

// Unlock `rwlock'.
extern int bthread_rwlock_unlock(bthread_rwlock_t* rwlock);

extern int bthread_rwlock_unrlock(bthread_rwlock_t* rwlock);
extern int bthread_rwlock_unwlock(bthread_rwlock_t* rwlock);

// ---------------------------------------------------
// Functions for handling read-write lock attributes.
// ---------------------------------------------------

// Initialize attribute object `attr' with default values.
extern int bthread_rwlockattr_init(bthread_rwlockattr_t* attr);

// Destroy attribute object `attr'.
extern int bthread_rwlockattr_destroy(bthread_rwlockattr_t* attr);

// Return current setting of reader/writer preference.
extern int bthread_rwlockattr_getkind_np(const bthread_rwlockattr_t* attr, int* pref);

// Set reader/write preference.
extern int bthread_rwlockattr_setkind_np(bthread_rwlockattr_t* attr, int pref);
__END_DECLS


// Specialize std::lock_guard and std::unique_lock for bthread_rwlock_t

namespace bthread {

class wlock_guard {
public:
    explicit wlock_guard(bthread_rwlock_t& mutex) : _pmutex(&mutex) {
#if !defined(NDEBUG)
        const int rc = bthread_rwlock_wrlock(_pmutex);
        if (rc) {
            LOG(FATAL) << "Fail to lock bthread_rwlock_t=" << _pmutex << ", " << berror(rc);
            _pmutex = NULL;
        }
#else
        bthread_rwlock_wrlock(_pmutex);
#endif // NDEBUG
    }

    ~wlock_guard() {
#ifndef NDEBUG
        if (_pmutex) {
            bthread_rwlock_unwlock(_pmutex);
        }
#else
        bthread_rwlock_unwlock(_pmutex);
#endif
    }

private:
    DISALLOW_COPY_AND_ASSIGN(wlock_guard);
    bthread_rwlock_t* _pmutex;
};

class rlock_guard {
public:
    explicit rlock_guard(bthread_rwlock_t& mutex) : _pmutex(&mutex) {
#if !defined(NDEBUG)
        const int rc = bthread_rwlock_rdlock(_pmutex);
        if (rc) {
            LOG(FATAL) << "Fail to lock bthread_rwlock_t=" << _pmutex << ", " << berror(rc);
            _pmutex = NULL;
        }
#else
        bthread_rwlock_rdlock(_pmutex);
#endif // NDEBUG
    }

    ~rlock_guard() {
#ifndef NDEBUG
        if (_pmutex) {
            bthread_rwlock_unrlock(_pmutex);
        }
#else
        bthread_rwlock_unrlock(_pmutex);
#endif
    }

private:
    DISALLOW_COPY_AND_ASSIGN(rlock_guard);
    bthread_rwlock_t* _pmutex;
};


} // namespace bthread


#endif