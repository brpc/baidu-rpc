// Copyright (c) 2020 Bigo, Inc.
// Author: HeTao (hetao@bigo.sg)
// Date: Jan 06 2020

#include <gtest/gtest.h>
#include "butil/compat.h"
#include "butil/time.h"
#include "butil/macros.h"
#include "butil/string_printf.h"
#include "butil/logging.h"
#include "bthread/bthread.h"
#include "bthread/butex.h"
#include "bthread/task_control.h"
#include "butil/gperftools_profiler.h"

#include <stdlib.h>

namespace {

TEST(RwlockTest, sanity) {
    bthread_rwlock_t m;
    ASSERT_EQ(0, bthread_rwlock_init(&m, NULL));
    ASSERT_EQ(0, bthread_rwlock_rdlock(&m));
    ASSERT_EQ(0, bthread_rwlock_unrlock(&m));
    ASSERT_EQ(0, bthread_rwlock_wrlock(&m));
    ASSERT_EQ(0, bthread_rwlock_unwlock(&m));
    ASSERT_EQ(0, *(m.rc_wlock));
    ASSERT_EQ(0, *(m.wc_rwait));
    ASSERT_EQ(0, bthread_rwlock_destroy(&m));
}



bool g_started = false;
bool g_stopped = false;

template <typename Rwlock>
struct BAIDU_CACHELINE_ALIGNMENT PerfArgs {
    Rwlock* rwlock;
    int64_t counter;
    int64_t elapse_ns;
    bool ready;
    int32_t op_type;   /*0 for read,1 for write*/

    PerfArgs() : rwlock(NULL), counter(0), elapse_ns(0), ready(false), op_type(0) {}
};

template <typename Rwlock>
void* add_with_rwlock(void* void_arg) {
    PerfArgs<Rwlock>* args = (PerfArgs<Rwlock>*)void_arg;
    args->ready = true;
    butil::Timer t;
    while (!g_stopped) {
        if (g_started) {
            break;
        }
        bthread_usleep(1000);
    }
    t.start();
    while (!g_stopped) {
        if(args->op_type == 0) {
            // args->rwlock->Rlock();
            bthread_rwlock_rdlock(args->rwlock);
        }
        else {
            // args->rwlock->Wlock();
            bthread_rwlock_wrlock(args->rwlock);
            }
        // args->rwlock->Unlock();
        bthread_rwlock_unlock(args->rwlock);
        ++args->counter;
    }
    t.stop();
    args->elapse_ns = t.n_elapsed();
    return NULL;
}

int g_prof_name_counter = 0;

template <typename Rwlock, typename ThreadId,
         typename ThreadCreateFn, typename ThreadJoinFn>
         void PerfTest(Rwlock* rwlock,
                 ThreadId* /*dummy*/,
                 int thread_num,
                 const ThreadCreateFn& create_fn,
                 const ThreadJoinFn& join_fn,
                 int op_type=0 /*0 for read,1 for write*/) {
    g_started = false;
    g_stopped = false;
    ThreadId threads[thread_num];
    std::vector<PerfArgs<Rwlock> > args(thread_num);
    for (int i = 0; i < thread_num; ++i) {
        args[i].rwlock = rwlock;
        args[i].op_type = op_type;
        create_fn(&threads[i], NULL, add_with_rwlock<Rwlock>, &args[i]);
    }
    while (true) {
        bool all_ready = true;
        for (int i = 0; i < thread_num; ++i) {
            if (!args[i].ready) {
                all_ready = false;
                break;
            }
        }
        if (all_ready) {
            break;
        }
        usleep(1000);
    }
    g_started = true;
    char prof_name[32];
    snprintf(prof_name, sizeof(prof_name), "rdlock_perf_%d.prof", ++g_prof_name_counter);
    ProfilerStart(prof_name);
    usleep(500 * 1000);
    ProfilerStop();
    g_stopped = true;
    int64_t wait_time = 0;
    int64_t count = 0;
    for (int i = 0; i < thread_num; ++i) {
        join_fn(threads[i], NULL);
        wait_time += args[i].elapse_ns;
        count += args[i].counter;
    }
    LOG(INFO) << butil::class_name<Rwlock>() << (op_type==0?" readlock ":" writelock ") << " in "
        << ((void*)create_fn == (void*)pthread_create ? "pthread" : "bthread")
        << " thread_num=" << thread_num
        << " count=" << count
        << " average_time=" << wait_time / (double)count;
}


TEST(RWLockTest, performance) {
    const int thread_num = 12;
    bthread_rwlock_t brw;
    bthread_rwlock_init(&brw, NULL);
    //rlock
    PerfTest(&brw, (pthread_t*)NULL, thread_num, pthread_create, pthread_join);
    PerfTest(&brw, (bthread_t*)NULL, thread_num, bthread_start_background, bthread_join);

    //add test 1 rlock for compare
    PerfTest(&brw, (pthread_t*)NULL, 1, pthread_create, pthread_join);
    PerfTest(&brw, (bthread_t*)NULL, 1, bthread_start_background, bthread_join);

    //for wlock
    PerfTest(&brw, (pthread_t*)NULL, thread_num, pthread_create, pthread_join, 1);
    PerfTest(&brw, (bthread_t*)NULL, thread_num, bthread_start_background, bthread_join, 1);

    //add test 1 wlock for compare
    PerfTest(&brw, (pthread_t*)NULL, 1, pthread_create, pthread_join, 1);
    PerfTest(&brw, (bthread_t*)NULL, 1, bthread_start_background, bthread_join, 1);
}

void* loop_until_stopped(void* arg) {
    bthread_rwlock_t *m = (bthread_rwlock_t*)arg;
    while (!g_stopped) {
        int r = rand() % 100;
        if((r&1)==0)
        {
            bthread::rlock_guard rg(*m);
        }
        else{
            bthread::wlock_guard wg(*m);
        }
        bthread_usleep(20);
    }
    return NULL;
}

TEST(RwlockTest, mix_thread_types) {
    g_stopped = false;
    const int N = 16;
    const int M = N * 2;
    // bthread::Mutex m;
    bthread_rwlock_t brw;
    bthread_rwlock_init(&brw, NULL);

    pthread_t pthreads[N];
    bthread_t bthreads[M];
    // reserve enough workers for test. This is a must since we have
    // BTHREAD_ATTR_PTHREAD bthreads which may cause deadlocks (the
    // bhtread_usleep below can't be scheduled and g_stopped is never
    // true, thus loop_until_stopped spins forever)
    bthread_setconcurrency(M);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(0, pthread_create(&pthreads[i], NULL, loop_until_stopped, &brw));
    }
    for (int i = 0; i < M; ++i) {
        const bthread_attr_t *attr = i % 2 ? NULL : &BTHREAD_ATTR_PTHREAD;
        ASSERT_EQ(0, bthread_start_urgent(&bthreads[i], attr, loop_until_stopped, &brw));
    }
    bthread_usleep(1000L * 1000);
    g_stopped = true;
    for (int i = 0; i < M; ++i) {
        bthread_join(bthreads[i], NULL);
    }
    for (int i = 0; i < N; ++i) {
        pthread_join(pthreads[i], NULL);
    }
}
} // namespace
