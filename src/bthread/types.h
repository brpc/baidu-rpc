// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// bthread - A M:N threading library to make applications more concurrent.

// Date: Tue Jul 10 17:40:58 CST 2012

#ifndef BTHREAD_TYPES_H
#define BTHREAD_TYPES_H

#include <stdint.h>                            // uint64_t
#if defined(__cplusplus)
#include "butil/logging.h"                      // CHECK
#endif

typedef uint64_t bthread_t;

// tid returned by bthread_start_* never equals this value.
static const bthread_t INVALID_BTHREAD = 0;

struct sockaddr;

typedef unsigned bthread_stacktype_t;
static const bthread_stacktype_t BTHREAD_STACKTYPE_UNKNOWN = 0;
static const bthread_stacktype_t BTHREAD_STACKTYPE_PTHREAD = 1;
static const bthread_stacktype_t BTHREAD_STACKTYPE_SMALL = 2;
static const bthread_stacktype_t BTHREAD_STACKTYPE_NORMAL = 3;
static const bthread_stacktype_t BTHREAD_STACKTYPE_LARGE = 4;

typedef unsigned bthread_attrflags_t;
static const bthread_attrflags_t BTHREAD_LOG_START_AND_FINISH = 8;
static const bthread_attrflags_t BTHREAD_LOG_CONTEXT_SWITCH = 16;
static const bthread_attrflags_t BTHREAD_NOSIGNAL = 32;

// Key of thread-local data, created by bthread_key_create.
typedef struct {
    uint32_t index;    // index in KeyTable
    uint32_t version;  // ABA avoidance
} bthread_key_t;

static const bthread_key_t INVALID_BTHREAD_KEY = { 0, 0 };

#if defined(__cplusplus)
// Overload operators for bthread_key_t
inline bool operator==(bthread_key_t key1, bthread_key_t key2)
{ return key1.index == key2.index && key1.version == key2.version; }
inline bool operator!=(bthread_key_t key1, bthread_key_t key2)
{ return !(key1 == key2); }
inline bool operator<(bthread_key_t key1, bthread_key_t key2) {
    return key1.index != key2.index ? (key1.index < key2.index) :
        (key1.version < key2.version);
}
inline bool operator>(bthread_key_t key1, bthread_key_t key2)
{ return key2 < key1; }
inline bool operator<=(bthread_key_t key1, bthread_key_t key2)
{ return !(key2 < key1); }
inline bool operator>=(bthread_key_t key1, bthread_key_t key2)
{ return !(key1 < key2); }
inline std::ostream& operator<<(std::ostream& os, bthread_key_t key) {
    return os << "bthread_key_t{index=" << key.index << " version="
              << key.version << '}';
}
#endif  // __cplusplus

typedef struct {
    pthread_mutex_t mutex;
    void* free_keytables;
    int destroyed;
} bthread_keytable_pool_t;

typedef struct {
    size_t nfree;
} bthread_keytable_pool_stat_t;

// Attributes for thread creation.
typedef struct bthread_attr_t {
    bthread_stacktype_t stack_type;
    bthread_attrflags_t flags;
    bthread_keytable_pool_t* keytable_pool;

#if defined(__cplusplus)
    void operator=(unsigned stacktype_and_flags) {
        stack_type = (stacktype_and_flags & 7);
        flags = (stacktype_and_flags & ~(unsigned)7u);
        keytable_pool = NULL;
    }
    bthread_attr_t operator|(unsigned other_flags) const {
        CHECK(!(other_flags & 7)) << "flags=" << other_flags;
        bthread_attr_t tmp = *this;
        tmp.flags |= (other_flags & ~(unsigned)7u);
        return tmp;
    }
#endif  // __cplusplus
} bthread_attr_t;

// bthreads started with this attribute will run on stack of worker pthread and
// all bthread functions that would block the bthread will block the pthread.
// The bthread will not allocate its own stack, simply occupying a little meta
// memory. This is required to run JNI code which checks layout of stack. The
// obvious drawback is that you need more worker pthreads when you have a lot
// of such bthreads.
static const bthread_attr_t BTHREAD_ATTR_PTHREAD =
{ BTHREAD_STACKTYPE_PTHREAD, 0, NULL };

// bthreads created with following attributes will have different size of
// stacks. Default is BTHREAD_ATTR_NORMAL.
static const bthread_attr_t BTHREAD_ATTR_SMALL =
{ BTHREAD_STACKTYPE_SMALL, 0, NULL };
static const bthread_attr_t BTHREAD_ATTR_NORMAL =
{ BTHREAD_STACKTYPE_NORMAL, 0, NULL };
static const bthread_attr_t BTHREAD_ATTR_LARGE =
{ BTHREAD_STACKTYPE_LARGE, 0, NULL };

// bthreads created with this attribute will print log when it's started,
// context-switched, finished.
static const bthread_attr_t BTHREAD_ATTR_DEBUG = {
    BTHREAD_STACKTYPE_NORMAL,
    BTHREAD_LOG_START_AND_FINISH | BTHREAD_LOG_CONTEXT_SWITCH,
    NULL
};

static const size_t BTHREAD_EPOLL_THREAD_NUM = 1;
static const bthread_t BTHREAD_ATOMIC_INIT = 0;

// Min/Max number of work pthreads.
static const int BTHREAD_MIN_CONCURRENCY = 3 + BTHREAD_EPOLL_THREAD_NUM;
static const int BTHREAD_MAX_CONCURRENCY = 1024;

typedef struct {
    void* impl;
    // following fields are part of previous impl. and not used right now.
    // Don't remove them to break ABI compatibility.
    unsigned head;
    unsigned size;
    unsigned conflict_head;
    unsigned conflict_size;
} bthread_list_t;

// TODO: bthread_contention_site_t should be put into butex.
typedef struct {
    int64_t duration_ns;
    size_t sampling_range;
} bthread_contention_site_t;

typedef struct {
    unsigned* butex;
    bthread_contention_site_t csite;
} bthread_mutex_t;

typedef struct {
} bthread_mutexattr_t;

typedef struct {
    bthread_mutex_t* m;
    int* seq;
} bthread_cond_t;

typedef struct {
} bthread_condattr_t;

typedef struct {
    //高30位读计数和低2位写锁标记
    //加写锁标记的时候要求读计数为0，
    //获得读锁的时候要加读计数且写锁标记为0
    //这些都要在一个原子操作中完成
    unsigned* rc_wlock;
    //高30位写计数和低2位读等待标记
    //读请求看到写计数大于0，则需要等待
    //写锁释放的时候，看到读等标记需要唤醒
    unsigned* wc_rwait;
} bthread_rwlock_t;

typedef struct {
} bthread_rwlockattr_t;

typedef struct {
    unsigned int count;
} bthread_barrier_t;

typedef struct {
} bthread_barrierattr_t;

typedef struct {
    uint64_t value;
} bthread_id_t;

// bthread_id returned by bthread_id_create* can never be this value.
// NOTE: don't confuse with INVALID_BTHREAD!
static const bthread_id_t INVALID_BTHREAD_ID = {0};

#if defined(__cplusplus)
// Overload operators for bthread_id_t
inline bool operator==(bthread_id_t id1, bthread_id_t id2)
{ return id1.value == id2.value; }
inline bool operator!=(bthread_id_t id1, bthread_id_t id2)
{ return !(id1 == id2); }
inline bool operator<(bthread_id_t id1, bthread_id_t id2)
{ return id1.value < id2.value; }
inline bool operator>(bthread_id_t id1, bthread_id_t id2)
{ return id2 < id1; }
inline bool operator<=(bthread_id_t id1, bthread_id_t id2)
{ return !(id2 < id1); }
inline bool operator>=(bthread_id_t id1, bthread_id_t id2)
{ return !(id1 < id2); }
inline std::ostream& operator<<(std::ostream& os, bthread_id_t id)
{ return os << id.value; }
#endif  // __cplusplus

typedef struct {
    void* impl;
    // following fields are part of previous impl. and not used right now.
    // Don't remove them to break ABI compatibility.
    unsigned head;
    unsigned size;
    unsigned conflict_head;
    unsigned conflict_size;
} bthread_id_list_t;

typedef uint64_t bthread_timer_t;

#endif  // BTHREAD_TYPES_H
