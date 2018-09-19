// Copyright (c) 2014 Baidu, Inc.G
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Authors: Lei He (helei@qiyi.com)

#include <cmath>
#include <gflags/gflags.h>
#include <bvar/bvar.h>
#include "brpc/errno.pb.h"
#include "brpc/policy/codel_concurrency_limiter.h"

namespace brpc {
namespace policy {

DEFINE_double(codel_cl_queuing_delay_threshold_ms, 0.15, 
    "It will be taken to be overloaded if the queuing delay exceeds this time");
DEFINE_double(codel_cl_discard_timeout_ms, 0.3, 
    "When the queuing delay exceeds this value, some requests are "
    "discarded directly");
DEFINE_double(codel_cl_max_queuing_delay_ms, 1.5, 
    "The maximum queuing delay that can be tolerated.");

CodelConcurrencyLimiter::CodelConcurrencyLimiter()
    : _reset_delay(true)
    , _overloaded(false) 
    , _min_delay_us(0)
    , _reset_delay_after_this_time_us(butil::cpuwide_time_us()) {
}

CodelConcurrencyLimiter* CodelConcurrencyLimiter::New(const AdaptiveMaxConcurrency&) const {
    return new (std::nothrow) CodelConcurrencyLimiter;
}

bool CodelConcurrencyLimiter::OnRequested(int current_concurrency, int64_t waiting_us) {
    return !AnalyzeLoad(waiting_us);
}

void CodelConcurrencyLimiter::OnResponded(int /*error_code*/, int64_t /*latency_us*/) {
    return;
}

int CodelConcurrencyLimiter::MaxConcurrency() {
    return 0;
}

bool CodelConcurrencyLimiter::AnalyzeLoad(uint64_t waiting_us) {
    bool ret = false;
    uint64_t now_us = butil::cpuwide_time_us();
    uint64_t min_delay_us = _min_delay_us.load(butil::memory_order_relaxed);
    
    const uint64_t interval_us = FLAGS_codel_cl_max_queuing_delay_ms * 1000;
    const uint64_t delay_threshold_us = FLAGS_codel_cl_queuing_delay_threshold_ms * 1000;
    if (now_us > _reset_delay_after_this_time_us.load(butil::memory_order_relaxed) &&
        (!_reset_delay.load(butil::memory_order_acquire) &&
         !_reset_delay.exchange(true))) {
        _reset_delay_after_this_time_us.store(now_us + interval_us, 
            butil::memory_order_relaxed);
        _overloaded.store(min_delay_us > delay_threshold_us, butil::memory_order_relaxed);
    }

    if (_reset_delay.load(butil::memory_order_acquire) &&
        _reset_delay.exchange(false)) {
        _min_delay_us.store(waiting_us);
        return false;
    } else {
        while (waiting_us < min_delay_us) {
            if (_min_delay_us.compare_exchange_weak(min_delay_us, waiting_us, 
                butil::memory_order_relaxed)) {
                break;
            }
        }
    }

    if ((_overloaded.load(butil::memory_order_relaxed) && 
            waiting_us > discard_timeout_us()) ||
        waiting_us > interval_us) {
        ret = true;
    }
    return ret;
}

uint64_t CodelConcurrencyLimiter::discard_timeout_us() const { 
    return FLAGS_codel_cl_discard_timeout_ms * 1000; 
}

}  // namespace policy
}  // namespace brpc
