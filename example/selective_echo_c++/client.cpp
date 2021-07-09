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

// A client sending requests to server in parallel by multiple threads.

#include <gflags/gflags.h>
#include <bthread/bthread.h>
#include <butil/logging.h>
#include <brpc/selective_channel.h>
#include <brpc/parallel_channel.h>
#include "echo.pb.h"

DEFINE_int32(thread_num, 50, "Number of threads to send requests");
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(attachment_size, 0, "Carry so many byte attachment along with requests");
DEFINE_int32(request_size, 16, "Bytes of each request");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(starting_server, "0.0.0.0:8114", "IP Address of the first server, port of i-th server is `first-port + i'");
DEFINE_string(load_balancer, "rr", "Name of load balancer");
DEFINE_string(sub_load_balancer, "rr", "Name of load balancer used in sub channel");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(backup_ms, -1, "backup timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_bool(dont_fail, false, "Print fatal when some call failed");
DEFINE_uint64(request_code, 0, "The request code");

std::string g_request;
std::string g_attachment;

bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_count("client_error_count");

static void* sender(void* arg) {
    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    example::EchoService_Stub stub(static_cast<google::protobuf::RpcChannel*>(arg));

    int log_id = 0;
    while (!brpc::IsAskedToQuit()) {
        // We will receive response synchronously, safe to put variables
        // on stack.
        example::EchoRequest request;
        example::EchoResponse response;
        brpc::Controller cntl;

        request.set_message(g_request);
        cntl.set_log_id(log_id++);  // set by user
        cntl.set_request_code(FLAGS_request_code);

        if (!g_attachment.empty()) {
            // Set attachment which is wired to network directly instead of 
            // being serialized into protobuf messages.
            cntl.request_attachment().append(g_attachment);
        }

        // Because `done'(last parameter) is NULL, this function waits until
        // the response comes back or error occurs(including timedout).
        stub.Echo(&cntl, &request, &response, NULL);
        const int64_t elp = cntl.latency_us();
        if (!cntl.Failed()) {
            g_latency_recorder << cntl.latency_us();
        } else {
            g_error_count << 1; 
            CHECK(brpc::IsAskedToQuit() || !FLAGS_dont_fail)
                << "error=" << cntl.ErrorText() << " latency=" << elp;
            // We can't connect to the server, sleep a while. Notice that this
            // is a specific sleeping to prevent this thread from spinning too // fast. You should continue the business logic in a production 
            // server rather than sleeping.
            bthread_usleep(50000);
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // A Channel represents a communication line to a Server. Notice that 
    // Channel is thread-safe and can be shared by all threads in your program.
    brpc::SelectiveChannel channel;
    brpc::ChannelOptions schan_options;
    schan_options.timeout_ms = FLAGS_timeout_ms;
    schan_options.backup_request_ms = FLAGS_backup_ms;
    schan_options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_load_balancer.c_str(), &schan_options) != 0) {
        LOG(ERROR) << "Fail to init SelectiveChannel";
        return -1;
    }

    // Add sub channels.
    // ================
    std::vector<brpc::ChannelBase*> sub_channels;
    
    // Add an ordinary channel.
    brpc::Channel* sub_channel1 = new brpc::Channel;
    butil::EndPoint pt;
    if (str2endpoint(FLAGS_starting_server.c_str(), &pt) != 0 &&
        hostname2endpoint(FLAGS_starting_server.c_str(), &pt) != 0) {
        LOG(ERROR) << "Invalid address=`" << FLAGS_starting_server << "'";
        return -1;
    }
    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    std::ostringstream os;
    os << "list://";
    for (int i = 0; i < 3; ++i) {
        os << butil::EndPoint(pt.ip, pt.port++) << ",";
    }
    if (sub_channel1->Init(os.str().c_str(), FLAGS_sub_load_balancer.c_str(),
                           &options) != 0) {
        LOG(ERROR) << "Fail to init ordinary channel";
        return -1;
    }
    sub_channels.push_back(sub_channel1);

    // Add a parallel channel.
    brpc::ParallelChannel* sub_channel2 = new brpc::ParallelChannel;
    brpc::ParallelChannelOptions pchan_options;
    pchan_options.fail_limit = 1;
    if (sub_channel2->Init(&pchan_options) != 0) {
        LOG(ERROR) << "Fail to init sub_channel2";
        return -1;
    }
    for (int i = 0; i < 3; ++i) {
        brpc::ChannelOptions options;
        options.protocol = FLAGS_protocol;
        options.connection_type = FLAGS_connection_type;
        brpc::Channel* c = new brpc::Channel;
        if (c->Init(butil::EndPoint(pt.ip, pt.port++), &options) != 0) {
            LOG(ERROR) << "Fail to init sub channel[" << i << "] of pchan";
            return -1;
        }
        if (sub_channel2->AddChannel(c, brpc::OWNS_CHANNEL, NULL, NULL) != 0) {
            LOG(ERROR) << "Fail to add sub channel[" << i << "] into pchan";
            return -1;
        }
    }
    sub_channels.push_back(sub_channel2);

    // Add another selective channel with default options.
    brpc::SelectiveChannel* sub_channel3 = new brpc::SelectiveChannel;
    if (sub_channel3->Init(FLAGS_sub_load_balancer.c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init schan";
        return -1;
    }
    for (int i = 0; i < 3; ++i) {
        brpc::Channel* c = new brpc::Channel;
        if (i == 0) {
            os.str("");
            os << "list://";
            for (int j = 0; j < 3; ++j) {
                os << butil::EndPoint(pt.ip, pt.port++) << ",";
            }
            if (c->Init(os.str().c_str(), FLAGS_sub_load_balancer.c_str(),
                        &options) != 0) {
                LOG(ERROR) << "Fail to init sub channel[" << i << "] of schan";
                return -1;
            }
        } else {
            if (c->Init(butil::EndPoint(pt.ip, pt.port++), &options) != 0) {
                LOG(ERROR) << "Fail to init sub channel[" << i << "] of schan";
                return -1;
            }
        }
        if (sub_channel3->AddChannel(c, NULL)) {
            LOG(ERROR) << "Fail to add sub channel[" << i << "] into schan";
            return -1;
        }
    }
    sub_channels.push_back(sub_channel3);

    // Add all sub channels into schan.
    for (size_t i = 0; i < sub_channels.size(); ++i) {
        // note: we don't need the handle for channel removal;
        if (channel.AddChannel(sub_channels[i], NULL/*note*/) != 0) {
            LOG(ERROR) << "Fail to add sub_channel[" << i << "]";
            return -1;
        }
    }
    if (FLAGS_attachment_size > 0) {
        g_attachment.resize(FLAGS_attachment_size, 'a');
    }
    if (FLAGS_request_size <= 0) {
        LOG(ERROR) << "Bad request_size=" << FLAGS_request_size;
        return -1;
    }
    g_request.resize(FLAGS_request_size, 'r');

    std::vector<bthread_t> bids;
    std::vector<pthread_t> pids;
    if (!FLAGS_use_bthread) {
        pids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&pids[i], NULL, sender, &channel) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        bids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(
                    &bids[i], NULL, sender, &channel) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
        LOG(INFO) << "Sending EchoRequest at qps=" << g_latency_recorder.qps(1)
                  << " latency=" << g_latency_recorder.latency(1);
    }

    LOG(INFO) << "EchoClient is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(pids[i], NULL);
        } else {
            bthread_join(bids[i], NULL);
        }
    }

    return 0;
}
