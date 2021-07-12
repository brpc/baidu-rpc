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

// brpc - A framework to host and access services throughout Baidu.

// Date: Sun Jul 13 15:04:18 CST 2014

#include <sys/types.h>
#include <sys/socket.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <google/protobuf/descriptor.h>
#include "butil/time.h"
#include "butil/macros.h"
#include "butil/logging.h"
#include "butil/files/temp_file.h"
#include "brpc/socket.h"
#include "brpc/acceptor.h"
#include "brpc/server.h"
#include "brpc/policy/baidu_rpc_protocol.h"
#include "brpc/policy/baidu_rpc_meta.pb.h"
#include "brpc/policy/most_common_message.h"
#include "brpc/channel.h"
#include "brpc/details/load_balancer_with_naming.h"
#include "brpc/parallel_channel.h"
#include "brpc/selective_channel.h"
#include "brpc/socket_map.h"
#include "brpc/controller.h"
#include "echo.pb.h"
#include "brpc/options.pb.h"

namespace brpc {
DECLARE_int32(idle_timeout_second);
DECLARE_int32(max_connection_pool_size);
class Server;
class MethodStatus;
namespace policy {
void SendRpcResponse(int64_t correlation_id, Controller* cntl, 
                     const google::protobuf::Message* req,
                     const google::protobuf::Message* res,
                     const Server* server_raw, MethodStatus *, int64_t);
} // policy
} // brpc

int main(int argc, char* argv[]) {
    brpc::FLAGS_idle_timeout_second = 0;
    brpc::FLAGS_max_connection_pool_size = 0;
    testing::InitGoogleTest(&argc, argv);
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}

namespace {
void* RunClosure(void* arg) {
    google::protobuf::Closure* done = (google::protobuf::Closure*)arg;
    done->Run();
    return NULL;
}

class DeleteOnlyOnceChannel : public brpc::Channel {
public:
    DeleteOnlyOnceChannel() : _c(1) {
    }
    ~DeleteOnlyOnceChannel() {
        if (_c.fetch_sub(1) != 1) {
            LOG(ERROR) << "Delete more than once!";
            abort();
        }
    }
private:
    butil::atomic<int> _c;
};

static std::string MOCK_CREDENTIAL = "mock credential";
static std::string MOCK_CONTEXT = "mock context";

class MyAuthenticator : public brpc::Authenticator {
public:
    MyAuthenticator() : count(0) {}

    int GenerateCredential(std::string* auth_str) const {
        *auth_str = MOCK_CREDENTIAL;
        count.fetch_add(1, butil::memory_order_relaxed);
        return 0;
    }

    int VerifyCredential(const std::string&,
                         const butil::EndPoint&,
                         brpc::AuthContext* ctx) const {
        ctx->set_user(MOCK_CONTEXT);
        ctx->set_group(MOCK_CONTEXT);
        ctx->set_roles(MOCK_CONTEXT);
        ctx->set_starter(MOCK_CONTEXT);
        ctx->set_is_service(true);
        return 0;
    }
    mutable butil::atomic<int32_t> count;
};

static bool VerifyMyRequest(const brpc::InputMessageBase* msg_base) {
    const brpc::policy::MostCommonMessage* msg = 
        static_cast<const brpc::policy::MostCommonMessage*>(msg_base);
    brpc::Socket* ptr = msg->socket();
    
    brpc::policy::RpcMeta meta;
    butil::IOBufAsZeroCopyInputStream wrapper(msg->meta);
    EXPECT_TRUE(meta.ParseFromZeroCopyStream(&wrapper));

    if (meta.has_authentication_data()) {
        // Credential MUST only appear in the first packet
        EXPECT_TRUE(NULL == ptr->auth_context());
        EXPECT_EQ(meta.authentication_data(), MOCK_CREDENTIAL);
        MyAuthenticator authenticator;
        return authenticator.VerifyCredential(
            "", butil::EndPoint(), ptr->mutable_auth_context()) == 0;
    }
    return true;
}

class MyEchoService : public ::test::EchoService {
    void Echo(google::protobuf::RpcController* cntl_base,
              const ::test::EchoRequest* req,
              ::test::EchoResponse* res,
              google::protobuf::Closure* done) {
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);
        brpc::ClosureGuard done_guard(done);
        if (req->server_fail()) {
            cntl->SetFailed(req->server_fail(), "Server fail1");
            cntl->SetFailed(req->server_fail(), "Server fail2");
            return;
        }
        if (req->close_fd()) {
            LOG(INFO) << "close fd...";
            cntl->CloseConnection("Close connection according to request");
            return;
        }
        if (req->sleep_us() > 0) {
            LOG(INFO) << "sleep " << req->sleep_us() << "us...";
            bthread_usleep(req->sleep_us());
        }
        res->set_message("received " + req->message());
        if (req->code() != 0) {
            res->add_code_list(req->code());
        }
        res->set_receiving_socket_id(cntl->_current_call.sending_sock->id());
    }
};

pthread_once_t register_mock_protocol = PTHREAD_ONCE_INIT;

class ChannelTest : public ::testing::Test{
protected:
    ChannelTest() 
        : _ep(butil::IP_ANY, 8787)
        , _close_fd_once(false) {
        pthread_once(&register_mock_protocol, register_protocol);
        const brpc::InputMessageHandler pairs[] = {
            { brpc::policy::ParseRpcMessage, 
              ProcessRpcRequest, VerifyMyRequest, this, "baidu_std" }
        };
        EXPECT_EQ(0, _messenger.AddHandler(pairs[0]));

        EXPECT_EQ(0, _server_list.save(butil::endpoint2str(_ep).c_str()));           
        _naming_url = std::string("File://") + _server_list.fname();
    };

    virtual ~ChannelTest(){};
    virtual void SetUp() {
    };
    virtual void TearDown() {
        StopAndJoin();
    };

    static void register_protocol() {
        brpc::Protocol dummy_protocol = 
                                 { brpc::policy::ParseRpcMessage,
                                   brpc::SerializeRequestDefault, 
                                   brpc::policy::PackRpcRequest,
                                   NULL, ProcessRpcRequest,
                                   VerifyMyRequest, NULL, NULL,
                                   brpc::CONNECTION_TYPE_ALL, "baidu_std" };
        ASSERT_EQ(0,  RegisterProtocol((brpc::ProtocolType)30, dummy_protocol));
    }

    static void ProcessRpcRequest(brpc::InputMessageBase* msg_base) {
        brpc::DestroyingPtr<brpc::policy::MostCommonMessage> msg(
            static_cast<brpc::policy::MostCommonMessage*>(msg_base));
        brpc::SocketUniquePtr ptr(msg->ReleaseSocket());
        const brpc::AuthContext* auth = ptr->auth_context();
        if (auth) {
            EXPECT_EQ(MOCK_CONTEXT, auth->user());
            EXPECT_EQ(MOCK_CONTEXT, auth->group());
            EXPECT_EQ(MOCK_CONTEXT, auth->roles());
            EXPECT_EQ(MOCK_CONTEXT, auth->starter());
            EXPECT_TRUE(auth->is_service());
        }
        ChannelTest* ts = (ChannelTest*)msg_base->arg();
        if (ts->_close_fd_once) {
            ts->_close_fd_once = false;
            ptr->SetFailed();
            return;
        }
        
        brpc::policy::RpcMeta meta;
        butil::IOBufAsZeroCopyInputStream wrapper(msg->meta);
        EXPECT_TRUE(meta.ParseFromZeroCopyStream(&wrapper));
        const brpc::policy::RpcRequestMeta& req_meta = meta.request();
        ASSERT_EQ(ts->_svc.descriptor()->full_name(), req_meta.service_name());
        const google::protobuf::MethodDescriptor* method =
            ts->_svc.descriptor()->FindMethodByName(req_meta.method_name());
        google::protobuf::Message* req =
              ts->_svc.GetRequestPrototype(method).New();
        if (meta.attachment_size() != 0) {
            butil::IOBuf req_buf;
            msg->payload.cutn(&req_buf, msg->payload.size() - meta.attachment_size());
            butil::IOBufAsZeroCopyInputStream wrapper2(req_buf);
            EXPECT_TRUE(req->ParseFromZeroCopyStream(&wrapper2));
        } else {
            butil::IOBufAsZeroCopyInputStream wrapper2(msg->payload);
            EXPECT_TRUE(req->ParseFromZeroCopyStream(&wrapper2));
        }
        brpc::Controller* cntl = new brpc::Controller();
        cntl->_current_call.peer_id = ptr->id();
        cntl->_current_call.sending_sock.reset(ptr.release());
        cntl->_server = &ts->_dummy;

        google::protobuf::Message* res =
              ts->_svc.GetResponsePrototype(method).New();
        google::protobuf::Closure* done =
              brpc::NewCallback<
            int64_t, brpc::Controller*,
            const google::protobuf::Message*,
            const google::protobuf::Message*,
            const brpc::Server*,
            brpc::MethodStatus*, int64_t>(
                &brpc::policy::SendRpcResponse,
                meta.correlation_id(), cntl, NULL, res,
                &ts->_dummy, NULL, -1);
        ts->_svc.CallMethod(method, cntl, req, res, done);
    }

    int StartAccept(butil::EndPoint ep) {
        int listening_fd = -1;
        while ((listening_fd = tcp_listen(ep)) < 0) {
            if (errno == EADDRINUSE) {
                bthread_usleep(1000);
            } else {
                return -1;
            }
        }
        if (_messenger.StartAccept(listening_fd, -1, NULL) != 0) {
            return -1;
        }
        return 0;
    }

    void StopAndJoin() {
        _messenger.StopAccept(0);
        _messenger.Join();
    }

    void SetUpChannel(brpc::Channel* channel, 
                      bool single_server,
                      bool short_connection,
                      const brpc::Authenticator* auth = NULL,
                      std::string connection_group = std::string()) {
        brpc::ChannelOptions opt;
        if (short_connection) {
            opt.connection_type = brpc::CONNECTION_TYPE_SHORT;
        }
        opt.auth = auth;
        opt.max_retry = 0;
        opt.connection_group = connection_group;
        if (single_server) {
            EXPECT_EQ(0, channel->Init(_ep, &opt)); 
        } else {                                                 
            EXPECT_EQ(0, channel->Init(_naming_url.c_str(), "rR", &opt));
        }                                         
    }
    
    void CallMethod(brpc::ChannelBase* channel, 
                    brpc::Controller* cntl,
                    test::EchoRequest* req, test::EchoResponse* res,
                    bool async, bool destroy = false) {
        google::protobuf::Closure* done = NULL;                     
        brpc::CallId sync_id = { 0 };
        if (async) {
            sync_id = cntl->call_id();
            done = brpc::DoNothing();
        }
        ::test::EchoService::Stub(channel).Echo(cntl, req, res, done);
        if (async) {
            if (destroy) {
                delete channel;
            }
            // Callback MUST be called for once and only once
            bthread_id_join(sync_id);
        }
    }

    void CallMethod(brpc::ChannelBase* channel, 
                    brpc::Controller* cntl,
                    test::ComboRequest* req, test::ComboResponse* res,
                    bool async, bool destroy = false) {
        google::protobuf::Closure* done = NULL;
        brpc::CallId sync_id = { 0 };
        if (async) {
            sync_id = cntl->call_id();
            done = brpc::DoNothing();
        }
        ::test::EchoService::Stub(channel).ComboEcho(cntl, req, res, done);
        if (async) {
            if (destroy) {
                delete channel;
            }
            // Callback MUST be called for once and only once
            bthread_id_join(sync_id);
        }
    }

    void TestConnectionFailed(bool single_server, bool async, 
                              bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(ECONNREFUSED, cntl.ErrorCode()) << cntl.ErrorText();
    }
    
    void TestConnectionFailedParallel(bool single_server, bool async, 
                                      bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_TRUE(brpc::ETOOMANYFAILS == cntl.ErrorCode() ||
                    ECONNREFUSED == cntl.ErrorCode()) << cntl.ErrorText();
        LOG(INFO) << cntl.ErrorText();
    }

    void TestConnectionFailedSelective(bool single_server, bool async, 
                                       bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        const size_t NCHANS = 8;
        brpc::SelectiveChannel channel;
        brpc::ChannelOptions options;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init("rr", &options));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(ECONNREFUSED, cntl.ErrorCode()) << cntl.ErrorText();
        ASSERT_EQ(1, cntl.sub_count());
        EXPECT_EQ(ECONNREFUSED, cntl.sub(0)->ErrorCode())
            << cntl.sub(0)->ErrorText();
        LOG(INFO) << cntl.ErrorText();
    }
    
    void TestSuccess(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) 
            << single_server << ", " << async << ", " << short_connection;
        const uint64_t receiving_socket_id = res.receiving_socket_id();
        EXPECT_EQ(0, cntl.sub_count());
        EXPECT_TRUE(NULL == cntl.sub(-1));
        EXPECT_TRUE(NULL == cntl.sub(0));
        EXPECT_TRUE(NULL == cntl.sub(1));
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }
        if (single_server && !short_connection) {
            // Reuse the connection
            brpc::Channel channel2;
            SetUpChannel(&channel2, single_server, short_connection);
            cntl.Reset();
            req.Clear();
            res.Clear();
            req.set_message(__FUNCTION__);
            CallMethod(&channel2, &cntl, &req, &res, async);
            EXPECT_EQ(0, cntl.ErrorCode())
                << single_server << ", " << async << ", " << short_connection;
            EXPECT_EQ(receiving_socket_id, res.receiving_socket_id());

            // A different connection_group does not reuse the connection
            brpc::Channel channel3;
            SetUpChannel(&channel3, single_server, short_connection,
                         NULL, "another_group");
            cntl.Reset();
            req.Clear();
            res.Clear();
            req.set_message(__FUNCTION__);
            CallMethod(&channel3, &cntl, &req, &res, async);
            EXPECT_EQ(0, cntl.ErrorCode())
                << single_server << ", " << async << ", " << short_connection;
            const uint64_t receiving_socket_id2 = res.receiving_socket_id();
            EXPECT_NE(receiving_socket_id, receiving_socket_id2);

            // Channel in the same connection_group reuses the connection
            // note that the leading/trailing spaces should be trimed.
            brpc::Channel channel4;
            SetUpChannel(&channel4, single_server, short_connection,
                         NULL, " another_group ");
            cntl.Reset();
            req.Clear();
            res.Clear();
            req.set_message(__FUNCTION__);
            CallMethod(&channel4, &cntl, &req, &res, async);
            EXPECT_EQ(0, cntl.ErrorCode())
                << single_server << ", " << async << ", " << short_connection;
            EXPECT_EQ(receiving_socket_id2, res.receiving_socket_id());
        }
        StopAndJoin();
    }

    class SetCode : public brpc::CallMapper {
    public:
        brpc::SubCall Map(
            int channel_index,
            const google::protobuf::MethodDescriptor* method,
            const google::protobuf::Message* req_base,
            google::protobuf::Message* response) {
            test::EchoRequest* req = brpc::Clone<test::EchoRequest>(req_base);
            req->set_code(channel_index + 1/*non-zero*/);
            return brpc::SubCall(method, req, response->New(),
                                brpc::DELETE_REQUEST | brpc::DELETE_RESPONSE);
        }
    };

    class SetCodeOnEven : public SetCode {
    public:
        brpc::SubCall Map(
            int channel_index,
            const google::protobuf::MethodDescriptor* method,
            const google::protobuf::Message* req_base,
            google::protobuf::Message* response) {
            if (channel_index % 2) {
                return brpc::SubCall::Skip();
            }
            return SetCode::Map(channel_index, method, req_base, response);
        }
    };


    class GetReqAndAddRes : public brpc::CallMapper {
        brpc::SubCall Map(
            int channel_index,
            const google::protobuf::MethodDescriptor* method,
            const google::protobuf::Message* req_base,
            google::protobuf::Message* res_base) {
            const test::ComboRequest* req =
                dynamic_cast<const test::ComboRequest*>(req_base);
            test::ComboResponse* res = dynamic_cast<test::ComboResponse*>(res_base);
            if (method->name() != "ComboEcho" ||
                res == NULL || req == NULL ||
                req->requests_size() <= channel_index) {
                return brpc::SubCall::Bad();
            }
            return brpc::SubCall(::test::EchoService::descriptor()->method(0),
                                &req->requests(channel_index),
                                res->add_responses(), 0);
        }
    };

    class MergeNothing : public brpc::ResponseMerger {
        Result Merge(google::protobuf::Message* /*response*/,
                     const google::protobuf::Message* /*sub_response*/) {
            return brpc::ResponseMerger::MERGED;
        }
    };

    void TestSuccessParallel(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          new SetCode, NULL));
        }
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_code(23);
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            EXPECT_TRUE(cntl.sub(i) && !cntl.sub(i)->Failed()) << "i=" << i;
        }
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        ASSERT_EQ(NCHANS, (size_t)res.code_list_size());
        for (size_t i = 0; i < NCHANS; ++i) {
            ASSERT_EQ((int)i+1, res.code_list(i));
        }
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }
        StopAndJoin();
    }

    void TestSuccessDuplicatedParallel(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        const size_t NCHANS = 8;
        brpc::Channel* subchan = new DeleteOnlyOnceChannel;
        SetUpChannel(subchan, single_server, short_connection);
        brpc::ParallelChannel channel;
        // Share the CallMapper and ResponseMerger should be fine because
        // they're intrusively shared.
        SetCode* set_code = new SetCode;
        for (size_t i = 0; i < NCHANS; ++i) {
            ASSERT_EQ(0, channel.AddChannel(
                          subchan,
                          // subchan should be deleted (for only once)
                          ((i % 2) ? brpc::DOESNT_OWN_CHANNEL : brpc::OWNS_CHANNEL),
                          set_code, NULL));
        }
        ASSERT_EQ((int)NCHANS, set_code->ref_count());
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_code(23);
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            EXPECT_TRUE(cntl.sub(i) && !cntl.sub(i)->Failed()) << "i=" << i;
        }
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        ASSERT_EQ(NCHANS, (size_t)res.code_list_size());
        for (size_t i = 0; i < NCHANS; ++i) {
            ASSERT_EQ((int)i+1, res.code_list(i));
        }
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }
        StopAndJoin();
    }
    
    void TestSuccessSelective(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        const size_t NCHANS = 8;
        ASSERT_EQ(0, StartAccept(_ep));
        brpc::SelectiveChannel channel;
        brpc::ChannelOptions options;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init("rr", &options));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_code(23);
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(1, cntl.sub_count());
        ASSERT_EQ(0, cntl.sub(0)->ErrorCode());
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        ASSERT_EQ(1, res.code_list_size());
        ASSERT_EQ(req.code(), res.code_list(0));
        ASSERT_EQ(_ep, cntl.remote_side());
        
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }
        StopAndJoin();
    }

    void TestSkipParallel(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          new SetCodeOnEven, NULL));
        }
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_code(23);
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            if (i % 2) {
                EXPECT_TRUE(NULL == cntl.sub(i)) << "i=" << i;
            } else {
                EXPECT_TRUE(cntl.sub(i) && !cntl.sub(i)->Failed()) << "i=" << i;
            }
        }
        ASSERT_EQ(NCHANS / 2, (size_t)res.code_list_size());
        for (int i = 0; i < res.code_list_size(); ++i) {
            ASSERT_EQ(i*2 + 1, res.code_list(i));
        }
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }
        StopAndJoin();
    }

    void TestSuccessParallel2(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          new GetReqAndAddRes, new MergeNothing));
        }
        brpc::Controller cntl;
        test::ComboRequest req;
        test::ComboResponse res;
        CallMethod(&channel, &cntl, &req, &res, false);
        ASSERT_TRUE(cntl.Failed()); // req does not have .requests
        ASSERT_EQ(brpc::EREQUEST, cntl.ErrorCode());

        for (size_t i = 0; i < NCHANS; ++i) {
            ::test::EchoRequest* sub_req = req.add_requests();
            sub_req->set_message(butil::string_printf("hello_%llu", (long long)i));
            sub_req->set_code(i + 1);
        }

        // non-parallel channel does not work.
        cntl.Reset();
        CallMethod(&subchans[0], &cntl, &req, &res, false);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(brpc::EINTERNAL, cntl.ErrorCode()) << cntl.ErrorText();
        ASSERT_TRUE(butil::StringPiece(cntl.ErrorText()).ends_with("Method ComboEcho() not implemented."));

        // do the rpc call.
        cntl.Reset();
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        ASSERT_GT(cntl.latency_us(), 0);
        ASSERT_EQ((int)NCHANS, res.responses_size());
        for (int i = 0; i < res.responses_size(); ++i) {
            EXPECT_EQ(butil::string_printf("received hello_%d", i),
                      res.responses(i).message());
            ASSERT_EQ(1, res.responses(i).code_list_size());
            EXPECT_EQ(i + 1, res.responses(i).code_list(0));
        }
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }
        StopAndJoin();
    }
    
    struct CancelerArg {
        int64_t sleep_before_cancel_us;
        brpc::CallId cid;
    };

    static void* Canceler(void* void_arg) {
        CancelerArg* arg = static_cast<CancelerArg*>(void_arg);
        if (arg->sleep_before_cancel_us > 0) {
            bthread_usleep(arg->sleep_before_cancel_us);
        }
        LOG(INFO) << "Start to cancel cid=" << arg->cid.value;
        brpc::StartCancel(arg->cid);
        return NULL;
    }


    void CancelBeforeCallMethod(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        brpc::StartCancel(cid);
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(ECANCELED, cntl.ErrorCode()) << cntl.ErrorText();
        StopAndJoin();
    }

    void CancelBeforeCallMethodParallel(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        brpc::StartCancel(cid);
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(ECANCELED, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        EXPECT_TRUE(NULL == cntl.sub(1));
        EXPECT_TRUE(NULL == cntl.sub(0));
        StopAndJoin();
    }

    void CancelBeforeCallMethodSelective(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::SelectiveChannel channel;
        ASSERT_EQ(0, channel.Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        brpc::StartCancel(cid);
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(ECANCELED, cntl.ErrorCode()) << cntl.ErrorText();
        StopAndJoin();
    }

    void CancelDuringCallMethod(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        pthread_t th;
        CancelerArg carg = { 10000, cid };
        ASSERT_EQ(0, pthread_create(&th, NULL, Canceler, &carg));
        req.set_sleep_us(carg.sleep_before_cancel_us * 2);
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_LT(labs(tm.u_elapsed() - carg.sleep_before_cancel_us), 10000);
        ASSERT_EQ(0, pthread_join(th, NULL));
        EXPECT_EQ(ECANCELED, cntl.ErrorCode());
        EXPECT_EQ(0, cntl.sub_count());
        EXPECT_TRUE(NULL == cntl.sub(1));
        EXPECT_TRUE(NULL == cntl.sub(0));
        StopAndJoin();
    }
    
    void CancelDuringCallMethodParallel(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        pthread_t th;
        CancelerArg carg = { 10000, cid };
        ASSERT_EQ(0, pthread_create(&th, NULL, Canceler, &carg));
        req.set_sleep_us(carg.sleep_before_cancel_us * 2);
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_LT(labs(tm.u_elapsed() - carg.sleep_before_cancel_us), 10000);
        ASSERT_EQ(0, pthread_join(th, NULL));
        EXPECT_EQ(ECANCELED, cntl.ErrorCode());
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            EXPECT_EQ(ECANCELED, cntl.sub(i)->ErrorCode()) << "i=" << i;
        }
        EXPECT_LT(labs(cntl.latency_us() - carg.sleep_before_cancel_us), 10000);
        StopAndJoin();
    }

    void CancelDuringCallMethodSelective(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::SelectiveChannel channel;
        ASSERT_EQ(0, channel.Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        pthread_t th;
        CancelerArg carg = { 10000, cid };
        ASSERT_EQ(0, pthread_create(&th, NULL, Canceler, &carg));
        req.set_sleep_us(carg.sleep_before_cancel_us * 2);
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_LT(labs(tm.u_elapsed() - carg.sleep_before_cancel_us), 10000);
        ASSERT_EQ(0, pthread_join(th, NULL));
        EXPECT_EQ(ECANCELED, cntl.ErrorCode());
        EXPECT_EQ(1, cntl.sub_count());
        EXPECT_EQ(ECANCELED, cntl.sub(0)->ErrorCode());
        StopAndJoin();
    }
    
    void CancelAfterCallMethod(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(0, cntl.ErrorCode());
        EXPECT_EQ(0, cntl.sub_count());
        ASSERT_EQ(EINVAL, bthread_id_error(cid, ECANCELED));
        StopAndJoin();
    }

    void CancelAfterCallMethodParallel(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        const brpc::CallId cid = cntl.call_id();
        ASSERT_TRUE(cid.value != 0);
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(0, cntl.ErrorCode());
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            EXPECT_TRUE(cntl.sub(i) && !cntl.sub(i)->Failed()) << "i=" << i;
        }
        ASSERT_EQ(EINVAL, bthread_id_error(cid, ECANCELED));
        StopAndJoin();
    }

    void TestAttachment(bool async, bool short_connection) {
        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, true, short_connection);
                
        brpc::Controller cntl;
        cntl.request_attachment().append("attachment");
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(0, cntl.ErrorCode())  << short_connection;
        EXPECT_FALSE(cntl.request_attachment().empty())
            << ", " << async << ", " << short_connection;
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        if (short_connection) {
            // Sleep to let `_messenger' detect `Socket' being `SetFailed'
            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            EXPECT_GE(1ul, _messenger.ConnectionCount());
        }            
        StopAndJoin();
    }

    void TestRequestNotInit(bool single_server, bool async,
                            bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(brpc::EREQUEST, cntl.ErrorCode()) << cntl.ErrorText();
        StopAndJoin();
    }

    void TestRequestNotInitParallel(bool single_server, bool async,
                                    bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }
        
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(brpc::EREQUEST, cntl.ErrorCode()) << cntl.ErrorText();
        LOG(WARNING) << cntl.ErrorText();
        StopAndJoin();
    }

    void TestRequestNotInitSelective(bool single_server, bool async,
                                     bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::SelectiveChannel channel;
        ASSERT_EQ(0, channel.Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }
        
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(brpc::EREQUEST, cntl.ErrorCode()) << cntl.ErrorText();
        LOG(WARNING) << cntl.ErrorText();
        ASSERT_EQ(1, cntl.sub_count());
        ASSERT_EQ(brpc::EREQUEST, cntl.sub(0)->ErrorCode());
        StopAndJoin();
    }
    
    void TestRPCTimeout(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_sleep_us(70000); // 70ms
        cntl.set_timeout_ms(17);
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_EQ(brpc::ERPCTIMEDOUT, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_LT(labs(tm.m_elapsed() - cntl.timeout_ms()), 15);
        StopAndJoin();
    }

    void TestRPCTimeoutParallel(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));
        
        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        cntl.set_timeout_ms(17);
        req.set_sleep_us(70000); // 70ms
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_EQ(brpc::ERPCTIMEDOUT, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            EXPECT_EQ(ECANCELED, cntl.sub(i)->ErrorCode()) << "i=" << i;
        }
        EXPECT_LT(labs(tm.m_elapsed() - cntl.timeout_ms()), 15);
        StopAndJoin();
    }

    class MakeTheRequestTimeout : public brpc::CallMapper {
    public:
        brpc::SubCall Map(
            int /*channel_index*/,
            const google::protobuf::MethodDescriptor* method,
            const google::protobuf::Message* req_base,
            google::protobuf::Message* response) {
            test::EchoRequest* req = brpc::Clone<test::EchoRequest>(req_base);
            req->set_sleep_us(70000); // 70ms
            return brpc::SubCall(method, req, response->New(),
                                brpc::DELETE_REQUEST | brpc::DELETE_RESPONSE);
        }
    };

    void TimeoutStillChecksSubChannelsParallel(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));
        
        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          ((i % 2) ? new MakeTheRequestTimeout : NULL), NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        cntl.set_timeout_ms(30);
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(NCHANS, (size_t)cntl.sub_count());
        for (int i = 0; i < cntl.sub_count(); ++i) {
            if (i % 2) {
                EXPECT_EQ(ECANCELED, cntl.sub(i)->ErrorCode());
            } else {
                EXPECT_EQ(0, cntl.sub(i)->ErrorCode());
            }
        }
        EXPECT_LT(labs(tm.m_elapsed() - cntl.timeout_ms()), 15);
        StopAndJoin();
    }

    void TestRPCTimeoutSelective(
        bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;
        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::SelectiveChannel channel;
        ASSERT_EQ(0, channel.Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        cntl.set_timeout_ms(17);
        req.set_sleep_us(70000); // 70ms
        butil::Timer tm;
        tm.start();
        CallMethod(&channel, &cntl, &req, &res, async);
        tm.stop();
        EXPECT_EQ(brpc::ERPCTIMEDOUT, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(1, cntl.sub_count());
        EXPECT_EQ(brpc::ERPCTIMEDOUT, cntl.sub(0)->ErrorCode());
        EXPECT_LT(labs(tm.m_elapsed() - cntl.timeout_ms()), 15);
        EXPECT_EQ(-1, cntl.sub(0)->_timeout_ms);
        EXPECT_EQ(17, cntl.sub(0)->_real_timeout_ms);
        StopAndJoin();
    }
    
    void TestCloseFD(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_close_fd(true);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(brpc::EEOF, cntl.ErrorCode()) << cntl.ErrorText();
        StopAndJoin();
    }

    void TestCloseFDParallel(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_close_fd(true);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_TRUE(brpc::EEOF == cntl.ErrorCode() ||
                    brpc::ETOOMANYFAILS == cntl.ErrorCode() ||
                    ECONNRESET == cntl.ErrorCode()) << cntl.ErrorText();
        StopAndJoin();
    }

    void TestCloseFDSelective(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::SelectiveChannel channel;
        brpc::ChannelOptions options;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init("rr", &options));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_close_fd(true);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(brpc::EEOF, cntl.ErrorCode()) << cntl.ErrorText();
        ASSERT_EQ(1, cntl.sub_count());
        ASSERT_EQ(brpc::EEOF, cntl.sub(0)->ErrorCode());

        StopAndJoin();
    }
    
    void TestServerFail(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_server_fail(brpc::EINTERNAL);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(brpc::EINTERNAL, cntl.ErrorCode()) << cntl.ErrorText();
        StopAndJoin();
    }

    void TestServerFailParallel(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 8;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (size_t i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_server_fail(brpc::EINTERNAL);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(brpc::EINTERNAL, cntl.ErrorCode()) << cntl.ErrorText();
        LOG(INFO) << cntl.ErrorText();
        StopAndJoin();
    }

    void TestServerFailSelective(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));

        const size_t NCHANS = 5;
        brpc::SelectiveChannel channel;
        ASSERT_EQ(0, channel.Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }

        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_server_fail(brpc::EINTERNAL);
        CallMethod(&channel, &cntl, &req, &res, async);
        
        EXPECT_EQ(brpc::EINTERNAL, cntl.ErrorCode()) << cntl.ErrorText();
        ASSERT_EQ(1, cntl.sub_count());
        ASSERT_EQ(brpc::EINTERNAL, cntl.sub(0)->ErrorCode());

        LOG(INFO) << cntl.ErrorText();
        StopAndJoin();
    }
    
    void TestDestroyChannel(bool single_server, bool short_connection) {
        std::cout << "*** single=" << single_server
                  << ", short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel* channel = new brpc::Channel();
        SetUpChannel(channel, single_server, short_connection);
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        req.set_sleep_us(10000);
        CallMethod(channel, &cntl, &req, &res, true, true/*destroy*/);

        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        // Sleep to let `_messenger' detect `Socket' being `SetFailed'
        const int64_t start_time = butil::gettimeofday_us();
        while (_messenger.ConnectionCount() != 0) {
            EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
            bthread_usleep(1000);
        }

        StopAndJoin();
    }
    
    void TestDestroyChannelParallel(bool single_server, bool short_connection) {
        std::cout << "*** single=" << single_server
                  << ", short=" << short_connection << std::endl;

        const size_t NCHANS = 5;
        ASSERT_EQ(0, StartAccept(_ep));
        brpc::ParallelChannel* channel = new brpc::ParallelChannel;
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel();
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel->AddChannel(
                          subchan, brpc::OWNS_CHANNEL, NULL, NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_sleep_us(10000);
        req.set_message(__FUNCTION__);
        CallMethod(channel, &cntl, &req, &res, true, true/*destroy*/);
        
        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        // Sleep to let `_messenger' detect `Socket' being `SetFailed'
        const int64_t start_time = butil::gettimeofday_us();
        while (_messenger.ConnectionCount() != 0) {
            EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
            bthread_usleep(1000);
        }
        StopAndJoin();
    }

    void TestDestroyChannelSelective(bool single_server, bool short_connection) {
        std::cout << "*** single=" << single_server
                  << ", short=" << short_connection << std::endl;

        const size_t NCHANS = 5;
        ASSERT_EQ(0, StartAccept(_ep));
        brpc::SelectiveChannel* channel = new brpc::SelectiveChannel;
        ASSERT_EQ(0, channel->Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel();
            SetUpChannel(subchan, single_server, short_connection);
            ASSERT_EQ(0, channel->AddChannel(subchan, NULL));
        }
                
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_sleep_us(10000);
        req.set_message(__FUNCTION__);
        CallMethod(channel, &cntl, &req, &res, true, true/*destroy*/);
        
        EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
        ASSERT_EQ(_ep, cntl.remote_side());
        ASSERT_EQ(1, cntl.sub_count());
        ASSERT_EQ(0, cntl.sub(0)->ErrorCode());

        // Sleep to let `_messenger' detect `Socket' being `SetFailed'
        const int64_t start_time = butil::gettimeofday_us();
        while (_messenger.ConnectionCount() != 0) {
            EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
            bthread_usleep(1000);
        }
        StopAndJoin();
    }
    
    void RPCThread(brpc::ChannelBase* channel, bool async) {
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message(__FUNCTION__);
        CallMethod(channel, &cntl, &req, &res, async);

        ASSERT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ("received " + std::string(__FUNCTION__), res.message());
    }

    void RPCThread(brpc::ChannelBase* channel, bool async, int count) {
        brpc::Controller cntl;
        for (int i = 0; i < count; ++i) {
            test::EchoRequest req;
            test::EchoResponse res;
            req.set_message(__FUNCTION__);
            CallMethod(channel, &cntl, &req, &res, async);
            
            ASSERT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
            ASSERT_EQ("received " + std::string(__FUNCTION__), res.message());
            cntl.Reset();
        }
    }

    void RPCThread(bool single_server, bool async, bool short_connection,
                   const brpc::Authenticator* auth, int count) {
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection, auth);
        brpc::Controller cntl;
        for (int i = 0; i < count; ++i) {
            test::EchoRequest req;
            test::EchoResponse res;
            req.set_message(__FUNCTION__);
            CallMethod(&channel, &cntl, &req, &res, async);

            ASSERT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
            ASSERT_EQ("received " + std::string(__FUNCTION__), res.message());
            cntl.Reset();
        }
    }

    void TestAuthentication(bool single_server, 
                            bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        MyAuthenticator auth;
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection, &auth);

        const int NUM = 10;
        pthread_t tids[NUM];
        for (int i = 0; i < NUM; ++i) {
            google::protobuf::Closure* thrd_func = 
                brpc::NewCallback(
                    this, &ChannelTest::RPCThread, (brpc::ChannelBase*)&channel, async);
            EXPECT_EQ(0, pthread_create(&tids[i], NULL,
                                        RunClosure, thrd_func));
        }
        for (int i = 0; i < NUM; ++i) {
            pthread_join(tids[i], NULL);
        }
        
        if (short_connection) {
            EXPECT_EQ(NUM, auth.count.load());
        } else {
            EXPECT_EQ(1, auth.count.load());
        }
        StopAndJoin();
    }

    void TestAuthenticationParallel(bool single_server, 
                                    bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        MyAuthenticator auth;

        const int NCHANS = 5;
        brpc::Channel subchans[NCHANS];
        brpc::ParallelChannel channel;
        for (int i = 0; i < NCHANS; ++i) {
            SetUpChannel(&subchans[i], single_server, short_connection, &auth);
            ASSERT_EQ(0, channel.AddChannel(
                          &subchans[i], brpc::DOESNT_OWN_CHANNEL,
                          NULL, NULL));
        }
        
        const int NUM = 10;
        pthread_t tids[NUM];
        for (int i = 0; i < NUM; ++i) {
            google::protobuf::Closure* thrd_func = 
                brpc::NewCallback(
                    this, &ChannelTest::RPCThread, (brpc::ChannelBase*)&channel, async);
            EXPECT_EQ(0, pthread_create(&tids[i], NULL,
                                        RunClosure, thrd_func));
        }
        for (int i = 0; i < NUM; ++i) {
            pthread_join(tids[i], NULL);
        }
        
        if (short_connection) {
            EXPECT_EQ(NUM * NCHANS, auth.count.load());
        } else {
            EXPECT_EQ(1, auth.count.load());
        }
        StopAndJoin();
    }

    void TestAuthenticationSelective(bool single_server, 
                                    bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        MyAuthenticator auth;

        const size_t NCHANS = 5;
        brpc::SelectiveChannel channel;
        ASSERT_EQ(0, channel.Init("rr", NULL));
        for (size_t i = 0; i < NCHANS; ++i) {
            brpc::Channel* subchan = new brpc::Channel;
            SetUpChannel(subchan, single_server, short_connection, &auth);
            ASSERT_EQ(0, channel.AddChannel(subchan, NULL)) << "i=" << i;
        }
        
        const int NUM = 10;
        pthread_t tids[NUM];
        for (int i = 0; i < NUM; ++i) {
            google::protobuf::Closure* thrd_func = 
                brpc::NewCallback(
                    this, &ChannelTest::RPCThread, (brpc::ChannelBase*)&channel, async);
            EXPECT_EQ(0, pthread_create(&tids[i], NULL,
                                        RunClosure, thrd_func));
        }
        for (int i = 0; i < NUM; ++i) {
            pthread_join(tids[i], NULL);
        }
        
        if (short_connection) {
            EXPECT_EQ(NUM, auth.count.load());
        } else {
            EXPECT_EQ(1, auth.count.load());
        }
        StopAndJoin();
    }
    
    void TestRetry(bool single_server, bool async, bool short_connection) {
        std::cout << " *** single=" << single_server
                  << " async=" << async
                  << " short=" << short_connection << std::endl;

        ASSERT_EQ(0, StartAccept(_ep));
        brpc::Channel channel;
        SetUpChannel(&channel, single_server, short_connection);

        const int RETRY_NUM = 3;
        test::EchoRequest req;
        test::EchoResponse res;
        brpc::Controller cntl;
        req.set_message(__FUNCTION__);

        // No retry when timeout
        cntl.set_max_retry(RETRY_NUM);
        cntl.set_timeout_ms(10);  // 10ms
        req.set_sleep_us(70000); // 70ms
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(brpc::ERPCTIMEDOUT, cntl.ErrorCode()) << cntl.ErrorText();
        EXPECT_EQ(0, cntl.retried_count());
        bthread_usleep(100000);  // wait for the sleep task to finish

        // Retry when connection broken
        cntl.Reset();
        cntl.set_max_retry(RETRY_NUM);
        _close_fd_once = true;
        req.set_sleep_us(0);
        CallMethod(&channel, &cntl, &req, &res, async);

        if (short_connection) {
            // Always succeed
            EXPECT_EQ(0, cntl.ErrorCode()) << cntl.ErrorText();
            EXPECT_EQ(1, cntl.retried_count());

            const int64_t start_time = butil::gettimeofday_us();
            while (_messenger.ConnectionCount() != 0) {
                EXPECT_LT(butil::gettimeofday_us(), start_time + 100000L/*100ms*/);
                bthread_usleep(1000);
            }
        } else {
            // May fail if health checker can't revive in time
            if (cntl.Failed()) {
                EXPECT_EQ(EHOSTDOWN, cntl.ErrorCode()) << single_server << ", " << async;
                EXPECT_EQ(RETRY_NUM, cntl.retried_count());
            } else {
                EXPECT_TRUE(cntl.retried_count() > 0);
            }
        }   
        StopAndJoin();
        bthread_usleep(100000);  // wait for stop
        
        // Retry when connection failed
        cntl.Reset();
        cntl.set_max_retry(RETRY_NUM);
        CallMethod(&channel, &cntl, &req, &res, async);
        EXPECT_EQ(EHOSTDOWN, cntl.ErrorCode());
        EXPECT_EQ(RETRY_NUM, cntl.retried_count());
    }

    void TestRetryOtherServer(bool async, bool short_connection) {
        ASSERT_EQ(0, StartAccept(_ep));

        brpc::Channel channel;
        brpc::ChannelOptions opt;
        if (short_connection) {
            opt.connection_type = brpc::CONNECTION_TYPE_SHORT;
        }
        butil::TempFile server_list;                                        
        EXPECT_EQ(0, server_list.save_format(
                      "127.0.0.1:100\n"
                      "127.0.0.1:200\n"
                      "%s", endpoint2str(_ep).c_str()));
        std::string naming_url = std::string("fIle://")
            + server_list.fname();
        EXPECT_EQ(0, channel.Init(naming_url.c_str(), "RR", &opt)); 

        const int RETRY_NUM = 3;
        test::EchoRequest req;
        test::EchoResponse res;
        brpc::Controller cntl;
        req.set_message(__FUNCTION__);
        cntl.set_max_retry(RETRY_NUM);
        CallMethod(&channel, &cntl, &req, &res, async);

        EXPECT_EQ(0, cntl.ErrorCode()) << async << ", " << short_connection;
        StopAndJoin();
    }

    butil::EndPoint _ep;
    butil::TempFile _server_list;                                        
    std::string _naming_url;
    
    brpc::Acceptor _messenger;
    // Dummy server for `Server::AddError'
    brpc::Server _dummy;
    std::string _mock_fail_str;

    bool _close_fd_once;
    
    MyEchoService _svc;
};

class MyShared : public brpc::SharedObject {
public:
    MyShared() { ++ nctor; }
    MyShared(const MyShared&) : brpc::SharedObject() { ++ nctor; }
    ~MyShared() { ++ ndtor; }

    static int nctor;
    static int ndtor;
};
int MyShared::nctor = 0;
int MyShared::ndtor = 0;

TEST_F(ChannelTest, intrusive_ptr_sanity) {
    MyShared::nctor = 0;
    MyShared::ndtor = 0;
    {
        MyShared* s1 = new MyShared;
        ASSERT_EQ(0, s1->ref_count());
        butil::intrusive_ptr<MyShared> p1 = s1;
        ASSERT_EQ(1, p1->ref_count());
        {
            butil::intrusive_ptr<MyShared> p2 = s1;
            ASSERT_EQ(2, p2->ref_count());
            ASSERT_EQ(2, p1->ref_count());
        }
        ASSERT_EQ(1, p1->ref_count());
    }
    ASSERT_EQ(1, MyShared::nctor);
    ASSERT_EQ(1, MyShared::ndtor);
}

TEST_F(ChannelTest, init_as_single_server) {
    {
        brpc::Channel channel;
        ASSERT_EQ(-1, channel.Init("127.0.0.1:12345:asdf", NULL));
        ASSERT_EQ(-1, channel.Init("127.0.0.1:99999", NULL)); 
        ASSERT_EQ(0, channel.Init("127.0.0.1:8888", NULL));
    }
    {
        brpc::Channel channel;
        ASSERT_EQ(-1, channel.Init("127.0.0.1asdf", 12345, NULL));
        ASSERT_EQ(-1, channel.Init("127.0.0.1", 99999, NULL));
        ASSERT_EQ(0, channel.Init("127.0.0.1", 8888, NULL));
    }

    butil::EndPoint ep;
    brpc::Channel channel;
    ASSERT_EQ(0, str2endpoint("127.0.0.1:8888", &ep));
    ASSERT_EQ(0, channel.Init(ep, NULL));
    ASSERT_TRUE(channel.SingleServer());
    ASSERT_EQ(ep, channel._server_address);

    brpc::SocketId id;
    ASSERT_EQ(0, brpc::SocketMapFind(brpc::SocketMapKey(ep), &id));
    ASSERT_EQ(id, channel._server_id);

    const int NUM = 10;
    brpc::Channel channels[NUM];
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(0, channels[i].Init(ep, NULL));
        // Share the same server socket
        ASSERT_EQ(id, channels[i]._server_id);
    }
}

TEST_F(ChannelTest, init_using_unknown_naming_service) {
    brpc::Channel channel;
    ASSERT_EQ(-1, channel.Init("unknown://unknown", "unknown", NULL));
}

TEST_F(ChannelTest, init_using_unexist_fns) {
    brpc::Channel channel;
    ASSERT_EQ(-1, channel.Init("fiLe://no_such_file", "rr", NULL));
}

TEST_F(ChannelTest, init_using_empty_fns) {
    brpc::ChannelOptions opt;
    opt.succeed_without_server = false;
    brpc::Channel channel;
    butil::TempFile server_list;
    ASSERT_EQ(0, server_list.save(""));
    std::string naming_url = std::string("file://") + server_list.fname();
    // empty file list results in error.
    ASSERT_EQ(-1, channel.Init(naming_url.c_str(), "rr", &opt));

    ASSERT_EQ(0, server_list.save("blahblah"));
    // No valid address.
    ASSERT_EQ(-1, channel.Init(naming_url.c_str(), "rr", NULL));
}

TEST_F(ChannelTest, init_using_empty_lns) {
    brpc::ChannelOptions opt;
    opt.succeed_without_server = false;
    brpc::Channel channel;
    ASSERT_EQ(-1, channel.Init("list:// ", "rr", &opt));
    ASSERT_EQ(-1, channel.Init("list://", "rr", &opt));
    ASSERT_EQ(-1, channel.Init("list://blahblah", "rr", &opt)); 
}

TEST_F(ChannelTest, init_using_naming_service) {
    brpc::Channel* channel = new brpc::Channel();
    butil::TempFile server_list;
    ASSERT_EQ(0, server_list.save("127.0.0.1:8888"));
    std::string naming_url = std::string("filE://") + server_list.fname();
    // Rr are intended to test case-insensitivity.
    ASSERT_EQ(0, channel->Init(naming_url.c_str(), "Rr", NULL));
    ASSERT_FALSE(channel->SingleServer());

    brpc::LoadBalancerWithNaming* lb =
        dynamic_cast<brpc::LoadBalancerWithNaming*>(channel->_lb.get());
    ASSERT_TRUE(lb != NULL);
    brpc::NamingServiceThread* ns = lb->_nsthread_ptr.get();

    {
        const int NUM = 10;
        brpc::Channel channels[NUM];
        for (int i = 0; i < NUM; ++i) {
            // Share the same naming thread
            ASSERT_EQ(0, channels[i].Init(naming_url.c_str(), "rr", NULL));
            brpc::LoadBalancerWithNaming* lb2 =
                dynamic_cast<brpc::LoadBalancerWithNaming*>(channels[i]._lb.get());
            ASSERT_TRUE(lb2 != NULL);
            ASSERT_EQ(ns, lb2->_nsthread_ptr.get());
        }
    }

    // `lb' should be valid even if `channel' has destroyed
    // since we hold another reference to it
    butil::intrusive_ptr<brpc::SharedLoadBalancer>
        another_ctx = channel->_lb;
    delete channel;
    ASSERT_EQ(lb, another_ctx.get());
    ASSERT_EQ(1, another_ctx->_nref.load());
    // `lb' should be destroyed after
}

TEST_F(ChannelTest, connection_failed) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestConnectionFailed(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, empty_parallel_channel) {
    brpc::ParallelChannel channel;

    brpc::Controller cntl;
    test::EchoRequest req;
    test::EchoResponse res;
    req.set_message(__FUNCTION__);
    CallMethod(&channel, &cntl, &req, &res, false);
    EXPECT_EQ(EPERM, cntl.ErrorCode()) << cntl.ErrorText();
}

TEST_F(ChannelTest, empty_selective_channel) {
    brpc::SelectiveChannel channel;
    ASSERT_EQ(0, channel.Init("rr", NULL));

    brpc::Controller cntl;
    test::EchoRequest req;
    test::EchoResponse res;
    req.set_message(__FUNCTION__);
    CallMethod(&channel, &cntl, &req, &res, false);
    EXPECT_EQ(ENODATA, cntl.ErrorCode()) << cntl.ErrorText();
}

class BadCall : public brpc::CallMapper {
    brpc::SubCall Map(int,
                     const google::protobuf::MethodDescriptor*,
                     const google::protobuf::Message*,
                     google::protobuf::Message*) {
        return brpc::SubCall::Bad();
    }
};

TEST_F(ChannelTest, returns_bad_parallel) {
    const size_t NCHANS = 5;
    brpc::ParallelChannel channel;
    for (size_t i = 0; i < NCHANS; ++i) {
        brpc::Channel* subchan = new brpc::Channel();
        SetUpChannel(subchan, true, false);
        ASSERT_EQ(0, channel.AddChannel(
                      subchan, brpc::OWNS_CHANNEL, new BadCall, NULL));
    }
                
    brpc::Controller cntl;
    test::EchoRequest req;
    test::EchoResponse res;
    req.set_message(__FUNCTION__);
    CallMethod(&channel, &cntl, &req, &res, false);
    EXPECT_EQ(brpc::EREQUEST, cntl.ErrorCode()) << cntl.ErrorText();
}

class SkipCall : public brpc::CallMapper {
    brpc::SubCall Map(int,
                     const google::protobuf::MethodDescriptor*,
                     const google::protobuf::Message*,
                     google::protobuf::Message*) {
        return brpc::SubCall::Skip();
    }
};

TEST_F(ChannelTest, skip_all_channels) {
    const size_t NCHANS = 5;
    brpc::ParallelChannel channel;
    for (size_t i = 0; i < NCHANS; ++i) {
        brpc::Channel* subchan = new brpc::Channel();
        SetUpChannel(subchan, true, false);
        ASSERT_EQ(0, channel.AddChannel(
                      subchan, brpc::OWNS_CHANNEL, new SkipCall, NULL));
    }
                
    brpc::Controller cntl;
    test::EchoRequest req;
    test::EchoResponse res;
    req.set_message(__FUNCTION__);
    CallMethod(&channel, &cntl, &req, &res, false);
        
    EXPECT_EQ(ECANCELED, cntl.ErrorCode()) << cntl.ErrorText();
    EXPECT_EQ((int)NCHANS, cntl.sub_count());
    for (int i = 0; i < cntl.sub_count(); ++i) {
        EXPECT_TRUE(NULL == cntl.sub(i)) << "i=" << i;
    }
}

TEST_F(ChannelTest, connection_failed_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestConnectionFailedParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, connection_failed_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestConnectionFailedSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, success) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestSuccess(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, success_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestSuccessParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, success_duplicated_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestSuccessDuplicatedParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, success_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestSuccessSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, skip_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestSkipParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, success_parallel2) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestSuccessParallel2(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_before_callmethod) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelBeforeCallMethod(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_before_callmethod_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelBeforeCallMethodParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_before_callmethod_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelBeforeCallMethodSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_during_callmethod) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelDuringCallMethod(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_during_callmethod_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelDuringCallMethodParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_during_callmethod_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelDuringCallMethodSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_after_callmethod) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelAfterCallMethod(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, cancel_after_callmethod_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                CancelAfterCallMethodParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, request_not_init) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRequestNotInit(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, request_not_init_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRequestNotInitParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, request_not_init_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRequestNotInitSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, timeout) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRPCTimeout(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, timeout_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRPCTimeoutParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, timeout_still_checks_sub_channels_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TimeoutStillChecksSubChannelsParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, timeout_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRPCTimeoutSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, close_fd) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestCloseFD(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, close_fd_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestCloseFDParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, close_fd_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestCloseFDSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, server_fail) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestServerFail(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, server_fail_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestServerFailParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, server_fail_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestServerFailSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, authentication) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestAuthentication(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, authentication_parallel) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestAuthenticationParallel(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, authentication_selective) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestAuthenticationSelective(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, retry) {
    for (int i = 0; i <= 1; ++i) { // Flag SingleServer 
        for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
            for (int k = 0; k <=1; ++k) { // Flag ShortConnection
                TestRetry(i, j, k);
            }
        }
    }
}

TEST_F(ChannelTest, retry_other_servers) {
    for (int j = 0; j <= 1; ++j) { // Flag Asynchronous
        for (int k = 0; k <=1; ++k) { // Flag ShortConnection
            TestRetryOtherServer(j, k);
        }
    }
}

TEST_F(ChannelTest, multiple_threads_single_channel) {
    srand(time(NULL));
    ASSERT_EQ(0, StartAccept(_ep));
    MyAuthenticator auth;
    const int NUM = 10;
    const int COUNT = 10000;
    pthread_t tids[NUM];

    // Cause massive connect/close log if setting to true
    bool short_connection = false;
    for (int single_server = 0; single_server <= 1; ++single_server) {
        for (int need_auth = 0; need_auth <= 1; ++need_auth) {
            for (int async = 0; async <= 1; ++async) {
                std::cout << " *** short=" << short_connection
                          << " single=" << single_server
                          << " auth=" << need_auth
                          << " async=" << async << std::endl;
                brpc::Channel channel;
                SetUpChannel(&channel, single_server, 
                             short_connection, (need_auth ? &auth : NULL));
                for (int i = 0; i < NUM; ++i) {
                    google::protobuf::Closure* thrd_func = 
                        brpc::NewCallback(
                            this, &ChannelTest::RPCThread, 
                            (brpc::ChannelBase*)&channel,
                            (bool)async, COUNT);
                    EXPECT_EQ(0, pthread_create(&tids[i], NULL,
                                                RunClosure, thrd_func));
                }
                for (int i = 0; i < NUM; ++i) {
                    pthread_join(tids[i], NULL);
                }
            }
        }
    }
}

TEST_F(ChannelTest, multiple_threads_multiple_channels) {
    srand(time(NULL));
    ASSERT_EQ(0, StartAccept(_ep));
    MyAuthenticator auth;
    const int NUM = 10;
    const int COUNT = 10000;
    pthread_t tids[NUM];

    // Cause massive connect/close log if setting to true
    bool short_connection = false;

    for (int single_server = 0; single_server <= 1; ++single_server) {
        for (int need_auth = 0; need_auth <= 1; ++need_auth) {
            for (int async = 0; async <= 1; ++async) {
                std::cout << " *** short=" << short_connection
                          << " single=" << single_server
                          << " auth=" << need_auth
                          << " async=" << async << std::endl;
                for (int i = 0; i < NUM; ++i) {
                    google::protobuf::Closure* thrd_func = 
                        brpc::NewCallback<
                        ChannelTest, ChannelTest*,
                        bool, bool, bool, const brpc::Authenticator*, int>
                        (this, &ChannelTest::RPCThread, single_server,
                         async, short_connection, (need_auth ? &auth : NULL), COUNT);
                    EXPECT_EQ(0, pthread_create(&tids[i], NULL,
                                                RunClosure, thrd_func));
                }
                for (int i = 0; i < NUM; ++i) {
                    pthread_join(tids[i], NULL);
                }
            }
        }
    }
}

TEST_F(ChannelTest, clear_attachment_after_retry) {
    for (int j = 0; j <= 1; ++j) {
        for (int k = 0; k <= 1; ++k) {
            TestAttachment(j, k);
        }
    }
}

TEST_F(ChannelTest, destroy_channel) {
    for (int i = 0; i <= 1; ++i) {
        for (int j = 0; j <= 1; ++j) {
            TestDestroyChannel(i, j);
        }
    }
}

TEST_F(ChannelTest, destroy_channel_parallel) {
    for (int i = 0; i <= 1; ++i) {
        for (int j = 0; j <= 1; ++j) {
            TestDestroyChannelParallel(i, j);
        }
    }
}

TEST_F(ChannelTest, destroy_channel_selective) {
    for (int i = 0; i <= 1; ++i) {
        for (int j = 0; j <= 1; ++j) {
            TestDestroyChannelSelective(i, j);
        }
    }
}

TEST_F(ChannelTest, sizeof) {
    LOG(INFO) << "Size of Channel is " << sizeof(brpc::Channel)
               << ", Size of ParallelChannel is " << sizeof(brpc::ParallelChannel)
               << ", Size of Controller is " << sizeof(brpc::Controller)
               << ", Size of vector is " << sizeof(std::vector<brpc::Controller>);
}

brpc::Channel g_chan;

TEST_F(ChannelTest, global_channel_should_quit_successfully) {
    g_chan.Init("bns://qa-pbrpc.SAT.tjyx", "rr", NULL);
}

TEST_F(ChannelTest, unused_call_id) {
    {
        brpc::Controller cntl;
    }
    {
        brpc::Controller cntl;
        cntl.Reset();
    }
    brpc::CallId cid1 = { 0 };
    {
        brpc::Controller cntl;
        cid1 = cntl.call_id();
    }
    ASSERT_EQ(EINVAL, bthread_id_error(cid1, ECANCELED));

    {
        brpc::CallId cid2 = { 0 };
        brpc::Controller cntl;
        cid2 = cntl.call_id();
        cntl.Reset();
        ASSERT_EQ(EINVAL, bthread_id_error(cid2, ECANCELED));
    }
}

TEST_F(ChannelTest, adaptive_connection_type) {
    brpc::AdaptiveConnectionType ctype;
    ASSERT_EQ(brpc::CONNECTION_TYPE_UNKNOWN, ctype);
    ASSERT_FALSE(ctype.has_error());
    ASSERT_STREQ("unknown", ctype.name());

    ctype = brpc::CONNECTION_TYPE_SINGLE;
    ASSERT_EQ(brpc::CONNECTION_TYPE_SINGLE, ctype);
    ASSERT_STREQ("single", ctype.name());

    ctype = "shorT";
    ASSERT_EQ(brpc::CONNECTION_TYPE_SHORT, ctype);
    ASSERT_STREQ("short", ctype.name());
    
    ctype = "PooLed";
    ASSERT_EQ(brpc::CONNECTION_TYPE_POOLED, ctype);
    ASSERT_STREQ("pooled", ctype.name());

    ctype = "SINGLE";
    ASSERT_EQ(brpc::CONNECTION_TYPE_SINGLE, ctype);
    ASSERT_FALSE(ctype.has_error());
    ASSERT_STREQ("single", ctype.name());

    ctype = "blah";
    ASSERT_EQ(brpc::CONNECTION_TYPE_UNKNOWN, ctype);
    ASSERT_TRUE(ctype.has_error());
    ASSERT_STREQ("unknown", ctype.name());

    ctype = "single";
    ASSERT_EQ(brpc::CONNECTION_TYPE_SINGLE, ctype);
    ASSERT_FALSE(ctype.has_error());
    ASSERT_STREQ("single", ctype.name());
}

TEST_F(ChannelTest, adaptive_protocol_type) {
    brpc::AdaptiveProtocolType ptype;
    ASSERT_EQ(brpc::PROTOCOL_UNKNOWN, ptype);
    ASSERT_STREQ("unknown", ptype.name());
    ASSERT_FALSE(ptype.has_param());
    ASSERT_EQ("", ptype.param());

    ptype = brpc::PROTOCOL_HTTP;
    ASSERT_EQ(brpc::PROTOCOL_HTTP, ptype);
    ASSERT_STREQ("http", ptype.name());
    ASSERT_FALSE(ptype.has_param());
    ASSERT_EQ("", ptype.param());

    ptype = "http:xyz ";
    ASSERT_EQ(brpc::PROTOCOL_HTTP, ptype);
    ASSERT_STREQ("http", ptype.name());
    ASSERT_TRUE(ptype.has_param());
    ASSERT_EQ("xyz ", ptype.param());

    ptype = "HuLu_pbRPC";
    ASSERT_EQ(brpc::PROTOCOL_HULU_PBRPC, ptype);
    ASSERT_STREQ("hulu_pbrpc", ptype.name());
    ASSERT_FALSE(ptype.has_param());
    ASSERT_EQ("", ptype.param());
    
    ptype = "blah";
    ASSERT_EQ(brpc::PROTOCOL_UNKNOWN, ptype);
    ASSERT_STREQ("blah", ptype.name());
    ASSERT_FALSE(ptype.has_param());
    ASSERT_EQ("", ptype.param());

    ptype = "Baidu_STD";
    ASSERT_EQ(brpc::PROTOCOL_BAIDU_STD, ptype);
    ASSERT_STREQ("baidu_std", ptype.name());
    ASSERT_FALSE(ptype.has_param());
    ASSERT_EQ("", ptype.param());
}

} //namespace
