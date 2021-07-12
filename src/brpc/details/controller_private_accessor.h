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

#ifndef BRPC_CONTROLLER_PRIVATE_ACCESSOR_H
#define BRPC_CONTROLLER_PRIVATE_ACCESSOR_H

// This is an rpc-internal file.

#include "brpc/socket.h"
#include "brpc/controller.h"
#include "brpc/stream.h"

namespace google {
namespace protobuf {
class Message;
}
}


namespace brpc {

class AuthContext;

// A wrapper to access some private methods/fields of `Controller'
// This is supposed to be used by internal RPC protocols ONLY
class ControllerPrivateAccessor {
public:
    explicit ControllerPrivateAccessor(Controller* cntl) {
        _cntl = cntl;
    }

    void OnResponse(CallId id, int saved_error) {
        const Controller::CompletionInfo info = { id, true };
        _cntl->OnVersionedRPCReturned(info, false, saved_error);
    }

    ControllerPrivateAccessor &set_peer_id(SocketId peer_id) {
        _cntl->_current_call.peer_id = peer_id;
        return *this;
    }

    Socket* get_sending_socket() {
        return _cntl->_current_call.sending_sock.get();
    }

    int64_t real_timeout_ms() {
        return _cntl->_real_timeout_ms;
    }

    void move_in_server_receiving_sock(SocketUniquePtr& ptr) {
        CHECK(_cntl->_current_call.sending_sock == NULL);
        _cntl->_current_call.sending_sock.reset(ptr.release());
    }

    StreamUserData* get_stream_user_data() {
        return _cntl->_current_call.stream_user_data;
    }

    ControllerPrivateAccessor &set_security_mode(bool security_mode) {
        _cntl->set_flag(Controller::FLAGS_SECURITY_MODE, security_mode);
        return *this;
    }

    ControllerPrivateAccessor &set_remote_side(const butil::EndPoint& pt) {
        _cntl->_remote_side = pt;
        return *this;
    }

    ControllerPrivateAccessor &set_local_side(const butil::EndPoint& pt) {
        _cntl->_local_side = pt;
        return *this;
    }
 
    ControllerPrivateAccessor &set_auth_context(const AuthContext* ctx) {
        _cntl->set_auth_context(ctx);
        return *this;
    }

    ControllerPrivateAccessor &set_span(Span* span) {
        _cntl->_span = span;
        return *this;
    }
    
    ControllerPrivateAccessor &set_request_protocol(ProtocolType protocol) {
        _cntl->_request_protocol = protocol;
        return *this;
    }
    
    Span* span() const { return _cntl->_span; }

    uint32_t pipelined_count() const { return _cntl->_pipelined_count; }
    void set_pipelined_count(uint32_t count) {  _cntl->_pipelined_count = count; }

    ControllerPrivateAccessor& set_server(const Server* server) {
        _cntl->_server = server;
        return *this;
    }

    // Pass the owership of |settings| to _cntl, while is going to be
    // destroyed in Controller::Reset()
    void set_remote_stream_settings(StreamSettings *settings) {
        _cntl->_remote_stream_settings = settings;
    }
    StreamSettings* remote_stream_settings() {
        return _cntl->_remote_stream_settings;
    }

    StreamId request_stream() { return _cntl->_request_stream; }
    StreamId response_stream() { return _cntl->_response_stream; }

    void set_method(const google::protobuf::MethodDescriptor* method) 
    { _cntl->_method = method; }

    void set_readable_progressive_attachment(ReadableProgressiveAttachment* s)
    { _cntl->_rpa.reset(s); }

    void add_with_auth() {
        _cntl->add_flag(Controller::FLAGS_REQUEST_WITH_AUTH);
    }

    void clear_with_auth() {
        _cntl->clear_flag(Controller::FLAGS_REQUEST_WITH_AUTH);
    }

    std::string& protocol_param() { return _cntl->protocol_param(); }
    const std::string& protocol_param() const { return _cntl->protocol_param(); }

    // Note: This function can only be called in server side. The deadline of client
    // side is properly set in the RPC sending path.
    void set_deadline_us(int64_t deadline_us) { _cntl->_deadline_us = deadline_us; }

    ControllerPrivateAccessor& set_begin_time_us(int64_t begin_time_us) {
        _cntl->_begin_time_us = begin_time_us;
        _cntl->_end_time_us = UNSET_MAGIC_NUM;
        return *this;
    }

    ControllerPrivateAccessor& set_health_check_call() {
        _cntl->add_flag(Controller::FLAGS_HEALTH_CHECK_CALL);
        return *this;
    }

private:
    Controller* _cntl;
};

// Inherit this class to intercept Controller::IssueRPC. This is an internal
// utility only useable by brpc developers.
class RPCSender {
public:
    virtual ~RPCSender() {}
    virtual int IssueRPC(int64_t start_realtime_us) = 0;
};

} // namespace brpc


#endif // BRPC_CONTROLLER_PRIVATE_ACCESSOR_H
