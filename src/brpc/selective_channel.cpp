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


#include <map>
#include <gflags/gflags.h>
#include "bthread/bthread.h"                         // bthread_id_xx
#include "brpc/socket.h"                             // SocketUser
#include "brpc/load_balancer.h"                      // LoadBalancer
#include "brpc/details/controller_private_accessor.h"        // RPCSender
#include "brpc/selective_channel.h"
#include "brpc/global.h"


namespace brpc {

DEFINE_int32(channel_check_interval, 1, 
             "seconds between consecutive health-checking of unaccessible"
             " sub channels inside SelectiveChannel");

namespace schan {

// This map is generally very small, std::map may be good enough.
typedef std::map<ChannelBase*, Socket*> ChannelToIdMap;

// Representing a sub channel.
class SubChannel : public SocketUser {
public:
    ChannelBase* chan;

    // internal channel is deleted after the fake Socket is SetFailed
    void BeforeRecycle(Socket*) {
        delete chan;
        delete this;
    }

    int CheckHealth(Socket* ptr) {
        if (ptr->health_check_count() == 0) {
            LOG(INFO) << "Checking " << *chan << " chan=0x" << (void*)chan
                      << " Fake" << *ptr;
        }
        return chan->CheckHealth();
    }

    void AfterRevived(Socket* ptr) {
        LOG(INFO) << "Revived " << *chan << " chan=0x" << (void*)chan
                  << " Fake" << *ptr << " (Connectable)";
    }
};

int GetSubChannelWeight(SocketUser* u) {
    return static_cast<SubChannel*>(u)->chan->Weight();
}

// Load balance between fake sockets whose SocketUsers are sub channels.
class ChannelBalancer : public SharedLoadBalancer {
public:
    struct SelectOut {
        SelectOut() : need_feedback(false) {}

        ChannelBase* channel() {
            return static_cast<SubChannel*>(fake_sock->user())->chan;
        }
        
        SocketUniquePtr fake_sock;
        bool need_feedback;
    };
    
    ChannelBalancer() {}
    ~ChannelBalancer();
    int Init(const char* lb_name);
    int AddChannel(ChannelBase* sub_channel,
                   SelectiveChannel::ChannelHandle* handle);
    void RemoveAndDestroyChannel(SelectiveChannel::ChannelHandle handle);
    int SelectChannel(const LoadBalancer::SelectIn& in, SelectOut* out);
    int CheckHealth();
    void Describe(std::ostream& os, const DescribeOptions&);

private:
    butil::Mutex _mutex;
    // Find out duplicated sub channels.
    ChannelToIdMap _chan_map;
};

class SubDone;
class Sender;

struct Resource {
    Resource() : response(NULL), sub_done(NULL) {}
        
    google::protobuf::Message* response;
    SubDone* sub_done;
};

// The done to sub channels.
class SubDone : public google::protobuf::Closure {
public:
    explicit SubDone(Sender* owner)
        : _owner(owner)
        , _cid(INVALID_BTHREAD_ID)
        , _peer_id(INVALID_SOCKET_ID) {
    }
    ~SubDone() {}
    void Run();

    Sender* _owner;
    CallId _cid;
    SocketId _peer_id;
    Controller _cntl;
};

// The sender to intercept Controller::IssueRPC
class Sender : public RPCSender,
               public google::protobuf::Closure {
friend class SubDone;
public:
    Sender(Controller* cntl,
           const google::protobuf::Message* request,
           google::protobuf::Message* response,
           google::protobuf::Closure* user_done);
    ~Sender() { Clear(); }
    int IssueRPC(int64_t start_realtime_us);
    Resource PopFree();
    bool PushFree(const Resource& r);
    const Controller* SubController(int index) const;
    void Run();
    void Clear();

private:
    Controller* _main_cntl;
    const google::protobuf::Message* _request;
    google::protobuf::Message* _response;
    google::protobuf::Closure* _user_done;
    short _nfree;
    short _nalloc;
    bool _finished;
    Resource _free_resources[2];
    Resource _alloc_resources[2];
    SubDone _sub_done0;
};

// ===============================================

ChannelBalancer::~ChannelBalancer() {
    for (ChannelToIdMap::iterator
             it = _chan_map.begin(); it != _chan_map.end(); ++it) {
        SocketUniquePtr ptr(it->second); // Dereference
        it->second->ReleaseAdditionalReference();
    }
    _chan_map.clear();
}

int ChannelBalancer::Init(const char* lb_name) {
    return SharedLoadBalancer::Init(lb_name);
}

int ChannelBalancer::AddChannel(ChannelBase* sub_channel,
                                SelectiveChannel::ChannelHandle* handle) {
    if (NULL == sub_channel) {
        LOG(ERROR) << "Parameter[sub_channel] is NULL";
        return -1;
    }
    BAIDU_SCOPED_LOCK(_mutex);
    if (_chan_map.find(sub_channel) != _chan_map.end()) {
        LOG(ERROR) << "Duplicated sub_channel=" << sub_channel;
        return -1;
    }
    SubChannel* sub_chan = new (std::nothrow) SubChannel;
    if (sub_chan == NULL) {
        LOG(FATAL) << "Fail to to new SubChannel";
        return -1;
    }
    sub_chan->chan = sub_channel;
    SocketId sock_id;
    SocketOptions options;
    options.user = sub_chan;
    options.health_check_interval_s = FLAGS_channel_check_interval;
            
    if (Socket::Create(options, &sock_id) != 0) {
        delete sub_chan;
        LOG(ERROR) << "Fail to create fake socket for sub channel";
        return -1;
    }
    SocketUniquePtr ptr;
    CHECK_EQ(0, Socket::Address(sock_id, &ptr));
    if (!AddServer(ServerId(sock_id))) {
        LOG(ERROR) << "Duplicated sub_channel=" << sub_channel;
        // sub_chan will be deleted when the socket is recycled.
        ptr->SetFailed();
        return -1;
    }
    _chan_map[sub_channel]= ptr.release();  // Add reference.
    if (handle) {
        *handle = sock_id;
    }
    return 0;
}

void ChannelBalancer::RemoveAndDestroyChannel(SelectiveChannel::ChannelHandle handle) {
    if (!RemoveServer(ServerId(handle))) {
        return;
    }
    SocketUniquePtr ptr;
    const int rc = Socket::AddressFailedAsWell(handle, &ptr);
    if (rc >= 0) {
        SubChannel* sub = static_cast<SubChannel*>(ptr->user());
        {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK_EQ(1UL, _chan_map.erase(sub->chan));
        }
        {
            SocketUniquePtr ptr2(ptr.get()); // Dereference.
        }
        if (rc == 0) {
            ptr->ReleaseAdditionalReference();
        }
    }
}

inline int ChannelBalancer::SelectChannel(const LoadBalancer::SelectIn& in,
                                          SelectOut* out) {
    LoadBalancer::SelectOut sel_out(&out->fake_sock);
    const int rc = SelectServer(in, &sel_out);
    if (rc != 0) {
        return rc;
    }
    out->need_feedback = sel_out.need_feedback;
    return 0;
}

int ChannelBalancer::CheckHealth() {
    BAIDU_SCOPED_LOCK(_mutex);
    for (ChannelToIdMap::const_iterator it = _chan_map.begin();
         it != _chan_map.end(); ++it) {
        if (!it->second->Failed() &&
            it->first->CheckHealth() == 0) {
            return 0;
        }
    }
    return -1;
}

void ChannelBalancer::Describe(std::ostream& os,
                               const DescribeOptions& options) {
    BAIDU_SCOPED_LOCK(_mutex);
    if (!options.verbose) {
        os << _chan_map.size();
        return;
    }
    for (ChannelToIdMap::const_iterator it = _chan_map.begin();
         it != _chan_map.end(); ++it) {
        if (it != _chan_map.begin()) {
            os << ' ';
        }
        it->first->Describe(os, options);
    }
}

// ===================================

Sender::Sender(Controller* cntl,
               const google::protobuf::Message* request,
               google::protobuf::Message* response,
               google::protobuf::Closure* user_done)
    : _main_cntl(cntl)
    , _request(request)
    , _response(response)
    , _user_done(user_done)
    , _nfree(0)
    , _nalloc(0)
    , _finished(false)
    , _sub_done0(this) {
}

int Sender::IssueRPC(int64_t start_realtime_us) {
    _main_cntl->_current_call.need_feedback = false;
    LoadBalancer::SelectIn sel_in = { start_realtime_us,
                                      true,
                                      _main_cntl->has_request_code(),
                                      _main_cntl->_request_code,
                                      _main_cntl->_accessed };
    ChannelBalancer::SelectOut sel_out;
    const int rc = static_cast<ChannelBalancer*>(_main_cntl->_lb.get())
        ->SelectChannel(sel_in, &sel_out);
    if (rc != 0) {
        _main_cntl->SetFailed(rc, "Fail to select channel, %s", berror(rc));
        return -1;
    }
    DLOG(INFO) << "Selected channel=" << sel_out.channel() << ", size="
                << (_main_cntl->_accessed ? _main_cntl->_accessed->size() : 0);
    _main_cntl->_current_call.need_feedback = sel_out.need_feedback;
    _main_cntl->_current_call.peer_id = sel_out.fake_sock->id();

    Resource r = PopFree();
    if (r.sub_done == NULL) {
        CHECK(false) << "Impossible!";
        _main_cntl->SetFailed("Impossible happens");
        return -1;
    }
    r.sub_done->_cid = _main_cntl->current_id();
    r.sub_done->_peer_id = sel_out.fake_sock->id();
    Controller* sub_cntl = &r.sub_done->_cntl;
    // No need to count timeout. We already managed timeout in schan. If
    // timeout occurs, sub calls are canceled with ERPCTIMEDOUT.
    sub_cntl->_timeout_ms = -1;
    sub_cntl->_real_timeout_ms = _main_cntl->timeout_ms();

    // Inherit following fields of _main_cntl.
    // TODO(gejun): figure out a better way to maintain these fields.
    sub_cntl->set_connection_type(_main_cntl->connection_type());
    sub_cntl->set_type_of_service(_main_cntl->_tos);
    sub_cntl->set_request_compress_type(_main_cntl->request_compress_type());
    sub_cntl->set_log_id(_main_cntl->log_id());
    sub_cntl->set_request_code(_main_cntl->request_code());
    // Forward request attachment to the subcall
    sub_cntl->request_attachment().append(_main_cntl->request_attachment());
    
    sel_out.channel()->CallMethod(_main_cntl->_method,
                                  &r.sub_done->_cntl,
                                  _request,
                                  r.response,
                                  r.sub_done);
    return 0;
}

void SubDone::Run() {
    Controller* main_cntl = NULL;
    const int rc = bthread_id_lock(_cid, (void**)&main_cntl);
    if (rc != 0) {
        // _cid must be valid because schan does not dtor before cancelling
        // all sub calls.
        LOG(ERROR) << "Fail to lock correlation_id="
                   << _cid.value << ": " << berror(rc);
        return;
    }
    // NOTE: Copying gettable-but-settable fields which are generally set
    // during the RPC to reflect details.
    main_cntl->_remote_side = _cntl._remote_side;
    // connection_type may be changed during CallMethod. 
    main_cntl->set_connection_type(_cntl.connection_type());
    Resource r;
    r.response = _cntl._response;
    r.sub_done = this;
    if (!_owner->PushFree(r)) {
        return;
    }
    const int saved_error = main_cntl->ErrorCode();
    
    if (_cntl.Failed()) {
        if (_cntl.ErrorCode() == ENODATA || _cntl.ErrorCode() == EHOSTDOWN) {
            // LB could not find a server.
            Socket::SetFailed(_peer_id);  // trigger HC.
        }
        main_cntl->SetFailed(_cntl._error_text);
        main_cntl->_error_code = _cntl._error_code;
    } else {
        if (_cntl._response != main_cntl->_response) {
            main_cntl->_response->GetReflection()->Swap(
                main_cntl->_response, _cntl._response);
        }
    }
    const Controller::CompletionInfo info = { _cid, true };
    main_cntl->OnVersionedRPCReturned(info, false, saved_error);
}

void Sender::Run() {
    _finished = true;
    if (_nfree != _nalloc) {
        const int saved_nalloc = _nalloc;
        int error = (_main_cntl->ErrorCode() == ERPCTIMEDOUT ? ERPCTIMEDOUT : ECANCELED);
        CallId ids[_nalloc];
        for (int i = 0; i < _nalloc; ++i) {
            ids[i] = _alloc_resources[i].sub_done->_cntl.call_id();
        }
        CallId cid = _main_cntl->call_id();
        CHECK_EQ(0, bthread_id_unlock(cid));
        for (int i = 0; i < saved_nalloc; ++i) {
            bthread_id_error(ids[i], error);
        }
    } else {
        Clear();
    }
}

void Sender::Clear() {
    if (_main_cntl == NULL) {
        return;
    }
    delete _alloc_resources[1].response;
    delete _alloc_resources[1].sub_done;
    _alloc_resources[1] = Resource();
    const CallId cid = _main_cntl->call_id();
    _main_cntl = NULL;
    if (_user_done) {
        _user_done->Run();
    }
    bthread_id_unlock_and_destroy(cid);
}

inline Resource Sender::PopFree() {
    if (_nfree == 0) {
        if (_nalloc == 0) {
            Resource r;
            r.response = _response;
            r.sub_done = &_sub_done0;
            _alloc_resources[_nalloc++] = r;
            return r;
        } else if (_nalloc == 1) {
            Resource r;
            r.response = _response->New();
            r.sub_done = new SubDone(this);
            _alloc_resources[_nalloc++] = r;
            return r;
        } else {
            CHECK(false) << "nalloc=" << _nalloc;
            return Resource();
        }
    } else {
        Resource r = _free_resources[--_nfree];
        r.response->Clear();
        Controller& sub_cntl = r.sub_done->_cntl;
        ExcludedServers* saved_accessed = sub_cntl._accessed;
        sub_cntl._accessed = NULL;
        sub_cntl.Reset();
        sub_cntl._accessed = saved_accessed;
        return r;
    }
}

inline bool Sender::PushFree(const Resource& r) {
    if (_nfree < 2) {
        _free_resources[_nfree++] = r;
        if (_finished && _nfree == _nalloc) {
            Clear();
            return false;
        }
        return true;
    } else {
        CHECK(false) << "Impossible!";
        return false;
    }
}

inline const Controller* Sender::SubController(int index) const {
    if (index != 0) {
        return NULL;
    }
    for (int i = 0; i < _nfree; ++i) {
        if (!_free_resources[i].sub_done->_cntl.Failed()) {
            return &_free_resources[i].sub_done->_cntl;
        }
    }
    if (_nfree != 0) {
        return &_free_resources[_nfree - 1].sub_done->_cntl;
    }
    return NULL;
}

}  // namespace schan

const Controller* GetSubControllerOfSelectiveChannel(
    const RPCSender* sender, int index) {
    return static_cast<const schan::Sender*>(sender)->SubController(index);
}

static void PassSerializeRequest(butil::IOBuf*, Controller*,
                                 const google::protobuf::Message*) {
}

SelectiveChannel::SelectiveChannel() {}

SelectiveChannel::~SelectiveChannel() {}

int SelectiveChannel::Init(const char* lb_name, const ChannelOptions* options) {
    // Force naming services to register.
    GlobalInitializeOrDie();
    if (initialized()) {
        LOG(ERROR) << "Already initialized";
        return -1;
    }
    schan::ChannelBalancer* lb = new (std::nothrow) schan::ChannelBalancer;
    if (NULL == lb) {
        LOG(FATAL) << "Fail to new ChannelBalancer";
        return -1;
    }
    if (lb->Init(lb_name) != 0) {
        LOG(ERROR) << "Fail to init lb";
        delete lb;
        return -1;
    }
    _chan._lb.reset(lb);
    _chan._serialize_request = PassSerializeRequest;
    if (options) {
        _chan._options = *options;
        // Modify some fields to be consistent with behavior of schan.
        _chan._options.connection_type = CONNECTION_TYPE_UNKNOWN;
        _chan._options.succeed_without_server = true;
        _chan._options.auth = NULL;
    }
    _chan._options.protocol = PROTOCOL_UNKNOWN;
    return 0;
}

bool SelectiveChannel::initialized() const {
    return _chan._lb != NULL;
}

int SelectiveChannel::AddChannel(ChannelBase* sub_channel,
                                 ChannelHandle* handle) {
    schan::ChannelBalancer* lb =
        static_cast<schan::ChannelBalancer*>(_chan._lb.get());
    if (lb == NULL) {
        LOG(ERROR) << "You must call Init() to initialize a SelectiveChannel";
        return -1;
    }
    return lb->AddChannel(sub_channel, handle);
}

void SelectiveChannel::RemoveAndDestroyChannel(ChannelHandle handle) {
    schan::ChannelBalancer* lb =
        static_cast<schan::ChannelBalancer*>(_chan._lb.get());
    if (lb == NULL) {
        LOG(ERROR) << "You must call Init() to initialize a SelectiveChannel";
        return;
    }
    lb->RemoveAndDestroyChannel(handle);
}

void SelectiveChannel::CallMethod(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::RpcController* controller_base,
    const google::protobuf::Message* request,
    google::protobuf::Message* response,
    google::protobuf::Closure* user_done) {
    Controller* cntl = static_cast<Controller*>(controller_base);
    if (!initialized()) {
        cntl->SetFailed(EINVAL, "SelectiveChannel=%p is not initialized yet",
                        this);
    }
    schan::Sender* sndr = new schan::Sender(cntl, request, response, user_done);
    cntl->_sender = sndr;
    cntl->add_flag(Controller::FLAGS_DESTROY_CID_IN_DONE);
    const CallId cid = cntl->call_id();
    _chan.CallMethod(method, cntl, request, response, sndr);
    if (user_done == NULL) {
        Join(cid);
        cntl->OnRPCEnd(butil::gettimeofday_us());
    }
}

int SelectiveChannel::CheckHealth() {
    schan::ChannelBalancer* lb =
        static_cast<schan::ChannelBalancer*>(_chan._lb.get());
    if (lb) {
        return lb->CheckHealth();
    }
    return -1;
}

void SelectiveChannel::Describe(
    std::ostream& os, const DescribeOptions& options) const {
    os << "SelectiveChannel[";
    if (_chan._lb != NULL) {
        _chan._lb->Describe(os, options);
    } else {
        os << "uninitialized";
    }
    os << ']';
}

} // namespace brpc
