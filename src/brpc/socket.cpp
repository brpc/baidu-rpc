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


#include "butil/compat.h"                        // OS_MACOSX
#include <openssl/ssl.h>
#include <openssl/err.h>
#ifdef USE_MESALINK
#include <mesalink/openssl/ssl.h>
#include <mesalink/openssl/err.h>
#include <mesalink/openssl/x509.h>
#endif
#include <netinet/tcp.h>                         // getsockopt
#include <gflags/gflags.h>
#include "bthread/unstable.h"                    // bthread_timer_del
#include "butil/fd_utility.h"                     // make_non_blocking
#include "butil/fd_guard.h"                       // fd_guard
#include "butil/time.h"                           // cpuwide_time_us
#include "butil/object_pool.h"                    // get_object
#include "butil/logging.h"                        // CHECK
#include "butil/macros.h"
#include "butil/class_name.h"                     // butil::class_name
#include "brpc/log.h"
#include "brpc/reloadable_flags.h"          // BRPC_VALIDATE_GFLAG
#include "brpc/errno.pb.h"
#include "brpc/event_dispatcher.h"          // RemoveConsumer
#include "brpc/socket.h"
#include "brpc/describable.h"               // Describable
#include "brpc/circuit_breaker.h"           // CircuitBreaker
#include "brpc/input_messenger.h"
#include "brpc/details/sparse_minute_counter.h"
#include "brpc/stream_impl.h"
#include "brpc/shared_object.h"
#include "brpc/policy/rtmp_protocol.h"  // FIXME
#include "brpc/periodic_task.h"
#include "brpc/details/health_check.h"
#if defined(OS_MACOSX)
#include <sys/event.h>
#endif

namespace bthread {
size_t __attribute__((weak))
get_sizes(const bthread_id_list_t* list, size_t* cnt, size_t n);
}


namespace brpc {

// NOTE: This flag was true by default before r31206. Connected to somewhere
// is not an important event now, we can check the connection in /connections
// if we're in doubt.
DEFINE_bool(log_connected, false, "Print log when a connection is established");
BRPC_VALIDATE_GFLAG(log_connected, PassValidate);

DEFINE_bool(log_idle_connection_close, false,
            "Print log when an idle connection is closed");
BRPC_VALIDATE_GFLAG(log_idle_connection_close, PassValidate);

DEFINE_int32(socket_recv_buffer_size, -1, 
            "Set the recv buffer size of socket if this value is positive");

// Default value of SNDBUF is 2500000 on most machines.
DEFINE_int32(socket_send_buffer_size, -1, 
            "Set send buffer size of sockets if this value is positive");

DEFINE_int32(ssl_bio_buffer_size, 16*1024, "Set buffer size for SSL read/write");

DEFINE_int64(socket_max_unwritten_bytes, 64 * 1024 * 1024,
             "Max unwritten bytes in each socket, if the limit is reached,"
             " Socket.Write fails with EOVERCROWDED");

DEFINE_int32(max_connection_pool_size, 100,
             "Max number of pooled connections to a single endpoint");
BRPC_VALIDATE_GFLAG(max_connection_pool_size, PassValidate);

DEFINE_int32(connect_timeout_as_unreachable, 3,
             "If the socket failed to connect due to ETIMEDOUT for so many "
             "times *continuously*, the error is changed to ENETUNREACH which "
             "fails the main socket as well when this socket is pooled.");

DECLARE_int32(health_check_timeout_ms);

static bool validate_connect_timeout_as_unreachable(const char*, int32_t v) {
    return v >= 2 && v < 1000/*large enough*/;
}
BRPC_VALIDATE_GFLAG(connect_timeout_as_unreachable,
                         validate_connect_timeout_as_unreachable);

const int WAIT_EPOLLOUT_TIMEOUT_MS = 50;

class BAIDU_CACHELINE_ALIGNMENT SocketPool {
friend class Socket;
public:
    explicit SocketPool(const SocketOptions& opt);
    ~SocketPool();

    // Get an address-able socket. If the pool is empty, create one.
    // Returns 0 on success.
    int GetSocket(SocketUniquePtr* ptr);
    
    // Return a socket (which was returned by GetSocket) back to the pool,
    // if the pool is full, setfail the socket directly.
    void ReturnSocket(Socket* sock);
    
    // Get all pooled sockets inside.
    void ListSockets(std::vector<SocketId>* list, size_t max_count);
    
private:
    // options used to create this instance
    SocketOptions _options;
    butil::Mutex _mutex;
    std::vector<SocketId> _pool;
    butil::EndPoint _remote_side;
    butil::atomic<int> _numfree; // #free sockets in all sub pools.
    butil::atomic<int> _numinflight; // #inflight sockets in all sub pools.
};

// NOTE: sizeof of this class is 1200 bytes. If we have 10K sockets, total
// memory is 12MB, not lightweight, but acceptable.
struct ExtendedSocketStat : public SocketStat {
    // For computing stat.
    size_t last_in_size;
    size_t last_in_num_messages;
    size_t last_out_size;
    size_t last_out_num_messages;

    struct Sampled {
        uint32_t in_size_s;
        uint32_t in_num_messages_s;
        uint32_t out_size_s;
        uint32_t out_num_messages_s;
    };
    SparseMinuteCounter<Sampled> _minute_counter;

    ExtendedSocketStat()
        : last_in_size(0)
        , last_in_num_messages(0)
        , last_out_size(0)
        , last_out_num_messages(0) {
        memset((SocketStat*)this, 0, sizeof(SocketStat));
    }
};

// Shared by main socket and derivative sockets.
class Socket::SharedPart : public SharedObject {
public:
    // A pool of sockets on which only a request can be sent, corresponding
    // to CONNECTION_TYPE_POOLED. When RPC begins, it picks one socket from
    // this pool and send the request, when the RPC ends, it returns the
    // socket back to this pool.
    // Before rev <= r32136, the pool is managed globally in socket_map.cpp
    // which has the disadvantage that accesses to different pools contend
    // with each other.
    butil::atomic<SocketPool*> socket_pool;
    
    // The socket newing this object.
    SocketId creator_socket_id;

    // Counting number of continuous ETIMEDOUT
    butil::atomic<int> num_continuous_connect_timeouts;

    // _in_size, _in_num_messages, _out_size, _out_num_messages of pooled
    // sockets are counted into the corresponding fields in their _main_socket.
    butil::atomic<size_t> in_size;
    butil::atomic<size_t> in_num_messages;
    butil::atomic<size_t> out_size;
    butil::atomic<size_t> out_num_messages;

    // For computing stats.
    ExtendedSocketStat* extended_stat;

    CircuitBreaker circuit_breaker;

    butil::atomic<uint64_t> recent_error_count;

    explicit SharedPart(SocketId creator_socket_id);
    ~SharedPart();

    // Call this method every second (roughly)
    void UpdateStatsEverySecond(int64_t now_ms);
};

Socket::SharedPart::SharedPart(SocketId creator_socket_id2)
    : socket_pool(NULL)
    , creator_socket_id(creator_socket_id2)
    , num_continuous_connect_timeouts(0)
    , in_size(0)
    , in_num_messages(0)
    , out_size(0)
    , out_num_messages(0)
    , extended_stat(NULL)
    , recent_error_count(0) {
}

Socket::SharedPart::~SharedPart() {
    delete extended_stat;
    extended_stat = NULL;
    delete socket_pool.exchange(NULL, butil::memory_order_relaxed);
}

void Socket::SharedPart::UpdateStatsEverySecond(int64_t now_ms) {
    ExtendedSocketStat* stat = extended_stat;
    if (stat == NULL) {
        stat = new (std::nothrow) ExtendedSocketStat;
        if (stat == NULL) {
            return;
        }
        extended_stat = stat;
    }

    // Save volatile counters.
    const size_t in_sz = in_size.load(butil::memory_order_relaxed);
    const size_t in_nmsg = in_num_messages.load(butil::memory_order_relaxed);
    const size_t out_sz = out_size.load(butil::memory_order_relaxed);
    const size_t out_nmsg = out_num_messages.load(butil::memory_order_relaxed);

    // Notice that we don't normalize any data, mainly because normalization
    // often make data inaccurate and confuse users. This assumes that this
    // function is called exactly every second. This may not be true when the
    // running machine gets very busy. TODO(gejun): Figure out a method to
    // selectively normalize data when the calling interval is far from 1 second.
    stat->in_size_s = in_sz - stat->last_in_size;
    stat->in_num_messages_s = in_nmsg - stat->last_in_num_messages;
    stat->out_size_s = out_sz - stat->last_out_size;
    stat->out_num_messages_s = out_nmsg - stat->last_out_num_messages;
    
    stat->last_in_size = in_sz;
    stat->last_in_num_messages = in_nmsg;
    stat->last_out_size = out_sz;
    stat->last_out_num_messages = out_nmsg;

    ExtendedSocketStat::Sampled popped;
    if (stat->in_size_s |/*bitwise or*/
        stat->in_num_messages_s |
        stat->out_size_s |
        stat->out_num_messages_s) {
        ExtendedSocketStat::Sampled s = {
            stat->in_size_s, stat->in_num_messages_s,
            stat->out_size_s, stat->out_num_messages_s
        };
        stat->in_size_m += s.in_size_s;
        stat->in_num_messages_m += s.in_num_messages_s;
        stat->out_size_m += s.out_size_s;
        stat->out_num_messages_m += s.out_num_messages_s;
        if (stat->_minute_counter.Add(now_ms, s, &popped)) {
            stat->in_size_m -= popped.in_size_s;
            stat->in_num_messages_m -= popped.in_num_messages_s;
            stat->out_size_m -= popped.out_size_s;
            stat->out_num_messages_m -= popped.out_num_messages_s;
        }
    }
    while (stat->_minute_counter.TryPop(now_ms, &popped)) {
        stat->in_size_m -= popped.in_size_s;
        stat->in_num_messages_m -= popped.in_num_messages_s;
        stat->out_size_m -= popped.out_size_s;
        stat->out_num_messages_m -= popped.out_num_messages_s;
    }
}

SocketVarsCollector* g_vars = NULL;

static pthread_once_t s_create_vars_once = PTHREAD_ONCE_INIT;
static void CreateVars() {
    g_vars = new SocketVarsCollector;
}

void Socket::CreateVarsOnce() {
    CHECK_EQ(0, pthread_once(&s_create_vars_once, CreateVars));
}

// Used by ConnectionService
int64_t GetChannelConnectionCount() {
    if (g_vars) {
        return g_vars->channel_conn.get_value();
    }
    return 0;
}

bool Socket::CreatedByConnect() const {
    return _user == static_cast<SocketUser*>(get_client_side_messenger());
}

SocketMessage* const DUMMY_USER_MESSAGE = (SocketMessage*)0x1;
const uint32_t MAX_PIPELINED_COUNT = 32768;

struct BAIDU_CACHELINE_ALIGNMENT Socket::WriteRequest {
    static WriteRequest* const UNCONNECTED;
    
    butil::IOBuf data;
    WriteRequest* next;
    bthread_id_t id_wait;
    Socket* socket;
    
    uint32_t pipelined_count() const {
        return (_pc_and_udmsg >> 48) & 0x7FFF;
    }
    bool is_with_auth() const {
        return _pc_and_udmsg & 0x8000000000000000ULL;
    }
    void clear_pipelined_count_and_with_auth() {
        _pc_and_udmsg &= 0xFFFFFFFFFFFFULL;
    }
    SocketMessage* user_message() const {
        return (SocketMessage*)(_pc_and_udmsg & 0xFFFFFFFFFFFFULL);
    }
    void clear_user_message() {
        _pc_and_udmsg &= 0xFFFF000000000000ULL;
    }
    void set_pipelined_count_and_user_message(
        uint32_t pc, SocketMessage* msg, bool with_auth) {
        if (with_auth) {
          pc |= (1 << 15);
        }
        _pc_and_udmsg = ((uint64_t)pc << 48) | (uint64_t)(uintptr_t)msg;
    }

    bool reset_pipelined_count_and_user_message() {
        SocketMessage* msg = user_message();
        if (msg) {
            if (msg != DUMMY_USER_MESSAGE) {
                butil::IOBuf dummy_buf;
                // We don't care about the return value since the request
                // is already failed.
                (void)msg->AppendAndDestroySelf(&dummy_buf, NULL);
            }
            set_pipelined_count_and_user_message(0, NULL, false);
            return true;
        }
        return false;
    }

    // Register pipelined_count and user_message
    void Setup(Socket* s);
    
private:
    uint64_t _pc_and_udmsg;
};

void Socket::WriteRequest::Setup(Socket* s) {
    SocketMessage* msg = user_message();
    if (msg) {
        clear_user_message();
        if (msg != DUMMY_USER_MESSAGE) {
            butil::Status st = msg->AppendAndDestroySelf(&data, s);
            if (!st.ok()) {
                // Abandon the request.
                data.clear();
                bthread_id_error2(id_wait, st.error_code(), st.error_cstr());
                return;
            }
        }
        const int64_t before_write =
            s->_unwritten_bytes.fetch_add(data.size(), butil::memory_order_relaxed);
        if (before_write + (int64_t)data.size() >= FLAGS_socket_max_unwritten_bytes) {
            s->_overcrowded = true;
        }
    }
    const uint32_t pc = pipelined_count();
    if (pc) {
        // For positional correspondence between responses and requests,
        // which is common in cache servers: memcache, redis...
        // The struct will be popped when reading a message from the socket.
        PipelinedInfo pi;
        pi.count = pc;
        pi.with_auth = is_with_auth();
        pi.id_wait = id_wait;
        clear_pipelined_count_and_with_auth(); // avoid being pushed again
        s->PushPipelinedInfo(pi);
    }
}

Socket::WriteRequest* const Socket::WriteRequest::UNCONNECTED =
    (Socket::WriteRequest*)(intptr_t)-1;

class Socket::EpollOutRequest : public SocketUser {
public:
    EpollOutRequest() : fd(-1), timer_id(0)
                      , on_epollout_event(NULL), data(NULL) {}

    ~EpollOutRequest() {
        // Remove the timer at last inside destructor to avoid
        // race with the place that registers the timer
        if (timer_id) {
            bthread_timer_del(timer_id);
            timer_id = 0;
        }
    }
    
    void BeforeRecycle(Socket*) {
        // Recycle itself
        delete this;
    }

    int fd;
    bthread_timer_t timer_id;
    int (*on_epollout_event)(int fd, int err, void* data);
    void* data;
};

static const uint64_t AUTH_FLAG = (1ul << 32);

Socket::Socket(Forbidden)
    // must be even because Address() relies on evenness of version
    : _versioned_ref(0)
    , _shared_part(NULL)
    , _nevent(0)
    , _keytable_pool(NULL)
    , _fd(-1)
    , _tos(0)
    , _reset_fd_real_us(-1)
    , _on_edge_triggered_events(NULL)
    , _user(NULL)
    , _conn(NULL)
    , _this_id(0)
    , _preferred_index(-1)
    , _hc_count(0)
    , _last_msg_size(0)
    , _avg_msg_size(0)
    , _last_readtime_us(0)
    , _parsing_context(NULL)
    , _correlation_id(0)
    , _health_check_interval_s(-1)
    , _ninprocess(1)
    , _auth_flag_error(0)
    , _auth_id(INVALID_BTHREAD_ID)
    , _auth_context(NULL)
    , _ssl_state(SSL_UNKNOWN)
    , _ssl_session(NULL)
    , _connection_type_for_progressive_read(CONNECTION_TYPE_UNKNOWN)
    , _controller_released_socket(false)
    , _overcrowded(false)
    , _fail_me_at_server_stop(false)
    , _logoff_flag(false)
    , _recycle_flag(false)
    , _error_code(0)
    , _pipeline_q(NULL)
    , _last_writetime_us(0)
    , _unwritten_bytes(0)
    , _epollout_butex(NULL)
    , _write_head(NULL)
    , _stream_set(NULL)
    , _ninflight_app_health_check(0)
{
    CreateVarsOnce();
    pthread_mutex_init(&_id_wait_list_mutex, NULL);
    _epollout_butex = bthread::butex_create_checked<butil::atomic<int> >();
}

Socket::~Socket() {
    pthread_mutex_destroy(&_id_wait_list_mutex);
    bthread::butex_destroy(_epollout_butex);
}

void Socket::ReturnSuccessfulWriteRequest(Socket::WriteRequest* p) {
    DCHECK(p->data.empty());
    AddOutputMessages(1);
    const bthread_id_t id_wait = p->id_wait;
    butil::return_object(p);
    if (id_wait != INVALID_BTHREAD_ID) {
        NotifyOnFailed(id_wait);
    }
}

void Socket::ReturnFailedWriteRequest(Socket::WriteRequest* p, int error_code,
                                      const std::string& error_text) {
    if (!p->reset_pipelined_count_and_user_message()) {
        CancelUnwrittenBytes(p->data.size());
    }
    p->data.clear();  // data is probably not written.
    const bthread_id_t id_wait = p->id_wait;
    butil::return_object(p);
    if (id_wait != INVALID_BTHREAD_ID) {
        bthread_id_error2(id_wait, error_code, error_text);
    }
}

Socket::WriteRequest* Socket::ReleaseWriteRequestsExceptLast(
    Socket::WriteRequest* req, int error_code, const std::string& error_text) {
    WriteRequest* p = req;
    while (p->next != NULL) {
        WriteRequest* const saved_next = p->next;
        ReturnFailedWriteRequest(p, error_code, error_text);
        p = saved_next;
    }
    return p;
}

void Socket::ReleaseAllFailedWriteRequests(Socket::WriteRequest* req) {
    CHECK(Failed());
    pthread_mutex_lock(&_id_wait_list_mutex);
    const int error_code = non_zero_error_code();
    const std::string error_text = _error_text;
    pthread_mutex_unlock(&_id_wait_list_mutex);
    // Notice that `req' is not tail if Address after IsWriteComplete fails.
    do {
        req = ReleaseWriteRequestsExceptLast(req, error_code, error_text);
        if (!req->reset_pipelined_count_and_user_message()) {
            CancelUnwrittenBytes(req->data.size());
        }
        req->data.clear();  // MUST, otherwise IsWriteComplete is false
    } while (!IsWriteComplete(req, true, NULL));
    ReturnFailedWriteRequest(req, error_code, error_text);
}

int Socket::ResetFileDescriptor(int fd) {
    // Reset message sizes when fd is changed.
    _last_msg_size = 0;
    _avg_msg_size = 0;
    // MUST store `_fd' before adding itself into epoll device to avoid
    // race conditions with the callback function inside epoll
    _fd.store(fd, butil::memory_order_release);
    _reset_fd_real_us = butil::gettimeofday_us();
    if (!ValidFileDescriptor(fd)) {
        return 0;
    }
    // OK to fail, non-socket fd does not support this.
    if (butil::get_local_side(fd, &_local_side) != 0) {
        _local_side = butil::EndPoint();
    }

    // FIXME : close-on-exec should be set by new syscalls or worse: set right
    // after fd-creation syscall. Setting at here has higher probabilities of
    // race condition.
    butil::make_close_on_exec(fd);

    // Make the fd non-blocking.
    if (butil::make_non_blocking(fd) != 0) {
        PLOG(ERROR) << "Fail to set fd=" << fd << " to non-blocking";
        return -1;
    }
    // turn off nagling.
    // OK to fail, namely unix domain socket does not support this.
    butil::make_no_delay(fd);
    if (_tos > 0 &&
        setsockopt(fd, IPPROTO_IP, IP_TOS, &_tos, sizeof(_tos)) < 0) {
        PLOG(FATAL) << "Fail to set tos of fd=" << fd << " to " << _tos;
    }

    if (FLAGS_socket_send_buffer_size > 0) {
        int buff_size = FLAGS_socket_send_buffer_size;
        socklen_t size = sizeof(buff_size);
        if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buff_size, size) != 0) {
            PLOG(FATAL) << "Fail to set sndbuf of fd=" << fd << " to " 
                        << buff_size;
        }
    }

    if (FLAGS_socket_recv_buffer_size > 0) {
        int buff_size = FLAGS_socket_recv_buffer_size;
        socklen_t size = sizeof(buff_size);
        if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buff_size, size) != 0) {
            PLOG(FATAL) << "Fail to set rcvbuf of fd=" << fd << " to " 
                        << buff_size;
        }
    }

    if (_on_edge_triggered_events) {
        if (GetGlobalEventDispatcher(fd).AddConsumer(id(), fd) != 0) {
            PLOG(ERROR) << "Fail to add SocketId=" << id() 
                        << " into EventDispatcher";
            _fd.store(-1, butil::memory_order_release);
            return -1;
        }
    }
    return 0;
}

// SocketId = 32-bit version + 32-bit slot.
//   version: from version part of _versioned_nref, must be an EVEN number.
//   slot: designated by ResourcePool.
int Socket::Create(const SocketOptions& options, SocketId* id) {
    butil::ResourceId<Socket> slot;
    Socket* const m = butil::get_resource(&slot, Forbidden());
    if (m == NULL) {
        LOG(FATAL) << "Fail to get_resource<Socket>";
        return -1;
    }
    g_vars->nsocket << 1;
    CHECK(NULL == m->_shared_part.load(butil::memory_order_relaxed));
    m->_nevent.store(0, butil::memory_order_relaxed);
    m->_keytable_pool = options.keytable_pool;
    m->_tos = 0;
    m->_remote_side = options.remote_side;
    m->_on_edge_triggered_events = options.on_edge_triggered_events;
    m->_user = options.user;
    m->_conn = options.conn;
    m->_app_connect = options.app_connect;
    // nref can be non-zero due to concurrent AddressSocket().
    // _this_id will only be used in destructor/Destroy of referenced
    // slots, which is safe and properly fenced. Although it's better
    // to put the id into SocketUniquePtr.
    m->_this_id = MakeSocketId(
            VersionOfVRef(m->_versioned_ref.fetch_add(
                    1, butil::memory_order_release)), slot);
    if (options.remote_side == butil::EndPoint()) {
        // Make sure that remote_side is different from each other by default,
        // which is helpful in fake socket. 
        m->_remote_side.ip = { static_cast<uint32_t>(m->_this_id >> 32) };
        m->_remote_side.port = m->_this_id & 0x0000ffff;
    }
    m->_preferred_index = -1;
    m->_hc_count = 0;
    CHECK(m->_read_buf.empty());
    const int64_t cpuwide_now = butil::cpuwide_time_us();
    m->_last_readtime_us.store(cpuwide_now, butil::memory_order_relaxed);
    m->reset_parsing_context(options.initial_parsing_context);
    m->_correlation_id = 0;
    m->_health_check_interval_s = options.health_check_interval_s;
    m->_ninprocess.store(1, butil::memory_order_relaxed);
    m->_auth_flag_error.store(0, butil::memory_order_relaxed);
    const int rc2 = bthread_id_create(&m->_auth_id, NULL, NULL);
    if (rc2) {
        LOG(ERROR) << "Fail to create auth_id: " << berror(rc2);
        m->SetFailed(rc2, "Fail to create auth_id: %s", berror(rc2));
        return -1;
    }
    // Disable SSL check if there is no SSL context
    m->_ssl_state = (options.initial_ssl_ctx == NULL ? SSL_OFF : SSL_UNKNOWN);
    m->_ssl_session = NULL;
    m->_ssl_ctx = options.initial_ssl_ctx;
    m->_connection_type_for_progressive_read = CONNECTION_TYPE_UNKNOWN;
    m->_controller_released_socket.store(false, butil::memory_order_relaxed);
    m->_overcrowded = false;
    // May be non-zero for RTMP connections.
    m->_fail_me_at_server_stop = false;
    m->_logoff_flag.store(false, butil::memory_order_relaxed);
    m->_recycle_flag.store(false, butil::memory_order_relaxed);
    m->_error_code = 0;
    m->_error_text.clear();
    m->_agent_socket_id.store(INVALID_SOCKET_ID, butil::memory_order_relaxed);
    m->_ninflight_app_health_check.store(0, butil::memory_order_relaxed);
    // NOTE: last two params are useless in bthread > r32787
    const int rc = bthread_id_list_init(&m->_id_wait_list, 512, 512);
    if (rc) {
        LOG(ERROR) << "Fail to init _id_wait_list: " << berror(rc);
        m->SetFailed(rc, "Fail to init _id_wait_list: %s", berror(rc));
        return -1;
    }
    m->_last_writetime_us.store(cpuwide_now, butil::memory_order_relaxed);
    m->_unwritten_bytes.store(0, butil::memory_order_relaxed);
    CHECK(NULL == m->_write_head.load(butil::memory_order_relaxed));
    // Must be last one! Internal fields of this Socket may be access
    // just after calling ResetFileDescriptor.
    if (m->ResetFileDescriptor(options.fd) != 0) {
        const int saved_errno = errno;
        PLOG(ERROR) << "Fail to ResetFileDescriptor";
        m->SetFailed(saved_errno, "Fail to ResetFileDescriptor: %s", 
                     berror(saved_errno));
        return -1;
    }
    *id = m->_this_id;
    return 0;
}

int Socket::WaitAndReset(int32_t expected_nref) {
    const uint32_t id_ver = VersionOfSocketId(_this_id);
    uint64_t vref;
    // Wait until nref == expected_nref.
    while (1) {
        // The acquire fence pairs with release fence in Dereference to avoid
        // inconsistent states to be seen by others.
        vref = _versioned_ref.load(butil::memory_order_acquire);
        if (VersionOfVRef(vref) != id_ver + 1) {
            LOG(WARNING) << "SocketId=" << _this_id << " is already alive or recycled";
            return -1;
        }
        if (NRefOfVRef(vref) > expected_nref) {
            if (bthread_usleep(1000L/*FIXME*/) < 0) {
                PLOG_IF(FATAL, errno != ESTOP) << "Fail to sleep";
                return -1;
            }
        } else if (NRefOfVRef(vref) < expected_nref) {
            RPC_VLOG << "SocketId=" << _this_id 
                     << " was abandoned during health checking";
            return -1;
        } else {
            break;
        }
    }

    // It's safe to close previous fd (provided expected_nref is correct).
    const int prev_fd = _fd.exchange(-1, butil::memory_order_relaxed);
    if (ValidFileDescriptor(prev_fd)) {
        if (_on_edge_triggered_events != NULL) {
            GetGlobalEventDispatcher(prev_fd).RemoveConsumer(prev_fd);
        }
        close(prev_fd);
        if (CreatedByConnect()) {
            g_vars->channel_conn << -1;
        }
    }
    _local_side = butil::EndPoint();
    if (_ssl_session) {
        SSL_free(_ssl_session);
        _ssl_session = NULL;
    }        
    _ssl_state = SSL_UNKNOWN;
    _nevent.store(0, butil::memory_order_relaxed);
    // parsing_context is very likely to be associated with the fd,
    // removing it is a safer choice and required by http2.
    reset_parsing_context(NULL);
    // Must clear _read_buf otehrwise even if the connections is recovered,
    // the kept old data is likely to make parsing fail.
    _read_buf.clear();
    _ninprocess.store(1, butil::memory_order_relaxed);
    _auth_flag_error.store(0, butil::memory_order_relaxed);
    bthread_id_error(_auth_id, 0);
    const int rc = bthread_id_create(&_auth_id, NULL, NULL);
    if (rc != 0) {
        LOG(FATAL) << "Fail to create _auth_id, " << berror(rc);
        return -1;
    }

    const int64_t cpuwide_now = butil::cpuwide_time_us();
    _last_readtime_us.store(cpuwide_now, butil::memory_order_relaxed);
    _last_writetime_us.store(cpuwide_now, butil::memory_order_relaxed);
    _logoff_flag.store(false, butil::memory_order_relaxed);
    {
        BAIDU_SCOPED_LOCK(_pipeline_mutex);
        if (_pipeline_q) {
            _pipeline_q->clear();
        }
    }

    SharedPart* sp = GetSharedPart();
    if (sp) {
        sp->circuit_breaker.Reset();
        sp->recent_error_count.store(0, butil::memory_order_relaxed);
    }
    return 0;
}

// We don't care about the return value of Revive.
void Socket::Revive() {
    const uint32_t id_ver = VersionOfSocketId(_this_id);
    uint64_t vref = _versioned_ref.load(butil::memory_order_relaxed);
    while (1) {
        CHECK_EQ(id_ver + 1, VersionOfVRef(vref));
        
        int32_t nref = NRefOfVRef(vref);
        if (nref <= 1) {
            CHECK_EQ(1, nref);
            LOG(WARNING) << *this << " was abandoned during revival";
            return;
        }
        // +1 is the additional ref added in Create(). TODO(gejun): we should
        // remove this additional nref someday.
        if (_versioned_ref.compare_exchange_weak(
                vref, MakeVRef(id_ver, nref + 1/*note*/),
                butil::memory_order_release,
                butil::memory_order_relaxed)) {
            // Set this flag to true since we add additional ref again
            _recycle_flag.store(false, butil::memory_order_relaxed);
            if (_user) {
                _user->AfterRevived(this);
            } else {
                LOG(INFO) << "Revived " << *this << " (Connectable)";
            }
            return;
        }
    }
}

int Socket::ReleaseAdditionalReference() {
    bool expect = false;
    // Use `relaxed' fence here since `Dereference' has `released' fence
    if (_recycle_flag.compare_exchange_strong(
            expect, true,
            butil::memory_order_relaxed,
            butil::memory_order_relaxed)) {
        return Dereference();
    }
    return -1;
}

void Socket::AddRecentError() {
    SharedPart* sp = GetSharedPart();
    if (sp) {
        sp->recent_error_count.fetch_add(1, butil::memory_order_relaxed);
    }
}

int64_t Socket::recent_error_count() const {
    SharedPart* sp = GetSharedPart();
    if (sp) {
        return sp->recent_error_count.load(butil::memory_order_relaxed);
    }
    return 0;
}

int Socket::isolated_times() const {
    SharedPart* sp = GetSharedPart();
    if (sp) {
        return sp->circuit_breaker.isolated_times();
    }
    return 0;
}

int Socket::SetFailed(int error_code, const char* error_fmt, ...) {
    if (error_code == 0) {
        CHECK(false) << "error_code is 0";
        error_code = EFAILEDSOCKET;
    }
    const uint32_t id_ver = VersionOfSocketId(_this_id);
    uint64_t vref = _versioned_ref.load(butil::memory_order_relaxed);
    for (;;) {  // need iteration to retry compare_exchange_strong
        if (VersionOfVRef(vref) != id_ver) {
            return -1;
        }
        // Try to set version=id_ver+1 (to make later Address() return NULL),
        // retry on fail.
        if (_versioned_ref.compare_exchange_strong(
                vref, MakeVRef(id_ver + 1, NRefOfVRef(vref)),
                butil::memory_order_release,
                butil::memory_order_relaxed)) {
            // Update _error_text
            std::string error_text;
            if (error_fmt != NULL) {
                va_list ap;
                va_start(ap, error_fmt);
                butil::string_vprintf(&error_text, error_fmt, ap);
                va_end(ap);
            }
            pthread_mutex_lock(&_id_wait_list_mutex);
            _error_code = error_code;
            _error_text = error_text;
            pthread_mutex_unlock(&_id_wait_list_mutex);
            
            // Do health-checking even if we're not connected before, needed
            // by Channel to revive never-connected socket when server side
            // comes online.
            if (_health_check_interval_s > 0) {
                GetOrNewSharedPart()->circuit_breaker.MarkAsBroken();
                StartHealthCheck(id(),
                        GetOrNewSharedPart()->circuit_breaker.isolation_duration_ms());
            }
            // Wake up all threads waiting on EPOLLOUT when closing fd
            _epollout_butex->fetch_add(1, butil::memory_order_relaxed);
            bthread::butex_wake_all(_epollout_butex);

            // Wake up all unresponded RPC.
            CHECK_EQ(0, bthread_id_list_reset2_pthreadsafe(
                         &_id_wait_list, error_code, error_text,
                         &_id_wait_list_mutex));

            ResetAllStreams();
            // _app_connect shouldn't be set to NULL in SetFailed otherwise
            // HC is always not supported.
            // FIXME: Design a better interface for AppConnect
            // if (_app_connect) {
            //     AppConnect* const saved_app_connect = _app_connect;
            //     _app_connect = NULL;
            //     saved_app_connect->StopConnect(this);
            // }

            // Deref additionally which is added at creation so that this
            // Socket's reference will hit 0(recycle) when no one addresses it.
            ReleaseAdditionalReference();
            // NOTE: This Socket may be recycled at this point, don't
            // touch anything.
            return 0;
        }
    }
}

int Socket::SetFailed() {
    return SetFailed(EFAILEDSOCKET, NULL);
}

void Socket::FeedbackCircuitBreaker(int error_code, int64_t latency_us) {
    if (!GetOrNewSharedPart()->circuit_breaker.OnCallEnd(error_code, latency_us)) {
        if (SetFailed(main_socket_id()) == 0) {
            LOG(ERROR) << "Socket[" << *this << "] isolated by circuit breaker";
        }
    }
}

int Socket::ReleaseReferenceIfIdle(int idle_seconds) {
    const int64_t last_active_us = last_active_time_us();
    if (butil::cpuwide_time_us() - last_active_us <= idle_seconds * 1000000L) {
        return 0;
    }
    LOG_IF(WARNING, FLAGS_log_idle_connection_close)
        << "Close " << *this << " due to no data transmission for "
        << idle_seconds << " seconds";
    if (shall_fail_me_at_server_stop()) {
        // sockets for streaming purposes (say RTMP) are probably referenced
        // by many places, ReleaseAdditionalReference() cannot notify other
        // places to release refs, SetFailed() is a must.
        return SetFailed(EUNUSED, "No data transmission for %d seconds",
                         idle_seconds);
    }
    return ReleaseAdditionalReference();
}

int Socket::SetFailed(SocketId id) {
    SocketUniquePtr ptr;
    if (Address(id, &ptr) != 0) {
        return -1;
    }
    return ptr->SetFailed();
}

void Socket::NotifyOnFailed(bthread_id_t id) {
    pthread_mutex_lock(&_id_wait_list_mutex);
    if (!Failed()) {
        const int rc = bthread_id_list_add(&_id_wait_list, id);
        pthread_mutex_unlock(&_id_wait_list_mutex);
        if (rc != 0) {
            bthread_id_error(id, rc);
        }
    } else {
        const int rc = non_zero_error_code();
        const std::string desc = _error_text;
        pthread_mutex_unlock(&_id_wait_list_mutex);
        bthread_id_error2(id, rc, desc);
    }
}

// For unit-test.
int Socket::Status(SocketId id, int32_t* nref) {
    const butil::ResourceId<Socket> slot = SlotOfSocketId(id);
    Socket* const m = address_resource(slot);
    if (m != NULL) {
        const uint64_t vref = m->_versioned_ref.load(butil::memory_order_relaxed);
        if (VersionOfVRef(vref) == VersionOfSocketId(id)) {
            if (nref) {
                *nref = NRefOfVRef(vref);
            }
            return 0;
        } else if (VersionOfVRef(vref) == VersionOfSocketId(id) + 1) {
            if (nref) {
                *nref = NRefOfVRef(vref);
            }
            return 1;
        }
    }
    return -1;
}

void Socket::OnRecycle() {
    const bool create_by_connect = CreatedByConnect();
    if (_app_connect) {
        std::shared_ptr<AppConnect> tmp;
        _app_connect.swap(tmp);
        tmp->StopConnect(this);
    }
    if (_conn) {
        SocketConnection* const saved_conn = _conn;
        _conn = NULL;
        saved_conn->BeforeRecycle(this);
    }
    if (_user) {
        SocketUser* const saved_user = _user;
        _user = NULL;
        saved_user->BeforeRecycle(this);
    }
    SharedPart* sp = _shared_part.exchange(NULL, butil::memory_order_acquire);
    if (sp) {
        sp->RemoveRefManually();
    }
    const int prev_fd = _fd.exchange(-1, butil::memory_order_relaxed);
    if (ValidFileDescriptor(prev_fd)) {
        if (_on_edge_triggered_events != NULL) {
            GetGlobalEventDispatcher(prev_fd).RemoveConsumer(prev_fd);
        }
        close(prev_fd);
        if (create_by_connect) {
            g_vars->channel_conn << -1;
        }
    }
    reset_parsing_context(NULL);
    _read_buf.clear();

    _auth_flag_error.store(0, butil::memory_order_relaxed);
    bthread_id_error(_auth_id, 0);
    
    bthread_id_list_destroy(&_id_wait_list);

    if (_ssl_session) {
        SSL_free(_ssl_session);
        _ssl_session = NULL;
    }

    _ssl_ctx = NULL;
    
    delete _pipeline_q;
    _pipeline_q = NULL;

    delete _auth_context;
    _auth_context = NULL;

    delete _stream_set;
    _stream_set = NULL;

    const SocketId asid = _agent_socket_id.load(butil::memory_order_relaxed);
    if (asid != INVALID_SOCKET_ID) {
        SocketUniquePtr ptr;
        if (Socket::Address(asid, &ptr) == 0) {
            ptr->ReleaseAdditionalReference();
        }
    }
    
    g_vars->nsocket << -1;
}

void* Socket::ProcessEvent(void* arg) {
    // the enclosed Socket is valid and free to access inside this function.
    SocketUniquePtr s(static_cast<Socket*>(arg));
    s->_on_edge_triggered_events(s.get());
    return NULL;
}

// Check if there're new requests appended.
// If yes, point old_head to to reversed new requests and return false;
// If no:
//    old_head is fully written, set _write_head to NULL and return true;
//    old_head is not written yet, keep _write_head unchanged and return false;
// `old_head' is last new_head got from this function or (in another word)
// tail of current writing list.
// `singular_node' is true iff `old_head' is the only node in its list.
bool Socket::IsWriteComplete(Socket::WriteRequest* old_head,
                             bool singular_node,
                             Socket::WriteRequest** new_tail) {
    CHECK(NULL == old_head->next);
    // Try to set _write_head to NULL to mark that the write is done.
    WriteRequest* new_head = old_head;
    WriteRequest* desired = NULL;
    bool return_when_no_more = true;
    if (!old_head->data.empty() || !singular_node) {
        desired = old_head;
        // Write is obviously not complete if old_head is not fully written.
        return_when_no_more = false;
    }
    if (_write_head.compare_exchange_strong(
            new_head, desired, butil::memory_order_acquire)) {
        // No one added new requests.
        if (new_tail) {
            *new_tail = old_head;
        }
        return return_when_no_more;
    }
    CHECK_NE(new_head, old_head);
    // Above acquire fence pairs release fence of exchange in Write() to make
    // sure that we see all fields of requests set.

    // Someone added new requests.
    // Reverse the list until old_head.
    WriteRequest* tail = NULL;
    WriteRequest* p = new_head;
    do {
        while (p->next == WriteRequest::UNCONNECTED) {
            // TODO(gejun): elaborate this
            sched_yield();
        }
        WriteRequest* const saved_next = p->next;
        p->next = tail;
        tail = p;
        p = saved_next;
        CHECK(p != NULL);
    } while (p != old_head);

    // Link old list with new list.
    old_head->next = tail;
    // Call Setup() from oldest to newest, notice that the calling sequence
    // matters for protocols using pipelined_count, this is why we don't
    // calling Setup in above loop which is from newest to oldest.
    for (WriteRequest* q = tail; q; q = q->next) {
        q->Setup(this);
    }
    if (new_tail) {
        *new_tail = new_head;
    }
    return false;
}

int Socket::WaitEpollOut(int fd, bool pollin, const timespec* abstime) {
    if (!ValidFileDescriptor(fd)) {
        return 0;
    }
    // Do not need to check addressable since it will be called by
    // health checker which called `SetFailed' before
    const int expected_val = _epollout_butex->load(butil::memory_order_relaxed);
    EventDispatcher& edisp = GetGlobalEventDispatcher(fd);
    if (edisp.AddEpollOut(id(), fd, pollin) != 0) {
        return -1;
    }

    int rc = bthread::butex_wait(_epollout_butex, expected_val, abstime);
    const int saved_errno = errno;
    if (rc < 0 && errno == EWOULDBLOCK) {
        // Could be writable or spurious wakeup
        rc = 0;
    }
    // Ignore return value since `fd' might have been removed
    // by `RemoveConsumer' in `SetFailed'
    butil::ignore_result(edisp.RemoveEpollOut(id(), fd, pollin));
    errno = saved_errno;
    // Could be writable or spurious wakeup (by former epollout)
    return rc;
}

int Socket::Connect(const timespec* abstime,
                    int (*on_connect)(int, int, void*), void* data) {
    if (_ssl_ctx) {
        _ssl_state = SSL_CONNECTING;
    } else {
        _ssl_state = SSL_OFF;
    }
    butil::fd_guard sockfd(socket(AF_INET, SOCK_STREAM, 0));
    if (sockfd < 0) {
        PLOG(ERROR) << "Fail to create socket";
        return -1;
    }
    CHECK_EQ(0, butil::make_close_on_exec(sockfd));
    // We need to do async connect (to manage the timeout by ourselves).
    CHECK_EQ(0, butil::make_non_blocking(sockfd));
    
    struct sockaddr_in serv_addr;
    bzero((char*)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr = remote_side().ip;
    serv_addr.sin_port = htons(remote_side().port);
    const int rc = ::connect(
        sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    if (rc != 0 && errno != EINPROGRESS) {
        PLOG(WARNING) << "Fail to connect to " << remote_side();
        return -1;
    }
    if (on_connect) {
        EpollOutRequest* req = new(std::nothrow) EpollOutRequest;
        if (req == NULL) {
            LOG(FATAL) << "Fail to new EpollOutRequest";
            return -1;
        }
        req->fd = sockfd;
        req->timer_id = 0;
        req->on_epollout_event = on_connect;
        req->data = data;
        // A temporary Socket to hold `EpollOutRequest', which will
        // be added into epoll device soon
        SocketId connect_id;
        SocketOptions options;
        options.user = req;
        if (Socket::Create(options, &connect_id) != 0) {
            LOG(FATAL) << "Fail to create Socket";
            delete req;
            return -1;
        }
        // From now on, ownership of `req' has been transferred to
        // `connect_id'. We hold an additional reference here to
        // ensure `req' to be valid in this scope
        SocketUniquePtr s;
        CHECK_EQ(0, Socket::Address(connect_id, &s));

        // Add `sockfd' into epoll so that `HandleEpollOutRequest' will
        // be called with `req' when epoll event reaches
        if (GetGlobalEventDispatcher(sockfd).
            AddEpollOut(connect_id, sockfd, false) != 0) {
            const int saved_errno = errno;
            PLOG(WARNING) << "Fail to add fd=" << sockfd << " into epoll";
            s->SetFailed(saved_errno, "Fail to add fd=%d into epoll: %s",
                         (int)sockfd, berror(saved_errno));
            return -1;
        }
        
        // Register a timer for EpollOutRequest. Note that the timeout
        // callback has no race with the one above as both of them try
        // to `SetFailed' `connect_id' while only one of them can succeed
        // It also work when `HandleEpollOutRequest' has already been
        // called before adding the timer since it will be removed
        // inside destructor of `EpollOutRequest' after leaving this scope
        if (abstime) {
            int rc = bthread_timer_add(&req->timer_id, *abstime,
                                       HandleEpollOutTimeout,
                                       (void*)connect_id);
            if (rc) {
                LOG(ERROR) << "Fail to add timer: " << berror(rc);
                s->SetFailed(rc, "Fail to add timer: %s", berror(rc));
                return -1;
            }
        }
        
    } else {
        if (WaitEpollOut(sockfd, false, abstime) != 0) {
            PLOG(WARNING) << "Fail to wait EPOLLOUT of fd=" << sockfd;
            return -1;
        }
        if (CheckConnected(sockfd) != 0) {
            return -1;
        }
    }
    return sockfd.release();
}

int Socket::CheckConnected(int sockfd) {    
    if (sockfd == STREAM_FAKE_FD) {
        return 0;
    }
    int err = 0;
    socklen_t errlen = sizeof(err);
    if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0) {
        PLOG(ERROR) << "Fail to getsockopt of fd=" << sockfd; 
        return -1;
    }
    if (err != 0) {
        CHECK_NE(err, EINPROGRESS);
        errno = err;
        return -1;
    }

    struct sockaddr_in client;
    socklen_t size = sizeof(client);
    CHECK_EQ(0, getsockname(sockfd, (struct sockaddr*) &client, &size));
    LOG_IF(INFO, FLAGS_log_connected)
            << "Connected to " << remote_side()
            << " via fd=" << (int)sockfd << " SocketId=" << id()
            << " local_port=" << ntohs(client.sin_port);
    if (CreatedByConnect()) {
        g_vars->channel_conn << 1;
    }
    // Doing SSL handshake after TCP connected
    return SSLHandshake(sockfd, false);
}

int Socket::ConnectIfNot(const timespec* abstime, WriteRequest* req) {
    if (_fd.load(butil::memory_order_consume) >= 0) {
       return 0;
    }

    // Have to hold a reference for `req'
    SocketUniquePtr s;
    ReAddress(&s);
    req->socket = s.get();
    if (_conn) {
        if (_conn->Connect(this, abstime, KeepWriteIfConnected, req) < 0) {
            return -1;
        }
    } else {
        if (Connect(abstime, KeepWriteIfConnected, req) < 0) {
            return -1;
        }
    }
    s.release();
    return 1;    
}

int Socket::HandleEpollOut(SocketId id) {
    SocketUniquePtr s;
    // Since Sockets might have been `SetFailed' before they were
    // added into epoll, these sockets miss the signal inside
    // `SetFailed' and therefore must be signalled here using
    // `AddressFailedAsWell' to prevent waiting forever
    if (Socket::AddressFailedAsWell(id, &s) < 0) {
        // Ignore recycled sockets
        return -1;
    }

    EpollOutRequest* req = dynamic_cast<EpollOutRequest*>(s->user());
    if (req != NULL) {
        return s->HandleEpollOutRequest(0, req);
    }
    
    // Currently `WaitEpollOut' needs `_epollout_butex'
    // TODO(jiangrujie): Remove this in the future
    s->_epollout_butex->fetch_add(1, butil::memory_order_relaxed);
    bthread::butex_wake_except(s->_epollout_butex, 0);  
    return 0;
}

void Socket::HandleEpollOutTimeout(void* arg) {
    SocketId id = (SocketId)arg;
    SocketUniquePtr s;
    if (Socket::Address(id, &s) != 0) {
        return;
    }
    EpollOutRequest* req = dynamic_cast<EpollOutRequest*>(s->user());
    if (req == NULL) {
        LOG(FATAL) << "Impossible! SocketUser MUST be EpollOutRequest here";
        return;
    }
    s->HandleEpollOutRequest(ETIMEDOUT, req);
}

int Socket::HandleEpollOutRequest(int error_code, EpollOutRequest* req) {
    // Only one thread can `SetFailed' this `Socket' successfully
    // Also after this `req' will be destroyed when its reference
    // hits zero
    if (SetFailed() != 0) {
        return -1;
    }
    // We've got the right to call user callback
    // The timer will be removed inside destructor of EpollOutRequest
    GetGlobalEventDispatcher(req->fd).RemoveEpollOut(id(), req->fd, false);
    return req->on_epollout_event(req->fd, error_code, req->data);
}

void Socket::AfterAppConnected(int err, void* data) {
    WriteRequest* req = static_cast<WriteRequest*>(data);
    if (err == 0) {
        Socket* const s = req->socket;
        SharedPart* sp = s->GetSharedPart();
        if (sp) {
            sp->num_continuous_connect_timeouts.store(0, butil::memory_order_relaxed);
        }
        // requests are not setup yet. check the comment on Setup() in Write()
        req->Setup(s);
        bthread_t th;
        if (bthread_start_background(
                &th, &BTHREAD_ATTR_NORMAL, KeepWrite, req) != 0) {
            PLOG(WARNING) << "Fail to start KeepWrite";
            KeepWrite(req);
        }
    } else {
        SocketUniquePtr s(req->socket);
        if (err == ETIMEDOUT) {
            SharedPart* sp = s->GetOrNewSharedPart();
            if (sp->num_continuous_connect_timeouts.fetch_add(
                    1, butil::memory_order_relaxed) + 1 >=
                FLAGS_connect_timeout_as_unreachable) {
                // the race between store and fetch_add(in another thread) is
                // OK since a critial error is about to return.
                sp->num_continuous_connect_timeouts.store(
                    0, butil::memory_order_relaxed);
                err = ENETUNREACH;
            }
        }

        s->SetFailed(err, "Fail to connect %s: %s",
                     s->description().c_str(), berror(err));
        s->ReleaseAllFailedWriteRequests(req);
    }
}

static void* RunClosure(void* arg) {
    google::protobuf::Closure* done = (google::protobuf::Closure*)arg;
    done->Run();
    return NULL;
}

int Socket::KeepWriteIfConnected(int fd, int err, void* data) {
    WriteRequest* req = static_cast<WriteRequest*>(data);
    Socket* s = req->socket;
    if (err == 0 && s->ssl_state() == SSL_CONNECTING) {
        // Run ssl connect in a new bthread to avoid blocking
        // the current bthread (thus blocking the EventDispatcher)
        bthread_t th;
        google::protobuf::Closure* thrd_func = brpc::NewCallback(
            Socket::CheckConnectedAndKeepWrite, fd, err, data);
        if ((err = bthread_start_background(&th, &BTHREAD_ATTR_NORMAL,
                                            RunClosure, thrd_func)) == 0) {
            return 0;
        } else {
            PLOG(ERROR) << "Fail to start bthread";
            // Fall through with non zero `err'
        }
    }
    CheckConnectedAndKeepWrite(fd, err, data);
    return 0;
}

void Socket::CheckConnectedAndKeepWrite(int fd, int err, void* data) {
    butil::fd_guard sockfd(fd);
    WriteRequest* req = static_cast<WriteRequest*>(data);
    Socket* s = req->socket;
    CHECK_GE(sockfd, 0);
    if (err == 0 && s->CheckConnected(sockfd) == 0
        && s->ResetFileDescriptor(sockfd) == 0) {
        if (s->_app_connect) {
            s->_app_connect->StartConnect(req->socket, AfterAppConnected, req);
        } else {
            // Successfully created a connection
            AfterAppConnected(0, req);
        }
        // Release this socket for KeepWrite
        sockfd.release();
    } else {
        if (err == 0) {
            err = errno ? errno : -1;
        }
        AfterAppConnected(err, req);
    }
}
     
inline int SetError(bthread_id_t id_wait, int ec) {
    if (id_wait != INVALID_BTHREAD_ID) {
        bthread_id_error(id_wait, ec);
        return 0;
    } else {
        errno = ec;
        return -1;
    }
}

int Socket::ConductError(bthread_id_t id_wait) {
    pthread_mutex_lock(&_id_wait_list_mutex);
    if (Failed()) {
        const int error_code = non_zero_error_code();
        if (id_wait != INVALID_BTHREAD_ID) {
            const std::string error_text = _error_text;
            pthread_mutex_unlock(&_id_wait_list_mutex);
            bthread_id_error2(id_wait, error_code, error_text);
            return 0;
        } else {
            pthread_mutex_unlock(&_id_wait_list_mutex);
            errno = error_code;
            return -1;
        }
    } else {
        pthread_mutex_unlock(&_id_wait_list_mutex);
        return 1;
    }
}

X509* Socket::GetPeerCertificate() const {
    if (ssl_state() != SSL_CONNECTED) {
        return NULL;
    }
    return SSL_get_peer_certificate(_ssl_session);
}

int Socket::Write(butil::IOBuf* data, const WriteOptions* options_in) {
    WriteOptions opt;
    if (options_in) {
        opt = *options_in;
    }
    if (data->empty()) {
        return SetError(opt.id_wait, EINVAL);
    }
    if (opt.pipelined_count > MAX_PIPELINED_COUNT) {
        LOG(ERROR) << "pipelined_count=" << opt.pipelined_count
                   << " is too large";
        return SetError(opt.id_wait, EOVERFLOW);
    }
    if (Failed()) {
        const int rc = ConductError(opt.id_wait);
        if (rc <= 0) {
            return rc;
        }
    }

    if (!opt.ignore_eovercrowded && _overcrowded) {
        return SetError(opt.id_wait, EOVERCROWDED);
    }

    WriteRequest* req = butil::get_object<WriteRequest>();
    if (!req) {
        return SetError(opt.id_wait, ENOMEM);
    }

    req->data.swap(*data);
    // Set `req->next' to UNCONNECTED so that the KeepWrite thread will
    // wait until it points to a valid WriteRequest or NULL.
    req->next = WriteRequest::UNCONNECTED;
    req->id_wait = opt.id_wait;
    req->set_pipelined_count_and_user_message(
        opt.pipelined_count, DUMMY_USER_MESSAGE, opt.with_auth);
    return StartWrite(req, opt);
}

int Socket::Write(SocketMessagePtr<>& msg, const WriteOptions* options_in) {
    WriteOptions opt;
    if (options_in) {
        opt = *options_in;
    }
    if (opt.pipelined_count > MAX_PIPELINED_COUNT) {
        LOG(ERROR) << "pipelined_count=" << opt.pipelined_count
                   << " is too large";
        return SetError(opt.id_wait, EOVERFLOW);
    }

    if (Failed()) {
        const int rc = ConductError(opt.id_wait);
        if (rc <= 0) {
            return rc;
        }
    }
    
    if (!opt.ignore_eovercrowded && _overcrowded) {
        return SetError(opt.id_wait, EOVERCROWDED);
    }
    
    WriteRequest* req = butil::get_object<WriteRequest>();
    if (!req) {
        return SetError(opt.id_wait, ENOMEM);
    }

    // Set `req->next' to UNCONNECTED so that the KeepWrite thread will
    // wait until it points to a valid WriteRequest or NULL.
    req->next = WriteRequest::UNCONNECTED;
    req->id_wait = opt.id_wait;
    req->set_pipelined_count_and_user_message(opt.pipelined_count, msg.release(), opt.with_auth);
    return StartWrite(req, opt);
}

int Socket::StartWrite(WriteRequest* req, const WriteOptions& opt) {
    // Release fence makes sure the thread getting request sees *req
    WriteRequest* const prev_head =
        _write_head.exchange(req, butil::memory_order_release);
    if (prev_head != NULL) {
        // Someone is writing to the fd. The KeepWrite thread may spin
        // until req->next to be non-UNCONNECTED. This process is not
        // lock-free, but the duration is so short(1~2 instructions,
        // depending on compiler) that the spin rarely occurs in practice
        // (I've not seen any spin in highly contended tests).
        req->next = prev_head;
        return 0;
    }

    int saved_errno = 0;
    bthread_t th;
    SocketUniquePtr ptr_for_keep_write;
    ssize_t nw = 0;

    // We've got the right to write.
    req->next = NULL;
    
    // Connect to remote_side() if not.
    int ret = ConnectIfNot(opt.abstime, req);
    if (ret < 0) {
        saved_errno = errno;
        SetFailed(errno, "Fail to connect %s directly: %m", description().c_str());
        goto FAIL_TO_WRITE;
    } else if (ret == 1) {
        // We are doing connection. Callback `KeepWriteIfConnected'
        // will be called with `req' at any moment after
        return 0;
    }

    // NOTE: Setup() MUST be called after Connect which may call app_connect,
    // which is assumed to run before any SocketMessage.AppendAndDestroySelf()
    // in some protocols(namely RTMP).
    req->Setup(this);
    
    if (ssl_state() != SSL_OFF) {
        // Writing into SSL may block the current bthread, always write
        // in the background.
        goto KEEPWRITE_IN_BACKGROUND;
    }
    
    // Write once in the calling thread. If the write is not complete,
    // continue it in KeepWrite thread.
    if (_conn) {
        butil::IOBuf* data_arr[1] = { &req->data };
        nw = _conn->CutMessageIntoFileDescriptor(fd(), data_arr, 1);
    } else {
        nw = req->data.cut_into_file_descriptor(fd());
    }
    if (nw < 0) {
        // RTMP may return EOVERCROWDED
        if (errno != EAGAIN && errno != EOVERCROWDED) {
            saved_errno = errno;
            // EPIPE is common in pooled connections + backup requests.
            PLOG_IF(WARNING, errno != EPIPE) << "Fail to write into " << *this;
            SetFailed(saved_errno, "Fail to write into %s: %s", 
                      description().c_str(), berror(saved_errno));
            goto FAIL_TO_WRITE;
        }
    } else {
        AddOutputBytes(nw);
    }
    if (IsWriteComplete(req, true, NULL)) {
        ReturnSuccessfulWriteRequest(req);
        return 0;
    }

KEEPWRITE_IN_BACKGROUND:
    ReAddress(&ptr_for_keep_write);
    req->socket = ptr_for_keep_write.release();
    if (bthread_start_background(&th, &BTHREAD_ATTR_NORMAL,
                                 KeepWrite, req) != 0) {
        LOG(FATAL) << "Fail to start KeepWrite";
        KeepWrite(req);
    }
    return 0;

FAIL_TO_WRITE:
    // `SetFailed' before `ReturnFailedWriteRequest' (which will calls
    // `on_reset' callback inside the id object) so that we immediately
    // know this socket has failed inside the `on_reset' callback
    ReleaseAllFailedWriteRequests(req);
    errno = saved_errno;
    return -1;
}

static const size_t DATA_LIST_MAX = 256;

void* Socket::KeepWrite(void* void_arg) {
    g_vars->nkeepwrite << 1;
    WriteRequest* req = static_cast<WriteRequest*>(void_arg);
    SocketUniquePtr s(req->socket);

    // When error occurs, spin until there's no more requests instead of
    // returning directly otherwise _write_head is permantly non-NULL which
    // makes later Write() abnormal.
    WriteRequest* cur_tail = NULL;
    do {
        // req was written, skip it.
        if (req->next != NULL && req->data.empty()) {
            WriteRequest* const saved_req = req;
            req = req->next;
            s->ReturnSuccessfulWriteRequest(saved_req);
        }
        const ssize_t nw = s->DoWrite(req);
        if (nw < 0) {
            if (errno != EAGAIN && errno != EOVERCROWDED) {
                const int saved_errno = errno;
                PLOG(WARNING) << "Fail to keep-write into " << *s;
                s->SetFailed(saved_errno, "Fail to keep-write into %s: %s",
                             s->description().c_str(), berror(saved_errno));
                break;
            }
        } else {
            s->AddOutputBytes(nw);
        }
        // Release WriteRequest until non-empty data or last request.
        while (req->next != NULL && req->data.empty()) {
            WriteRequest* const saved_req = req;
            req = req->next;
            s->ReturnSuccessfulWriteRequest(saved_req);
        }
        // TODO(gejun): wait for epollout when we actually have written
        // all the data. This weird heuristic reduces 30us delay...
        // Update(12/22/2015): seem not working. better switch to correct code.
        // Update(1/8/2016, r31823): Still working.
        // Update(8/15/2017): Not working, performance downgraded.
        //if (nw <= 0 || req->data.empty()/*note*/) {
        if (nw <= 0) {
            g_vars->nwaitepollout << 1;
            bool pollin = (s->_on_edge_triggered_events != NULL);
            // NOTE: Waiting epollout within timeout is a must to force
            // KeepWrite to check and setup pending WriteRequests periodically,
            // which may turn on _overcrowded to stop pending requests from
            // growing infinitely.
            const timespec duetime =
                butil::milliseconds_from_now(WAIT_EPOLLOUT_TIMEOUT_MS);
            const int rc = s->WaitEpollOut(s->fd(), pollin, &duetime);
            if (rc < 0 && errno != ETIMEDOUT) {
                const int saved_errno = errno;
                PLOG(WARNING) << "Fail to wait epollout of " << *s;
                s->SetFailed(saved_errno, "Fail to wait epollout of %s: %s",
                             s->description().c_str(), berror(saved_errno));
                break;
            }
        }
        if (NULL == cur_tail) {
            for (cur_tail = req; cur_tail->next != NULL;
                 cur_tail = cur_tail->next);
        }
        // Return when there's no more WriteRequests and req is completely
        // written.
        if (s->IsWriteComplete(cur_tail, (req == cur_tail), &cur_tail)) {
            CHECK_EQ(cur_tail, req);
            s->ReturnSuccessfulWriteRequest(req);
            return NULL;
        }
    } while (1);

    // Error occurred, release all requests until no new requests.
    s->ReleaseAllFailedWriteRequests(req);
    return NULL;
}

ssize_t Socket::DoWrite(WriteRequest* req) {
    // Group butil::IOBuf in the list into a batch array.
    butil::IOBuf* data_list[DATA_LIST_MAX];
    size_t ndata = 0;
    for (WriteRequest* p = req; p != NULL && ndata < DATA_LIST_MAX;
         p = p->next) {
        data_list[ndata++] = &p->data;
    }

    if (ssl_state() == SSL_OFF) {
        // Write IOBuf in the batch array into the fd.
        if (_conn) {
            return _conn->CutMessageIntoFileDescriptor(fd(), data_list, ndata);
        } else {
            ssize_t nw = butil::IOBuf::cut_multiple_into_file_descriptor(
                fd(), data_list, ndata);
            return nw;
        }
    }

    CHECK_EQ(SSL_CONNECTED, ssl_state());
    if (_conn) {
        // TODO: Separate SSL stuff from SocketConnection
        return _conn->CutMessageIntoSSLChannel(_ssl_session, data_list, ndata);
    }
    int ssl_error = 0;
    ssize_t nw = butil::IOBuf::cut_multiple_into_SSL_channel(
        _ssl_session, data_list, ndata, &ssl_error);
    switch (ssl_error) {
    case SSL_ERROR_NONE:
        break;

    case SSL_ERROR_WANT_READ:
        // Disable renegotiation
        errno = EPROTO;
        return -1;

    case SSL_ERROR_WANT_WRITE:
        errno = EAGAIN;
        break;

    default: {
        const unsigned long e = ERR_get_error();
        if (e != 0) {
            LOG(WARNING) << "Fail to write into ssl_fd=" << fd() <<  ": "
                         << SSLError(ERR_get_error());
            errno = ESSL;
         } else {
            // System error with corresponding errno set
            PLOG(WARNING) << "Fail to write into ssl_fd=" << fd();
        }
        break;
    }
    }
    return nw;
}

int Socket::SSLHandshake(int fd, bool server_mode) {
    if (_ssl_ctx == NULL) {
        if (server_mode) {
            LOG(ERROR) << "Lack SSL configuration to handle SSL request";
            return -1;
        }
        return 0;
    }

    // TODO: Reuse ssl session id for client
    if (_ssl_session) {
        // Free the last session, which may be deprecated when socket failed
        SSL_free(_ssl_session);
    }
    _ssl_session = CreateSSLSession(_ssl_ctx->raw_ctx, id(), fd, server_mode);
    if (_ssl_session == NULL) {
        LOG(ERROR) << "Fail to CreateSSLSession";
        return -1;
    }
#if defined(SSL_CTRL_SET_TLSEXT_HOSTNAME) || defined(USE_MESALINK)
    if (!_ssl_ctx->sni_name.empty()) {
        SSL_set_tlsext_host_name(_ssl_session, _ssl_ctx->sni_name.c_str());
    }
#endif

    _ssl_state = SSL_CONNECTING;

    // Loop until SSL handshake has completed. For SSL_ERROR_WANT_READ/WRITE,
    // we use bthread_fd_wait as polling mechanism instead of EventDispatcher
    // as it may confuse the origin event processing code.
    while (true) {
        int rc = SSL_do_handshake(_ssl_session);
        if (rc == 1) {
            _ssl_state = SSL_CONNECTED;
            AddBIOBuffer(_ssl_session, fd, FLAGS_ssl_bio_buffer_size);
            return 0;
        }

        int ssl_error = SSL_get_error(_ssl_session, rc);
        switch (ssl_error) {
        case SSL_ERROR_WANT_READ:
#if defined(OS_LINUX)
            if (bthread_fd_wait(fd, EPOLLIN) != 0) {
#elif defined(OS_MACOSX)
            if (bthread_fd_wait(fd, EVFILT_READ) != 0) {
#endif
                return -1;
            }
            break;

        case SSL_ERROR_WANT_WRITE:
#if defined(OS_LINUX)
            if (bthread_fd_wait(fd, EPOLLOUT) != 0) {
#elif defined(OS_MACOSX)
            if (bthread_fd_wait(fd, EVFILT_WRITE) != 0) {
#endif
                return -1;
            }
            break;
 
        default: {
            const unsigned long e = ERR_get_error();
            if (ssl_error == SSL_ERROR_ZERO_RETURN || e == 0) {
                errno = ECONNRESET;
                LOG(ERROR) << "SSL connection was shutdown by peer: " << _remote_side;
            } else if (ssl_error == SSL_ERROR_SYSCALL) {
                PLOG(ERROR) << "Fail to SSL_do_handshake";
            } else {
                errno = ESSL;
                LOG(ERROR) << "Fail to SSL_do_handshake: " << SSLError(e);
            }
            return -1;
        }
        }
    }
}

ssize_t Socket::DoRead(size_t size_hint) {
    if (ssl_state() == SSL_UNKNOWN) {
        int error_code = 0;
        _ssl_state = DetectSSLState(fd(), &error_code);
        switch (ssl_state()) {
        case SSL_UNKNOWN:
            if (error_code == 0) {  // EOF
                return 0;
            } else {
                errno = error_code;
                return -1;
            }

        case SSL_CONNECTING:
            if (SSLHandshake(fd(), true) != 0) {
                errno = EINVAL;
                return -1;
            }
            break;

        case SSL_CONNECTED:
            CHECK(false) << "Impossible to reach here";
            break;
            
        case SSL_OFF:
            break;
        }
    }
    // _ssl_state has been set
    if (ssl_state() == SSL_OFF) {
        return _read_buf.append_from_file_descriptor(fd(), size_hint);
    }

    CHECK_EQ(SSL_CONNECTED, ssl_state());
    int ssl_error = 0;
    ssize_t nr = _read_buf.append_from_SSL_channel(_ssl_session, &ssl_error, size_hint);
    switch (ssl_error) {
    case SSL_ERROR_NONE:  // `nr' > 0
        break;
            
    case SSL_ERROR_WANT_READ:
        // Regard this error as EAGAIN
        errno = EAGAIN;
        break;
            
    case SSL_ERROR_WANT_WRITE:
        // Disable renegotiation
        errno = EPROTO;
        return -1;

    default: {
        const unsigned long e = ERR_get_error();
        if (nr == 0) {
            // Socket EOF or SSL session EOF
        } else if (e != 0) {
            LOG(WARNING) << "Fail to read from ssl_fd=" << fd()
                         << ": " << SSLError(e);
            errno = ESSL;
        } else {
            // System error with corresponding errno set
            PLOG(WARNING) << "Fail to read from ssl_fd=" << fd();
        }
        break;
    }
    }
    return nr;
}

int Socket::FightAuthentication(int* auth_error) {
    // Use relaxed fence since `bthread_id_trylock' ensures thread safety
    // Here `flag_error' just acts like a cache information
    uint64_t flag_error = _auth_flag_error.load(butil::memory_order_relaxed);
    if (flag_error & AUTH_FLAG) {
        // Already authenticated
        *auth_error = (int32_t)(flag_error & 0xFFFFFFFFul);
        return EINVAL;
    }
    if (0 == bthread_id_trylock(_auth_id, NULL)) {
        // Winner
        return 0;
    } else {
        // Use relaxed fence since `bthread_id_join' has acquire fence to ensure
        // `_auth_flag_error' to be the latest value
        bthread_id_join(_auth_id);
        flag_error = _auth_flag_error.load(butil::memory_order_relaxed);
        *auth_error = (int32_t)(flag_error & 0xFFFFFFFFul);
        return EINVAL;
    }
}

void Socket::SetAuthentication(int error_code) {
    uint64_t expected = 0;       
    // `bthread_id_destroy' has release fence to prevent this CAS being
    // reordered after it.
    if (_auth_flag_error.compare_exchange_strong(
                expected, (AUTH_FLAG | error_code),
                butil::memory_order_relaxed)) {
        // As expected
        if (error_code != 0) {
            SetFailed(error_code, "Fail to authenticate %s", description().c_str());
        }
        CHECK_EQ(0, bthread_id_unlock_and_destroy(_auth_id));
    }
}

AuthContext* Socket::mutable_auth_context() {
    if (_auth_context != NULL) {
        LOG(FATAL) << "Impossible! This function is supposed to be called "
              "only once when verification succeeds in server side";
        return NULL;
    }
    _auth_context = new(std::nothrow) AuthContext();
    CHECK(_auth_context);
    return _auth_context;
}

int Socket::StartInputEvent(SocketId id, uint32_t events,
                            const bthread_attr_t& thread_attr) {
    SocketUniquePtr s;
    if (Address(id, &s) < 0) {
        return -1;
    }
    if (NULL == s->_on_edge_triggered_events) {
        // Callback can be NULL when receiving error epoll events
        // (Added into epoll by `WaitConnected')
        return 0;
    }
    if (s->fd() < 0) {
#if defined(OS_LINUX)
        CHECK(!(events & EPOLLIN)) << "epoll_events=" << events;
#elif defined(OS_MACOSX)
        CHECK((short)events != EVFILT_READ) << "kqueue filter=" << events;
#endif
        return -1;
    }

    // if (events & has_epollrdhup) {
    //     s->_eof = 1;
    // }
    // Passing e[i].events causes complex visibility issues and
    // requires stronger memory fences, since reading the fd returns
    // error as well, we don't pass the events.
    if (s->_nevent.fetch_add(1, butil::memory_order_acq_rel) == 0) {
        // According to the stats, above fetch_add is very effective. In a
        // server processing 1 million requests per second, this counter
        // is just 1500~1700/s
        g_vars->neventthread << 1;

        bthread_t tid;
        // transfer ownership as well, don't use s anymore!
        Socket* const p = s.release();

        bthread_attr_t attr = thread_attr;
        attr.keytable_pool = p->_keytable_pool;
        if (bthread_start_urgent(&tid, &attr, ProcessEvent, p) != 0) {
            LOG(FATAL) << "Fail to start ProcessEvent";
            ProcessEvent(p);
        }
    }
    return 0;
}

void DereferenceSocket(Socket* s) {
    if (s) {
        s->Dereference();
    }
}

void Socket::UpdateStatsEverySecond(int64_t now_ms) {
    SharedPart* sp = GetSharedPart();
    if (sp) {
        sp->UpdateStatsEverySecond(now_ms);
    }
}

template <typename T>
struct ObjectPtr {
    ObjectPtr(const T* obj) : _obj(obj) {}
    const T* _obj;
};

template <typename T>
ObjectPtr<T> ShowObject(const T* obj) { return ObjectPtr<T>(obj); }

template <typename T>
std::ostream& operator<<(std::ostream& os, const ObjectPtr<T>& obj) {
    if (obj._obj != NULL) {
        os << '(' << butil::class_name_str(*obj._obj) << "*)";
    }
    return os << obj._obj;
}

void Socket::DebugSocket(std::ostream& os, SocketId id) {
    SocketUniquePtr ptr;
    int ret = Socket::AddressFailedAsWell(id, &ptr);
    if (ret < 0) {
        os << "SocketId=" << id << " is invalid or recycled";
        return;
    } else if (ret > 0) {
        // NOTE: Printing a broken socket w/o HC is informational for
        // debugging referential issues.
        // if (ptr->_health_check_interval_s <= 0) {
        //     // Sockets without HC will soon be destroyed
        //     os << "Invalid SocketId=" << id;
        //     return;
        // }
        os << "# This is a broken Socket\n";
    }
    const uint64_t vref = ptr->_versioned_ref.load(butil::memory_order_relaxed);
    size_t npipelined = 0;
    size_t idsizes[4];
    size_t nidsize = 0;
    {
        BAIDU_SCOPED_LOCK(ptr->_pipeline_mutex);
        if (ptr->_pipeline_q) {
            npipelined = ptr->_pipeline_q->size();
        }
    }
    {
        BAIDU_SCOPED_LOCK(ptr->_id_wait_list_mutex);
        if (bthread::get_sizes) {
            nidsize = bthread::get_sizes(
                &ptr->_id_wait_list, idsizes, arraysize(idsizes));
        }
    }
    const int preferred_index = ptr->preferred_index();
    SharedPart* sp = ptr->GetSharedPart();
    os << "version=" << VersionOfVRef(vref);
    if (sp) {
        os << "\nshared_part={\n  ref_count=" << sp->ref_count()
           << "\n  socket_pool=";
        SocketPool* pool = sp->socket_pool.load(butil::memory_order_consume);
        if (pool) {
            os << '[';
            std::vector<SocketId> pooled_sockets;
            pool->ListSockets(&pooled_sockets, 0);
            for (size_t i = 0; i < pooled_sockets.size(); ++i) {
                if (i) {
                    os << ' ';
                }
                os << pooled_sockets[i];
            }
            os << "]\n  numfree="
               << pool->_numfree.load(butil::memory_order_relaxed)
               << "\n  numinflight="
               << pool->_numinflight.load(butil::memory_order_relaxed);
        } else {
            os << "null";
        }
        os << "\n  creator_socket=" << sp->creator_socket_id
           << "\n  in_size=" << sp->in_size.load(butil::memory_order_relaxed)
           << "\n  in_num_messages=" << sp->in_num_messages.load(butil::memory_order_relaxed)
           << "\n  out_size=" << sp->out_size.load(butil::memory_order_relaxed)
           << "\n  out_num_messages=" << sp->out_num_messages.load(butil::memory_order_relaxed)
           << "\n}";
    }
    const int fd = ptr->_fd.load(butil::memory_order_relaxed);
    os << "\nnref=" << NRefOfVRef(vref) - 1
        //                                ^
        // minus the ref of current callsite(calling PrintSocket)
       << "\nnevent=" << ptr->_nevent.load(butil::memory_order_relaxed)
       << "\nfd=" << fd
       << "\ntos=" << ptr->_tos
       << "\nreset_fd_to_now=" << butil::gettimeofday_us() - ptr->_reset_fd_real_us << "us"
       << "\nremote_side=" << ptr->_remote_side
       << "\nlocal_side=" << ptr->_local_side
       << "\non_et_events=" << (void*)ptr->_on_edge_triggered_events
       << "\nuser=" << ShowObject(ptr->_user)
       << "\nthis_id=" << ptr->_this_id
       << "\npreferred_index=" << preferred_index;
    InputMessenger* messenger = dynamic_cast<InputMessenger*>(ptr->user());
    if (messenger != NULL) {
        os << " (" << messenger->NameOfProtocol(preferred_index) << ')';
    }
    const int64_t cpuwide_now = butil::cpuwide_time_us();
    os << "\nhc_count=" << ptr->_hc_count
       << "\navg_input_msg_size=" << ptr->_avg_msg_size
        // NOTE: We're assuming that butil::IOBuf.size() is thread-safe, it is now
        // however it's not guaranteed.
       << "\nread_buf=" << ptr->_read_buf.size()
       << "\nlast_read_to_now=" << cpuwide_now - ptr->_last_readtime_us << "us"
       << "\nlast_write_to_now=" << cpuwide_now - ptr->_last_writetime_us << "us"
       << "\novercrowded=" << ptr->_overcrowded;
    os << "\nid_wait_list={";
    for (size_t i = 0; i < nidsize; ++i) {
        if (i) {
            os << ' ';
        }
        os << idsizes[i];
    }
    os << '}';
    Destroyable* const parsing_context = ptr->parsing_context();
    Describable* parsing_context_desc = dynamic_cast<Describable*>(parsing_context);
    if (parsing_context_desc) {
        os << "\nparsing_context=" << butil::class_name_str(*parsing_context) << '{';
        DescribeOptions opt;
        opt.verbose = true;
        IndentingOStream os2(os, 2);
        parsing_context_desc->Describe(os2, opt);
        os << '}';
    } else {
        os << "\nparsing_context=" << ShowObject(parsing_context);
    }
    const SSLState ssl_state = ptr->ssl_state();
    os << "\npipeline_q=" << npipelined
       << "\nhc_interval_s=" << ptr->_health_check_interval_s
       << "\nninprocess=" << ptr->_ninprocess.load(butil::memory_order_relaxed)
       << "\nauth_flag_error=" << ptr->_auth_flag_error.load(butil::memory_order_relaxed)
       << "\nauth_id=" << ptr->_auth_id.value
       << "\nauth_context=" << ptr->_auth_context
       << "\nlogoff_flag=" << ptr->_logoff_flag.load(butil::memory_order_relaxed)
       << "\nrecycle_flag=" << ptr->_recycle_flag.load(butil::memory_order_relaxed)
       << "\nninflight_app_health_check="
       << ptr->_ninflight_app_health_check.load(butil::memory_order_relaxed)
       << "\nagent_socket_id=";
    const SocketId asid = ptr->_agent_socket_id.load(butil::memory_order_relaxed);
    if (asid != INVALID_SOCKET_ID) {
        os << asid;
    } else {
        os << "(none)";
    }
    os << "\ncid=" << ptr->_correlation_id
       << "\nwrite_head=" << ptr->_write_head.load(butil::memory_order_relaxed)
       << "\nssl_state=" << SSLStateToString(ssl_state);
    const SocketSSLContext* ssl_ctx = ptr->_ssl_ctx.get();
    if (ssl_ctx) {
        os << "\ninitial_ssl_ctx=" << ssl_ctx->raw_ctx;
        if (!ssl_ctx->sni_name.empty()) {
            os << "\nsni_name=" << ssl_ctx->sni_name;
        }
    }
    if (ssl_state == SSL_CONNECTED) {
        os << "\nssl_session={\n  ";
        Print(os, ptr->_ssl_session, "\n  ");
        os << "\n}";
    }
#if defined(OS_MACOSX)
    struct tcp_connection_info ti;
    socklen_t len = sizeof(ti);
    if (fd >= 0 && getsockopt(fd, IPPROTO_TCP, TCP_CONNECTION_INFO, &ti, &len) == 0) {
        os << "\ntcpi={\n  state=" << (uint32_t)ti.tcpi_state
           << "\n  snd_wscale=" << (uint32_t)ti.tcpi_snd_wscale
           << "\n  rcv_wscale=" << (uint32_t)ti.tcpi_rcv_wscale
           << "\n  options=" << (uint32_t)ti.tcpi_options
           << "\n  flags=" << (uint32_t)ti.tcpi_flags
           << "\n  rto=" << ti.tcpi_rto
           << "\n  maxseg=" << ti.tcpi_maxseg
           << "\n  snd_ssthresh=" << ti.tcpi_snd_ssthresh
           << "\n  snd_cwnd=" << ti.tcpi_snd_cwnd
           << "\n  snd_wnd=" << ti.tcpi_snd_wnd
           << "\n  snd_sbbytes=" << ti.tcpi_snd_sbbytes
           << "\n  rcv_wnd=" << ti.tcpi_rcv_wnd
           << "\n  srtt=" << ti.tcpi_srtt
           << "\n  rttvar=" << ti.tcpi_rttvar
           << "\n}";
    }
#elif defined(OS_LINUX)
    struct tcp_info ti;
    socklen_t len = sizeof(ti);
    if (fd >= 0 && getsockopt(fd, SOL_TCP, TCP_INFO, &ti, &len) == 0) {
        os << "\ntcpi={\n  state=" << (uint32_t)ti.tcpi_state
           << "\n  ca_state=" << (uint32_t)ti.tcpi_ca_state
           << "\n  retransmits=" << (uint32_t)ti.tcpi_retransmits
           << "\n  probes=" << (uint32_t)ti.tcpi_probes
           << "\n  backoff=" << (uint32_t)ti.tcpi_backoff
           << "\n  options=" << (uint32_t)ti.tcpi_options
           << "\n  snd_wscale=" << (uint32_t)ti.tcpi_snd_wscale
           << "\n  rcv_wscale=" << (uint32_t)ti.tcpi_rcv_wscale
           << "\n  rto=" << ti.tcpi_rto
           << "\n  ato=" << ti.tcpi_ato
           << "\n  snd_mss=" << ti.tcpi_snd_mss
           << "\n  rcv_mss=" << ti.tcpi_rcv_mss
           << "\n  unacked=" << ti.tcpi_unacked
           << "\n  sacked=" << ti.tcpi_sacked
           << "\n  lost=" << ti.tcpi_lost
           << "\n  retrans=" << ti.tcpi_retrans
           << "\n  fackets=" << ti.tcpi_fackets
           << "\n  last_data_sent=" << ti.tcpi_last_data_sent
           << "\n  last_ack_sent=" << ti.tcpi_last_ack_sent
           << "\n  last_data_recv=" << ti.tcpi_last_data_recv
           << "\n  last_ack_recv=" << ti.tcpi_last_ack_recv
           << "\n  pmtu=" << ti.tcpi_pmtu
           << "\n  rcv_ssthresh=" << ti.tcpi_rcv_ssthresh
           << "\n  rtt=" << ti.tcpi_rtt  // smoothed
           << "\n  rttvar=" << ti.tcpi_rttvar
           << "\n  snd_ssthresh=" << ti.tcpi_snd_ssthresh
           << "\n  snd_cwnd=" << ti.tcpi_snd_cwnd
           << "\n  advmss=" << ti.tcpi_advmss
           << "\n  reordering=" << ti.tcpi_reordering
           << "\n}";
    }
#endif
}

int Socket::CheckHealth() {
    if (_hc_count == 0) {
        LOG(INFO) << "Checking " << *this;
    }
    const timespec duetime =
        butil::milliseconds_from_now(FLAGS_health_check_timeout_ms);
    const int connected_fd = Connect(&duetime, NULL, NULL);
    if (connected_fd >= 0) {
        ::close(connected_fd);
        return 0;
    }
    return errno;
}

int Socket::AddStream(StreamId stream_id) {
    _stream_mutex.lock();
    if (Failed()) {
        _stream_mutex.unlock();
        return -1;
    }
    if (_stream_set == NULL) {
        _stream_set = new std::set<StreamId>();
    }
    _stream_set->insert(stream_id);
    _stream_mutex.unlock();
    return 0;
}

int Socket::RemoveStream(StreamId stream_id) {
    _stream_mutex.lock();
    if (_stream_set == NULL) {
        _stream_mutex.unlock();
        CHECK(false) << "AddStream was not called";
        return -1;
    }
    _stream_set->erase(stream_id);
    _stream_mutex.unlock();
    return 0;
}

void Socket::ResetAllStreams() {
    DCHECK(Failed());
    std::set<StreamId> saved_stream_set;
    _stream_mutex.lock();
    if (_stream_set != NULL) {
        // Not delete _stream_set because there are likely more streams added
        // after reviving if the Socket is still in use, or it is to be deleted in 
        // OnRecycle()
        saved_stream_set.swap(*_stream_set);
    }
    _stream_mutex.unlock();
    for (std::set<StreamId>::const_iterator 
            it = saved_stream_set.begin(); it != saved_stream_set.end(); ++it) {
        Stream::SetFailed(*it);
    }
}

int SocketUser::CheckHealth(Socket* ptr) {
    return ptr->CheckHealth();
}

void SocketUser::AfterRevived(Socket* ptr) {
    LOG(INFO) << "Revived " << *ptr << " (Connectable)";
}

////////// SocketPool //////////////

inline SocketPool::SocketPool(const SocketOptions& opt)
    : _options(opt)
    , _remote_side(opt.remote_side)
    , _numfree(0)
    , _numinflight(0) {
}

inline SocketPool::~SocketPool() {
    for (std::vector<SocketId>::iterator it = _pool.begin();
         it != _pool.end(); ++it) {
        SocketUniquePtr ptr;
        if (Socket::Address(*it, &ptr) == 0) {
            ptr->ReleaseAdditionalReference();
        }
    }
}

inline int SocketPool::GetSocket(SocketUniquePtr* ptr) {
    const int connection_pool_size = FLAGS_max_connection_pool_size;

    // In prev rev, SocketPool could be sharded into multiple SubSocketPools to
    // reduce thread contentions. The sharding key is mixed from pthread-id so
    // that data locality are better kept.
    // However sharding also makes the socket more frequently to be created
    // and closed, especially in real-world applications that one client
    // connects to many servers where one socket is lowly contended, different
    // threads accessing the socket may create pooled sockets in different sub
    // pools without reusing sockets left in other sub pools, which will
    // probably be closed by the CloseIdleConnections thread in socket_map.cpp,
    // resulting in frequent-create-and-close of connections.
    // Thus the sharding is merely a mechanism only meaningful in benchmarking
    // scenarios where one server is connected by one client with many threads.
    // Starting from r32203 the sharding capability is removed.

    SocketId sid = 0;
    if (connection_pool_size > 0) {
        for (;;) {
            {
                BAIDU_SCOPED_LOCK(_mutex);
                if (_pool.empty()) {
                    break;
                }
                sid = _pool.back();
                _pool.pop_back();
            }
            _numfree.fetch_sub(1, butil::memory_order_relaxed);
            // Not address inside the lock since at most time the pooled socket
            // is likely to be valid.
            if (Socket::Address(sid, ptr) == 0) {
                _numinflight.fetch_add(1, butil::memory_order_relaxed);
                return 0;
            }
        }
    }
    // Not found in pool
    SocketOptions opt = _options;
    opt.health_check_interval_s = -1;
    if (get_client_side_messenger()->Create(opt, &sid) == 0 &&
        Socket::Address(sid, ptr) == 0) {
        _numinflight.fetch_add(1, butil::memory_order_relaxed);
        return 0;
    }
    return -1;
}

inline void SocketPool::ReturnSocket(Socket* sock) {
    // NOTE: save the gflag which may be reloaded at any time.
    const int connection_pool_size = FLAGS_max_connection_pool_size;

    // Check if the pool is full.
    if (_numfree.fetch_add(1, butil::memory_order_relaxed) <
        connection_pool_size) {
        const SocketId sid = sock->id();
        BAIDU_SCOPED_LOCK(_mutex);
        _pool.push_back(sid);
    } else {
        // Cancel the addition and close the pooled socket.
        _numfree.fetch_sub(1, butil::memory_order_relaxed);
        sock->SetFailed(EUNUSED, "Close unused pooled socket");
    }
    _numinflight.fetch_sub(1, butil::memory_order_relaxed);
}

inline void SocketPool::ListSockets(std::vector<SocketId>* out, size_t max_count) {
    out->clear();
    // NOTE: size() of vector is thread-unsafe and may return a very 
    // large value during resizing.
    _mutex.lock();
    size_t expected_size = _pool.size();
    if (max_count > 0 && max_count < _pool.size()) {
        expected_size = max_count;
    }
    if (out->capacity() < expected_size) {
        _mutex.unlock();
        out->reserve(expected_size + 4); // pool may add sockets.
        _mutex.lock();
    }
    if (max_count == 0) {
        out->insert(out->end(), _pool.begin(), _pool.end());
    } else {
        for (size_t i = 0; i < expected_size; ++i) {
            out->push_back(_pool[i]);
        }
    }
    _mutex.unlock();
}

Socket::SharedPart* Socket::GetOrNewSharedPartSlower() {
    // Create _shared_part optimistically.
    SharedPart* shared_part = GetSharedPart();
    if (shared_part == NULL) {
        shared_part = new SharedPart(id());
        shared_part->AddRefManually();
        SharedPart* expected = NULL;
        if (!_shared_part.compare_exchange_strong(
                expected, shared_part, butil::memory_order_acq_rel)) {
            shared_part->RemoveRefManually();
            CHECK(expected);
            shared_part = expected;
        }
    }
    return shared_part;
}

void Socket::ShareStats(Socket* main_socket) {
    SharedPart* main_sp = main_socket->GetOrNewSharedPart();
    main_sp->AddRefManually();
    SharedPart* my_sp =
        _shared_part.exchange(main_sp, butil::memory_order_acq_rel);
    if (my_sp) {
        my_sp->RemoveRefManually();
    }
}

int Socket::GetPooledSocket(SocketUniquePtr* pooled_socket) {
    if (pooled_socket == NULL) {
        LOG(ERROR) << "pooled_socket is NULL";
        return -1;
    }
    SharedPart* main_sp = GetOrNewSharedPart();
    if (main_sp == NULL) {
        LOG(ERROR) << "_shared_part is NULL";
        return -1;
    }
    // Create socket_pool optimistically.
    SocketPool* socket_pool = main_sp->socket_pool.load(butil::memory_order_consume);
    if (socket_pool == NULL) {
        SocketOptions opt;
        opt.remote_side = remote_side();
        opt.user = user();
        opt.on_edge_triggered_events = _on_edge_triggered_events;
        opt.initial_ssl_ctx = _ssl_ctx;
        opt.keytable_pool = _keytable_pool;
        opt.app_connect = _app_connect;
        socket_pool = new SocketPool(opt);
        SocketPool* expected = NULL;
        if (!main_sp->socket_pool.compare_exchange_strong(
                expected, socket_pool, butil::memory_order_acq_rel)) {
            delete socket_pool;
            CHECK(expected);
            socket_pool = expected;
        }
    }
    if (socket_pool->GetSocket(pooled_socket) != 0) {
        return -1;
    }
    (*pooled_socket)->ShareStats(this);
    CHECK((*pooled_socket)->parsing_context() == NULL)
        << "context=" << (*pooled_socket)->parsing_context()
        << " is not NULL when " << *(*pooled_socket) << " is got from"
        " SocketPool, the protocol implementation is buggy";
    return 0;
}

int Socket::ReturnToPool() {
    SharedPart* sp = _shared_part.exchange(NULL, butil::memory_order_acquire);
    if (sp == NULL) {
        LOG(ERROR) << "_shared_part is NULL";
        SetFailed(EINVAL, "_shared_part is NULL");
        return -1;
    }
    SocketPool* pool = sp->socket_pool.load(butil::memory_order_consume);
    if (pool == NULL) {
        LOG(ERROR) << "_shared_part->socket_pool is NULL";
        SetFailed(EINVAL, "_shared_part->socket_pool is NULL");
        sp->RemoveRefManually();
        return -1;
    }
    CHECK(parsing_context() == NULL)
        << "context=" << parsing_context() << " is not released when "
        << *this << " is returned to SocketPool, the protocol "
        "implementation is buggy";
    // NOTE: be careful with the sequence.
    // - related fields must be reset before returning to pool
    // - sp must be released after returning to pool because it owns pool
    _connection_type_for_progressive_read = CONNECTION_TYPE_UNKNOWN;
    _controller_released_socket.store(false, butil::memory_order_relaxed);
    pool->ReturnSocket(this);
    sp->RemoveRefManually();
    return 0;
}

bool Socket::HasSocketPool() const {
    SharedPart* sp = GetSharedPart();
    if (sp != NULL) {
        return sp->socket_pool.load(butil::memory_order_consume) != NULL;
    }
    return false;
}

void Socket::ListPooledSockets(std::vector<SocketId>* out, size_t max_count) {
    out->clear();
    SharedPart* sp = GetSharedPart();
    if (sp == NULL) {
        return;
    }
    SocketPool* pool = sp->socket_pool.load(butil::memory_order_consume);
    if (pool == NULL) {
        return;
    }
    pool->ListSockets(out, max_count);
}

bool Socket::GetPooledSocketStats(int* numfree, int* numinflight) {
    SharedPart* sp = GetSharedPart();
    if (sp == NULL) {
        return false;
    }
    SocketPool* pool = sp->socket_pool.load(butil::memory_order_consume);
    if (pool == NULL) {
        return false;
    }
    *numfree = pool->_numfree.load(butil::memory_order_relaxed);
    *numinflight = pool->_numinflight.load(butil::memory_order_relaxed);
    return true;
}
    
int Socket::GetShortSocket(SocketUniquePtr* short_socket) {
    if (short_socket == NULL) {
        LOG(ERROR) << "short_socket is NULL";
        return -1;
    }
    SocketId id;
    SocketOptions opt;
    opt.remote_side = remote_side();
    opt.user = user();
    opt.on_edge_triggered_events = _on_edge_triggered_events;
    opt.initial_ssl_ctx = _ssl_ctx;
    opt.keytable_pool = _keytable_pool;
    opt.app_connect = _app_connect;
    if (get_client_side_messenger()->Create(opt, &id) != 0 ||
        Socket::Address(id, short_socket) != 0) {
        return -1;
    }
    (*short_socket)->ShareStats(this);
    return 0;
}

int Socket::GetAgentSocket(SocketUniquePtr* out, bool (*checkfn)(Socket*)) {
    SocketId id = _agent_socket_id.load(butil::memory_order_relaxed);
    SocketUniquePtr tmp_sock;
    do {
        if (Socket::Address(id, &tmp_sock) == 0) {
            if (checkfn == NULL || checkfn(tmp_sock.get())) {
                out->swap(tmp_sock);
                return 0;
            }
            tmp_sock->ReleaseAdditionalReference();
        }
        do {
            if (GetShortSocket(&tmp_sock) != 0) {
                LOG(ERROR) << "Fail to get short socket from " << *this;
                return -1;
            }
            if (checkfn == NULL || checkfn(tmp_sock.get())) {
                break;
            }
            tmp_sock->ReleaseAdditionalReference();
        } while (1);

        if (_agent_socket_id.compare_exchange_strong(
                id, tmp_sock->id(), butil::memory_order_acq_rel)) {
            out->swap(tmp_sock);
            return 0;
        }
        tmp_sock->ReleaseAdditionalReference();
        // id was updated, re-address
    } while (1);
}

int Socket::PeekAgentSocket(SocketUniquePtr* out) const {
    SocketId id = _agent_socket_id.load(butil::memory_order_relaxed);
    if (id == INVALID_SOCKET_ID) {
        return -1;
    }
    return Address(id, out);
}

void Socket::GetStat(SocketStat* s) const {
    BAIDU_CASSERT(offsetof(Socket, _preferred_index) >= 64, different_cacheline);
    BAIDU_CASSERT(sizeof(WriteRequest) == 64, sizeof_write_request_is_64);

    SharedPart* sp = GetSharedPart();
    if (sp != NULL && sp->extended_stat != NULL) {
        *s = *sp->extended_stat;
    } else {
        memset(s, 0, sizeof(*s));
    }
}

void Socket::AddInputBytes(size_t bytes) {
    GetOrNewSharedPart()->in_size.fetch_add(bytes, butil::memory_order_relaxed);
}
void Socket::AddInputMessages(size_t count) {
    GetOrNewSharedPart()->in_num_messages.fetch_add(count, butil::memory_order_relaxed);
}
void Socket::CancelUnwrittenBytes(size_t bytes) {
    const int64_t before_minus =
        _unwritten_bytes.fetch_sub(bytes, butil::memory_order_relaxed);
    if (before_minus < (int64_t)bytes + FLAGS_socket_max_unwritten_bytes) {
        _overcrowded = false;
    }
}
void Socket::AddOutputBytes(size_t bytes) {
    GetOrNewSharedPart()->out_size.fetch_add(bytes, butil::memory_order_relaxed);
    _last_writetime_us.store(butil::cpuwide_time_us(),
                             butil::memory_order_relaxed);
    CancelUnwrittenBytes(bytes);
}
void Socket::AddOutputMessages(size_t count) {
    GetOrNewSharedPart()->out_num_messages.fetch_add(count, butil::memory_order_relaxed);
}

SocketId Socket::main_socket_id() const {
    SharedPart* sp = GetSharedPart();
    if (sp) {
        return sp->creator_socket_id;
    }
    return INVALID_SOCKET_ID;
}

void Socket::OnProgressiveReadCompleted() {
    if (is_read_progressive() &&
        (_controller_released_socket.load(butil::memory_order_relaxed) ||
         _controller_released_socket.exchange(
             true, butil::memory_order_relaxed))) {
        if (_connection_type_for_progressive_read == CONNECTION_TYPE_POOLED) {
            ReturnToPool();
        } else if (_connection_type_for_progressive_read == CONNECTION_TYPE_SHORT) {
            SetFailed(EUNUSED, "[%s]Close short connection", __FUNCTION__);
        }
    }
}

std::string Socket::description() const {
    // NOTE: The output should be consistent with operator<<()
    std::string result;
    result.reserve(64);
    butil::string_appendf(&result, "Socket{id=%" PRIu64, id());
    const int saved_fd = fd();
    if (saved_fd >= 0) {
        butil::string_appendf(&result, " fd=%d", saved_fd);
    }
    butil::string_appendf(&result, " addr=%s",
                          butil::endpoint2str(remote_side()).c_str());
    const int local_port = local_side().port;
    if (local_port > 0) {
        butil::string_appendf(&result, ":%d", local_port);
    }
    butil::string_appendf(&result, "} (0x%p)", this);
    return result;
}

SocketSSLContext::SocketSSLContext()
    : raw_ctx(NULL)
{}

SocketSSLContext::~SocketSSLContext() {
    if (raw_ctx) {
        SSL_CTX_free(raw_ctx);
    }
}

} // namespace brpc


namespace std {
ostream& operator<<(ostream& os, const brpc::Socket& sock) {
    // NOTE: The output should be consistent with Socket::description()
    os << "Socket{id=" << sock.id();
    const int fd = sock.fd();
    if (fd >= 0) {
        os << " fd=" << fd;
    }
    os << " addr=" << sock.remote_side();
    const int local_port = sock.local_side().port;
    if (local_port > 0) {
        os << ':' << local_port;
    }
    os << "} (" << (void*)&sock << ')';
    return os;
}
}
