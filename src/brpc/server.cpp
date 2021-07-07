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


#include <wordexp.h>                                // wordexp
#include <iomanip>
#include <arpa/inet.h>                              // inet_aton
#include <fcntl.h>                                  // O_CREAT
#include <sys/stat.h>                               // mkdir
#include <gflags/gflags.h>
#include <google/protobuf/descriptor.h>             // ServiceDescriptor
#include "idl_options.pb.h"                         // option(idl_support)
#include "bthread/unstable.h"                       // bthread_keytable_pool_init
#include "butil/macros.h"                            // ARRAY_SIZE
#include "butil/fd_guard.h"                          // fd_guard
#include "butil/logging.h"                           // CHECK
#include "butil/time.h"
#include "butil/class_name.h"
#include "butil/string_printf.h"
#include "brpc/log.h"
#include "brpc/compress.h"
#include "brpc/policy/nova_pbrpc_protocol.h"
#include "brpc/global.h"
#include "brpc/socket_map.h"                   // SocketMapList
#include "brpc/acceptor.h"                     // Acceptor
#include "brpc/details/ssl_helper.h"           // CreateServerSSLContext
#include "brpc/protocol.h"                     // ListProtocols
#include "brpc/nshead_service.h"               // NsheadService
#ifdef ENABLE_THRIFT_FRAMED_PROTOCOL
#include "brpc/thrift_service.h"               // ThriftService
#endif
#include "brpc/builtin/bad_method_service.h"   // BadMethodService
#include "brpc/builtin/get_favicon_service.h"
#include "brpc/builtin/get_js_service.h"
#include "brpc/builtin/version_service.h"
#include "brpc/builtin/health_service.h"
#include "brpc/builtin/list_service.h"
#include "brpc/builtin/status_service.h"
#include "brpc/builtin/protobufs_service.h"
#include "brpc/builtin/threads_service.h"
#include "brpc/builtin/vlog_service.h"
#include "brpc/builtin/index_service.h"        // IndexService
#include "brpc/builtin/connections_service.h"  // ConnectionsService
#include "brpc/builtin/flags_service.h"        // FlagsService
#include "brpc/builtin/vars_service.h"         // VarsService
#include "brpc/builtin/rpcz_service.h"         // RpczService
#include "brpc/builtin/dir_service.h"          // DirService
#include "brpc/builtin/pprof_service.h"        // PProfService
#include "brpc/builtin/bthreads_service.h"     // BthreadsService
#include "brpc/builtin/ids_service.h"          // IdsService
#include "brpc/builtin/sockets_service.h"      // SocketsService
#include "brpc/builtin/hotspots_service.h"     // HotspotsService
#include "brpc/builtin/prometheus_metrics_service.h"
#include "brpc/details/method_status.h"
#include "brpc/load_balancer.h"
#include "brpc/naming_service.h"
#include "brpc/simple_data_pool.h"
#include "brpc/server.h"
#include "brpc/trackme.h"
#include "brpc/restful.h"
#include "brpc/rtmp.h"
#include "brpc/builtin/common.h"               // GetProgramName
#include "brpc/details/tcmalloc_extension.h"

inline std::ostream& operator<<(std::ostream& os, const timeval& tm) {
    const char old_fill = os.fill();
    os << tm.tv_sec << '.' << std::setw(6) << std::setfill('0') << tm.tv_usec;
    os.fill(old_fill);
    return os;
}

extern "C" {
void* bthread_get_assigned_data();
}

namespace brpc {

BAIDU_CASSERT(sizeof(int32_t) == sizeof(butil::subtle::Atomic32),
              Atomic32_must_be_int32);

extern const char* const g_server_info_prefix = "rpc_server";

const char* status_str(Server::Status s) {
    switch (s) {
    case Server::UNINITIALIZED: return "UNINITIALIZED";
    case Server::READY: return "READY";
    case Server::RUNNING: return "RUNNING";
    case Server::STOPPING: return "STOPPING";
    }
    return "UNKNOWN_STATUS";
}

butil::static_atomic<int> g_running_server_count = BUTIL_STATIC_ATOMIC_INIT(0);

// Following services may have security issues and are disabled by default.
DEFINE_bool(enable_dir_service, false, "Enable /dir");
DEFINE_bool(enable_threads_service, false, "Enable /threads");

DECLARE_int32(usercode_backup_threads);
DECLARE_bool(usercode_in_pthread);

const int INITIAL_SERVICE_CAP = 64;
const int INITIAL_CERT_MAP = 64;
// NOTE: never make s_ncore extern const whose ctor seq against other
// compilation units is undefined.
const int s_ncore = sysconf(_SC_NPROCESSORS_ONLN);

ServerOptions::ServerOptions()
    : idle_timeout_sec(-1)
    , nshead_service(NULL)
    , thrift_service(NULL)
    , mongo_service_adaptor(NULL)
    , auth(NULL)
    , server_owns_auth(false)
    , num_threads(8)
    , max_concurrency(0)
    , session_local_data_factory(NULL)
    , reserved_session_local_data(0)
    , thread_local_data_factory(NULL)
    , reserved_thread_local_data(0)
    , bthread_init_fn(NULL)
    , bthread_init_args(NULL)
    , bthread_init_count(0)
    , internal_port(-1)
    , has_builtin_services(true)
    , http_master_service(NULL)
    , health_reporter(NULL)
    , rtmp_service(NULL)
    , redis_service(NULL) {
    if (s_ncore > 0) {
        num_threads = s_ncore + 1;
    }
}

ServerSSLOptions* ServerOptions::mutable_ssl_options() {
    if (!_ssl_options) {
        _ssl_options.reset(new ServerSSLOptions);
    }
    return _ssl_options.get();
}

Server::MethodProperty::OpaqueParams::OpaqueParams()
    : is_tabbed(false)
    , allow_default_url(false)
    , allow_http_body_to_pb(true)
    , pb_bytes_to_base64(false) {
}

Server::MethodProperty::MethodProperty()
    : is_builtin_service(false)
    , own_method_status(false)
    , http_url(NULL)
    , service(NULL)
    , method(NULL)
    , status(NULL) {
}

static timeval GetUptime(void* arg/*start_time*/) {
    return butil::microseconds_to_timeval(butil::cpuwide_time_us() - (intptr_t)arg);
}

static void PrintStartTime(std::ostream& os, void* arg) {
    // Print when this server was Server::Start()-ed.
    time_t start_time = static_cast<Server*>(arg)->last_start_time();
    struct tm timeinfo;
    char buf[64];
    strftime(buf, sizeof(buf), "%Y/%m/%d-%H:%M:%S",
             localtime_r(&start_time, &timeinfo));
    os << buf;
}

static void PrintSupportedLB(std::ostream& os, void*) {
    LoadBalancerExtension()->List(os, ' ');
}

static void PrintSupportedNS(std::ostream& os, void*) {
    NamingServiceExtension()->List(os, ' ');
}

static void PrintSupportedProtocols(std::ostream& os, void*) {
    std::vector<Protocol> protocols;
    ListProtocols(&protocols);
    for (size_t i = 0; i < protocols.size(); ++i) {
        if (i != 0) {
            os << ' ';
        }
        os << (protocols[i].name ? protocols[i].name : "(null)");
    }
}

static void PrintSupportedCompressions(std::ostream& os, void*) {
    std::vector<CompressHandler> compressors;
    ListCompressHandler(&compressors);
    for (size_t i = 0; i < compressors.size(); ++i) {
        if (i != 0) {
            os << ' ';
        }
        os << (compressors[i].name ? compressors[i].name : "(null)");
    }
}

static void PrintEnabledProfilers(std::ostream& os, void*) {
    if (cpu_profiler_enabled) {
        os << "cpu ";
    }
    if (IsHeapProfilerEnabled()) {
        if (has_TCMALLOC_SAMPLE_PARAMETER()) {
            os << "heap ";
        } else {
            os << "heap(no TCMALLOC_SAMPLE_PARAMETER in env) ";
        }
    }
    os << "contention";
}

static bvar::PassiveStatus<std::string> s_lb_st(
    "rpc_load_balancer", PrintSupportedLB, NULL);

static bvar::PassiveStatus<std::string> s_ns_st(
    "rpc_naming_service", PrintSupportedNS, NULL);

static bvar::PassiveStatus<std::string> s_proto_st(
    "rpc_protocols", PrintSupportedProtocols, NULL);

static bvar::PassiveStatus<std::string> s_comp_st(
    "rpc_compressions", PrintSupportedCompressions, NULL);

static bvar::PassiveStatus<std::string> s_prof_st(
    "rpc_profilers", PrintEnabledProfilers, NULL);

static int32_t GetConnectionCount(void* arg) {
    ServerStatistics ss;
    static_cast<Server*>(arg)->GetStat(&ss);
    return ss.connection_count;
}

static int32_t GetServiceCount(void* arg) {
    ServerStatistics ss;
    static_cast<Server*>(arg)->GetStat(&ss);
    return ss.user_service_count;
}

static int32_t GetBuiltinServiceCount(void* arg) {
    ServerStatistics ss;
    static_cast<Server*>(arg)->GetStat(&ss);
    return ss.builtin_service_count;
}

static bvar::Vector<unsigned, 2> GetSessionLocalDataCount(void* arg) {
    bvar::Vector<unsigned, 2> v;
    SimpleDataPool::Stat s =
        static_cast<Server*>(arg)->session_local_data_pool()->stat();
    v[0] = s.ncreated - s.nfree;
    v[1] = s.nfree;
    return v;
}

std::string Server::ServerPrefix() const {
    return butil::string_printf("%s_%d", g_server_info_prefix, listen_address().port);
}

void* Server::UpdateDerivedVars(void* arg) {
    const int64_t start_us = butil::cpuwide_time_us();

    Server* server = static_cast<Server*>(arg);
    const std::string prefix = server->ServerPrefix();
    std::vector<SocketId> conns;
    std::vector<SocketId> internal_conns;

    server->_nerror_bvar.expose_as(prefix, "error");

    bvar::PassiveStatus<timeval> uptime_st(
        prefix, "uptime", GetUptime, (void*)(intptr_t)start_us);

    bvar::PassiveStatus<std::string> start_time_st(
        prefix, "start_time", PrintStartTime, server);

    bvar::PassiveStatus<int32_t> nconn_st(
        prefix, "connection_count", GetConnectionCount, server);

    bvar::PassiveStatus<int32_t> nservice_st(
        prefix, "service_count", GetServiceCount, server);

    bvar::PassiveStatus<int32_t> nbuiltinservice_st(
        prefix, "builtin_service_count", GetBuiltinServiceCount, server);

    bvar::PassiveStatus<bvar::Vector<unsigned, 2> > nsessiondata_st(
        GetSessionLocalDataCount, server);
    if (server->session_local_data_pool()) {
        nsessiondata_st.expose_as(prefix, "session_local_data_count");
        nsessiondata_st.set_vector_names("using,free");
    }

    std::string mprefix = prefix;
    for (MethodMap::iterator it = server->_method_map.begin();
         it != server->_method_map.end(); ++it) {
        // Not expose counters on builtin services.
        if (!it->second.is_builtin_service) {
            mprefix.resize(prefix.size());
            mprefix.push_back('_');
            bvar::to_underscored_name(&mprefix, it->second.method->full_name());
            it->second.status->Expose(mprefix);
        }
    }
    if (server->options().nshead_service) {
        server->options().nshead_service->Expose(prefix);
    }

#ifdef ENABLE_THRIFT_FRAMED_PROTOCOL
    if (server->options().thrift_service) {
        server->options().thrift_service->Expose(prefix);
    }
#endif

    int64_t last_time = butil::gettimeofday_us();
    int consecutive_nosleep = 0;
    while (1) {
        const int64_t sleep_us = 1000000L + last_time - butil::gettimeofday_us();
        if (sleep_us < 1000L) {
            if (++consecutive_nosleep >= 2) {
                consecutive_nosleep = 0;
                LOG(WARNING) << __FUNCTION__ << " is too busy!";
            }
        } else {
            consecutive_nosleep = 0;
            if (bthread_usleep(sleep_us) < 0) {
                PLOG_IF(ERROR, errno != ESTOP) << "Fail to sleep";
                return NULL;
            }
        }
        last_time = butil::gettimeofday_us();

        // Update stats of accepted sockets.
        if (server->_am) {
            server->_am->ListConnections(&conns);
        }
        if (server->_internal_am) {
            server->_internal_am->ListConnections(&internal_conns);
        }
        const int64_t now_ms = butil::cpuwide_time_ms();
        for (size_t i = 0; i < conns.size(); ++i) {
            SocketUniquePtr ptr;
            if (Socket::Address(conns[i], &ptr) == 0) {
                ptr->UpdateStatsEverySecond(now_ms);
            }
        }
        for (size_t i = 0; i < internal_conns.size(); ++i) {
            SocketUniquePtr ptr;
            if (Socket::Address(internal_conns[i], &ptr) == 0) {
                ptr->UpdateStatsEverySecond(now_ms);
            }
        }
    }
}

const std::string& Server::ServiceProperty::service_name() const {
    if (service) {
        return service->GetDescriptor()->full_name();
    } else if (restful_map) {
        return restful_map->service_name();
    }
    const static std::string s_unknown_name = "";
    return s_unknown_name;
}

Server::Server(ProfilerLinker)
    : _session_local_data_pool(NULL)
    , _status(UNINITIALIZED)
    , _builtin_service_count(0)
    , _virtual_service_count(0)
    , _failed_to_set_max_concurrency_of_method(false)
    , _am(NULL)
    , _internal_am(NULL)
    , _first_service(NULL)
    , _tab_info_list(NULL)
    , _global_restful_map(NULL)
    , _last_start_time(0)
    , _derivative_thread(INVALID_BTHREAD)
    , _keytable_pool(NULL)
    , _concurrency(0) {
    BAIDU_CASSERT(offsetof(Server, _concurrency) % 64 == 0,
                  Server_concurrency_must_be_aligned_by_cacheline);
}

Server::~Server() {
    Stop(0);
    Join();
    ClearServices();
    FreeSSLContexts();

    delete _session_local_data_pool;
    _session_local_data_pool = NULL;

    delete _options.nshead_service;
    _options.nshead_service = NULL;

#ifdef ENABLE_THRIFT_FRAMED_PROTOCOL
    delete _options.thrift_service;
    _options.thrift_service = NULL;
#endif

    delete _options.http_master_service;
    _options.http_master_service = NULL;

    delete _am;
    _am = NULL;
    delete _internal_am;
    _internal_am = NULL;

    delete _tab_info_list;
    _tab_info_list = NULL;

    delete _global_restful_map;
    _global_restful_map = NULL;

    if (!_options.pid_file.empty()) {
        unlink(_options.pid_file.c_str());
    }
    if (_options.server_owns_auth) {
        delete _options.auth;
        _options.auth = NULL;
    }

    delete _options.redis_service;
    _options.redis_service = NULL;
}

int Server::AddBuiltinServices() {
    // Firstly add services shown in tabs.
    if (AddBuiltinService(new (std::nothrow) StatusService)) {
        LOG(ERROR) << "Fail to add StatusService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) VarsService)) {
        LOG(ERROR) << "Fail to add VarsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) ConnectionsService)) {
        LOG(ERROR) << "Fail to add ConnectionsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) FlagsService)) {
        LOG(ERROR) << "Fail to add FlagsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) RpczService)) {
        LOG(ERROR) << "Fail to add RpczService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) HotspotsService)) {
        LOG(ERROR) << "Fail to add HotspotsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) IndexService)) {
        LOG(ERROR) << "Fail to add IndexService";
        return -1;
    }

    // Add other services.
    if (AddBuiltinService(new (std::nothrow) VersionService(this))) {
        LOG(ERROR) << "Fail to add VersionService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) HealthService)) {
        LOG(ERROR) << "Fail to add HealthService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) ProtobufsService(this))) {
        LOG(ERROR) << "Fail to add ProtobufsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) BadMethodService)) {
        LOG(ERROR) << "Fail to add BadMethodService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) ListService(this))) {
        LOG(ERROR) << "Fail to add ListService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) PrometheusMetricsService)) {
        LOG(ERROR) << "Fail to add MetricsService";
        return -1;
    }
    if (FLAGS_enable_threads_service &&
        AddBuiltinService(new (std::nothrow) ThreadsService)) {
        LOG(ERROR) << "Fail to add ThreadsService";
        return -1;
    }

#if !BRPC_WITH_GLOG
    if (AddBuiltinService(new (std::nothrow) VLogService)) {
        LOG(ERROR) << "Fail to add VLogService";
        return -1;
    }
#endif

    if (AddBuiltinService(new (std::nothrow) PProfService)) {
        LOG(ERROR) << "Fail to add PProfService";
        return -1;
    }
    if (FLAGS_enable_dir_service &&
        AddBuiltinService(new (std::nothrow) DirService)) {
        LOG(ERROR) << "Fail to add DirService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) BthreadsService)) {
        LOG(ERROR) << "Fail to add BthreadsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) IdsService)) {
        LOG(ERROR) << "Fail to add IdsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) SocketsService)) {
        LOG(ERROR) << "Fail to add SocketsService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) GetFaviconService)) {
        LOG(ERROR) << "Fail to add GetFaviconService";
        return -1;
    }
    if (AddBuiltinService(new (std::nothrow) GetJsService)) {
        LOG(ERROR) << "Fail to add GetJsService";
        return -1;
    }
    return 0;
}

bool is_http_protocol(const char* name) {
    if (name[0] != 'h') {
        return false;
    }
    return strcmp(name, "http") == 0 || strcmp(name, "h2") == 0;
}

Acceptor* Server::BuildAcceptor() {
    std::set<std::string> whitelist;
    for (butil::StringSplitter sp(_options.enabled_protocols.c_str(), ' ');
         sp; ++sp) {
        std::string protocol(sp.field(), sp.length());
        whitelist.insert(protocol);
    }
    const bool has_whitelist = !whitelist.empty();
    Acceptor* acceptor = new (std::nothrow) Acceptor(_keytable_pool);
    if (NULL == acceptor) {
        LOG(ERROR) << "Fail to new Acceptor";
        return NULL;
    }
    InputMessageHandler handler;
    std::vector<Protocol> protocols;
    ListProtocols(&protocols);
    for (size_t i = 0; i < protocols.size(); ++i) {
        if (protocols[i].process_request == NULL) {
            // The protocol does not support server-side.
            continue;
        }
        if (has_whitelist &&
            !is_http_protocol(protocols[i].name) &&
            !whitelist.erase(protocols[i].name)) {
            // the protocol is not allowed to serve.
            RPC_VLOG << "Skip protocol=" << protocols[i].name;
            continue;
        }
        // `process_request' is required at server side
        handler.parse = protocols[i].parse;
        handler.process = protocols[i].process_request;
        handler.verify = protocols[i].verify;
        handler.arg = this;
        handler.name = protocols[i].name;
        if (acceptor->AddHandler(handler) != 0) {
            LOG(ERROR) << "Fail to add handler into Acceptor("
                       << acceptor << ')';
            delete acceptor;
            return NULL;
        }
    }
    if (!whitelist.empty()) {
        std::ostringstream err;
        err << "ServerOptions.enabled_protocols has unknown protocols=`";
        for (std::set<std::string>::const_iterator it = whitelist.begin();
             it != whitelist.end(); ++it) {
            err << *it << ' ';
        }
        err << '\'';
        delete acceptor;
        LOG(ERROR) << err.str();
        return NULL;
    }
    return acceptor;
}

int Server::InitializeOnce() {
    if (_status != UNINITIALIZED) {
        return 0;
    }
    GlobalInitializeOrDie();

    if (_status != UNINITIALIZED) {
        return 0;
    }
    if (_fullname_service_map.init(INITIAL_SERVICE_CAP) != 0) {
        LOG(ERROR) << "Fail to init _fullname_service_map";
        return -1;
    }
    if (_service_map.init(INITIAL_SERVICE_CAP) != 0) {
        LOG(ERROR) << "Fail to init _service_map";
        return -1;
    }
    if (_method_map.init(INITIAL_SERVICE_CAP * 2) != 0) {
        LOG(ERROR) << "Fail to init _method_map";
        return -1;
    }
    if (_ssl_ctx_map.init(INITIAL_CERT_MAP) != 0) {
        LOG(ERROR) << "Fail to init _ssl_ctx_map";
        return -1;
    }
    _status = READY;
    return 0;
}

static void* CreateServerTLS(const void* args) {
    return static_cast<const DataFactory*>(args)->CreateData();
}
static void DestroyServerTLS(void* data, const void* void_factory) {
    static_cast<const DataFactory*>(void_factory)->DestroyData(data);
}

struct BthreadInitArgs {
    bool (*bthread_init_fn)(void* args); // default: NULL (do nothing)
    void* bthread_init_args;             // default: NULL
    bool result;
    bool done;
    bool stop;
    bthread_t th;
};

static void* BthreadInitEntry(void* void_args) {
    BthreadInitArgs* args = (BthreadInitArgs*)void_args;
    args->result = args->bthread_init_fn(args->bthread_init_args);
    args->done = true;
    while (!args->stop) {
        bthread_usleep(1000);
    }
    return NULL;
}

struct RevertServerStatus {
    inline void operator()(Server* s) const {
        if (s != NULL) {
            s->Stop(0);
            s->Join();
        }
    }
};

static int get_port_from_fd(int fd) {
    struct sockaddr_in addr;
    socklen_t size = sizeof(addr);
    if (getsockname(fd, (struct sockaddr*)&addr, &size) < 0) {
        return -1;
    }
    return ntohs(addr.sin_port);
}

static bool CreateConcurrencyLimiter(const AdaptiveMaxConcurrency& amc,
                                     ConcurrencyLimiter** out) {
    if (amc.type() == AdaptiveMaxConcurrency::UNLIMITED()) {
        *out = NULL;
        return true;
    }
    const ConcurrencyLimiter* cl =
        ConcurrencyLimiterExtension()->Find(amc.type().c_str());
    if (cl == NULL) {
        LOG(ERROR) << "Fail to find ConcurrencyLimiter by `" << amc.value() << "'";
        return false;
    }
    ConcurrencyLimiter* cl_copy = cl->New(amc);
    if (cl_copy == NULL) {
        LOG(ERROR) << "Fail to new ConcurrencyLimiter";
        return false;
    }
    *out = cl_copy;
    return true;
}

static AdaptiveMaxConcurrency g_default_max_concurrency_of_method(0);

int Server::StartInternal(const butil::ip_t& ip,
                          const PortRange& port_range,
                          const ServerOptions *opt) {
    std::unique_ptr<Server, RevertServerStatus> revert_server(this);
    if (_failed_to_set_max_concurrency_of_method) {
        _failed_to_set_max_concurrency_of_method = false;
        LOG(ERROR) << "previous call to MaxConcurrencyOf() was failed, "
            "fix it before starting server";
        return -1;
    }
    if (InitializeOnce() != 0) {
        LOG(ERROR) << "Fail to initialize Server[" << version() << ']';
        return -1;
    }
    const Status st = status();
    if (st != READY) {
        if (st == RUNNING) {
            LOG(ERROR) << "Server[" << version() << "] is already running on "
                       << _listen_addr;
        } else {
            LOG(ERROR) << "Can't start Server[" << version()
                       << "] which is " << status_str(status());
        }
        return -1;
    }
    if (opt) {
        _options = *opt;
    } else {
        // Always reset to default options explicitly since `_options'
        // may be the options for the last run or even bad options
        _options = ServerOptions();
    }

    if (!_options.h2_settings.IsValid(true/*log_error*/)) {
        LOG(ERROR) << "Invalid h2_settings";
        return -1;
    }

    if (_options.http_master_service) {
        // Check requirements for http_master_service:
        //  has "default_method" & request/response have no fields
        const google::protobuf::ServiceDescriptor* sd =
            _options.http_master_service->GetDescriptor();
        const google::protobuf::MethodDescriptor* md =
            sd->FindMethodByName("default_method");
        if (md == NULL) {
            LOG(ERROR) << "http_master_service must have a method named `default_method'";
            return -1;
        }
        if (md->input_type()->field_count() != 0) {
            LOG(ERROR) << "The request type of http_master_service must have "
                "no fields, actually " << md->input_type()->field_count();
            return -1;
        }
        if (md->output_type()->field_count() != 0) {
            LOG(ERROR) << "The response type of http_master_service must have "
                "no fields, actually " << md->output_type()->field_count();
            return -1;
        }
    }

    // CAUTION:
    //   Following code may run multiple times if this server is started and
    //   stopped more than once. Reuse or delete previous resources!

    if (_options.session_local_data_factory) {
        if (_session_local_data_pool == NULL) {
            _session_local_data_pool =
                new (std::nothrow) SimpleDataPool(_options.session_local_data_factory);
            if (NULL == _session_local_data_pool) {
                LOG(ERROR) << "Fail to new SimpleDataPool";
                return -1;
            }
        } else {
            _session_local_data_pool->Reset(_options.session_local_data_factory);
        }
        _session_local_data_pool->Reserve(_options.reserved_session_local_data);
    }

    // Init _keytable_pool always. If the server was stopped before, the pool
    // should be destroyed in Join().
    _keytable_pool = new bthread_keytable_pool_t;
    if (bthread_keytable_pool_init(_keytable_pool) != 0) {
        LOG(ERROR) << "Fail to init _keytable_pool";
        delete _keytable_pool;
        _keytable_pool = NULL;
        return -1;
    }

    if (_options.thread_local_data_factory) {
        _tl_options.thread_local_data_factory = _options.thread_local_data_factory;
        if (bthread_key_create2(&_tl_options.tls_key, DestroyServerTLS,
                                _options.thread_local_data_factory) != 0) {
            LOG(ERROR) << "Fail to create thread-local key";
            return -1;
        }
        if (_options.reserved_thread_local_data) {
            bthread_keytable_pool_reserve(_keytable_pool,
                                          _options.reserved_thread_local_data,
                                          _tl_options.tls_key,
                                          CreateServerTLS,
                                          _options.thread_local_data_factory);
        }
    } else {
        _tl_options = ThreadLocalOptions();
    }

    if (_options.bthread_init_count != 0 &&
        _options.bthread_init_fn != NULL) {
        // Create some special bthreads to call the init functions. The
        // bthreads will not quit until all bthreads finish the init function.
        BthreadInitArgs* init_args
            = new BthreadInitArgs[_options.bthread_init_count];
        size_t ncreated = 0;
        for (size_t i = 0; i < _options.bthread_init_count; ++i, ++ncreated) {
            init_args[i].bthread_init_fn = _options.bthread_init_fn;
            init_args[i].bthread_init_args = _options.bthread_init_args;
            init_args[i].result = false;
            init_args[i].done = false;
            init_args[i].stop = false;
            bthread_attr_t tmp = BTHREAD_ATTR_NORMAL;
            tmp.keytable_pool = _keytable_pool;
            if (bthread_start_background(
                    &init_args[i].th, &tmp, BthreadInitEntry, &init_args[i]) != 0) {
                break;
            }
        }
        // Wait until all created bthreads finish the init function.
        for (size_t i = 0; i < ncreated; ++i) {
            while (!init_args[i].done) {
                bthread_usleep(1000);
            }
        }
        // Stop and join created bthreads.
        for (size_t i = 0; i < ncreated; ++i) {
            init_args[i].stop = true;
        }
        for (size_t i = 0; i < ncreated; ++i) {
            bthread_join(init_args[i].th, NULL);
        }
        size_t num_failed_result = 0;
        for (size_t i = 0; i < ncreated; ++i) {
            if (!init_args[i].result) {
                ++num_failed_result;
            }
        }
        delete [] init_args;
        if (ncreated != _options.bthread_init_count) {
            LOG(ERROR) << "Fail to create "
                       << _options.bthread_init_count - ncreated << " bthreads";
            return -1;
        }
        if (num_failed_result != 0) {
            LOG(ERROR) << num_failed_result << " bthread_init_fn failed";
            return -1;
        }
    }

    // Free last SSL contexts
    FreeSSLContexts();
    if (_options.has_ssl_options()) {
        CertInfo& default_cert = _options.mutable_ssl_options()->default_cert;
        if (default_cert.certificate.empty()) {
            LOG(ERROR) << "default_cert is empty";
            return -1;
        }
        if (AddCertificate(default_cert) != 0) {
            return -1;
        }
        _default_ssl_ctx = _ssl_ctx_map.begin()->second.ctx;

        const std::vector<CertInfo>& certs = _options.mutable_ssl_options()->certs;
        for (size_t i = 0; i < certs.size(); ++i) {
            if (AddCertificate(certs[i]) != 0) {
                return -1;
            }
        }
    }

    _concurrency = 0;

    if (_options.has_builtin_services &&
        _builtin_service_count <= 0 &&
        AddBuiltinServices() != 0) {
        LOG(ERROR) << "Fail to add builtin services";
        return -1;
    }
    // If a server is started/stopped for mutiple times and one of the options
    // sets has_builtin_service to true, builtin services will be enabled for
    // any later re-start. Check this case and report to user.
    if (!_options.has_builtin_services && _builtin_service_count > 0) {
        LOG(ERROR) << "A server started/stopped for multiple times must be "
            "consistent on ServerOptions.has_builtin_services";
        return -1;
    }

    // Prepare all restful maps
    for (ServiceMap::const_iterator it = _fullname_service_map.begin();
         it != _fullname_service_map.end(); ++it) {
        if (it->second.restful_map) {
            it->second.restful_map->PrepareForFinding();
        }
    }
    if (_global_restful_map) {
        _global_restful_map->PrepareForFinding();
    }

    if (_options.num_threads > 0) {
        if (FLAGS_usercode_in_pthread) {
            _options.num_threads += FLAGS_usercode_backup_threads;
        }
        if (_options.num_threads < BTHREAD_MIN_CONCURRENCY) {
            _options.num_threads = BTHREAD_MIN_CONCURRENCY;
        }
        bthread_setconcurrency(_options.num_threads);
    }

    for (MethodMap::iterator it = _method_map.begin();
        it != _method_map.end(); ++it) {
        if (it->second.is_builtin_service) {
            it->second.status->SetConcurrencyLimiter(NULL);
        } else {
            const AdaptiveMaxConcurrency* amc = &it->second.max_concurrency;
            if (amc->type() == AdaptiveMaxConcurrency::UNLIMITED()) {
                amc = &_options.method_max_concurrency;
            }
            ConcurrencyLimiter* cl = NULL;
            if (!CreateConcurrencyLimiter(*amc, &cl)) {
                LOG(ERROR) << "Fail to create ConcurrencyLimiter for method";
                return -1;
            }
            it->second.status->SetConcurrencyLimiter(cl);
        }
    }

    // Create listening ports
    if (port_range.min_port > port_range.max_port) {
        LOG(ERROR) << "Invalid port_range=[" << port_range.min_port << '-'
                   << port_range.max_port << ']';
        return -1;
    }
    _listen_addr.ip = ip;
    for (int port = port_range.min_port; port <= port_range.max_port; ++port) {
        _listen_addr.port = port;
        butil::fd_guard sockfd(tcp_listen(_listen_addr));
        if (sockfd < 0) {
            if (port != port_range.max_port) { // not the last port, try next
                continue;
            }
            if (port_range.min_port != port_range.max_port) {
                LOG(ERROR) << "Fail to listen " << ip
                           << ":[" << port_range.min_port << '-'
                           << port_range.max_port << ']';
            } else {
                LOG(ERROR) << "Fail to listen " << _listen_addr;
            }
            return -1;
        }
        if (_listen_addr.port == 0) {
            // port=0 makes kernel dynamically select a port from
            // https://en.wikipedia.org/wiki/Ephemeral_port
            _listen_addr.port = get_port_from_fd(sockfd);
            if (_listen_addr.port <= 0) {
                LOG(ERROR) << "Fail to get port from fd=" << sockfd;
                return -1;
            }
        }
        if (_am == NULL) {
            _am = BuildAcceptor();
            if (NULL == _am) {
                LOG(ERROR) << "Fail to build acceptor";
                return -1;
            }
        }
        // Set `_status' to RUNNING before accepting connections
        // to prevent requests being rejected as ELOGOFF
        _status = RUNNING;
        time(&_last_start_time);
        GenerateVersionIfNeeded();
        g_running_server_count.fetch_add(1, butil::memory_order_relaxed);

        // Pass ownership of `sockfd' to `_am'
        if (_am->StartAccept(sockfd, _options.idle_timeout_sec,
                             _default_ssl_ctx) != 0) {
            LOG(ERROR) << "Fail to start acceptor";
            return -1;
        }
        sockfd.release();
        break; // stop trying
    }
    if (_options.internal_port >= 0 && _options.has_builtin_services) {
        if (_options.internal_port  == _listen_addr.port) {
            LOG(ERROR) << "ServerOptions.internal_port=" << _options.internal_port
                       << " is same with port=" << _listen_addr.port << " to Start()";
            return -1;
        }
        if (_options.internal_port == 0) {
            LOG(ERROR) << "ServerOptions.internal_port cannot be 0, which"
                " allocates a dynamic and probabaly unfiltered port,"
                " against the purpose of \"being internal\".";
            return -1;
        }
        butil::EndPoint internal_point = _listen_addr;
        internal_point.port = _options.internal_port;
        butil::fd_guard sockfd(tcp_listen(internal_point));
        if (sockfd < 0) {
            LOG(ERROR) << "Fail to listen " << internal_point << " (internal)";
            return -1;
        }
        if (NULL == _internal_am) {
            _internal_am = BuildAcceptor();
            if (NULL == _internal_am) {
                LOG(ERROR) << "Fail to build internal acceptor";
                return -1;
            }
        }
        // Pass ownership of `sockfd' to `_internal_am'
        if (_internal_am->StartAccept(sockfd, _options.idle_timeout_sec,
                                      _default_ssl_ctx) != 0) {
            LOG(ERROR) << "Fail to start internal_acceptor";
            return -1;
        }
        sockfd.release();
    }

    PutPidFileIfNeeded();

    // Launch _derivative_thread.
    CHECK_EQ(INVALID_BTHREAD, _derivative_thread);
    if (bthread_start_background(&_derivative_thread, NULL,
                                 UpdateDerivedVars, this) != 0) {
        LOG(ERROR) << "Fail to create _derivative_thread";
        return -1;
    }

    // Print tips to server launcher.
    int http_port = _listen_addr.port;
    std::ostringstream server_info;
    server_info << "Server[" << version() << "] is serving on port="
                << _listen_addr.port;
    if (_options.internal_port >= 0 && _options.has_builtin_services) {
        http_port = _options.internal_port;
        server_info << " and internal_port=" << _options.internal_port;
    }
    LOG(INFO) << server_info.str() << '.';

    if (_options.has_builtin_services) {
        LOG(INFO) << "Check out http://" << butil::my_hostname() << ':'
                  << http_port << " in web browser.";
    } else {
        LOG(WARNING) << "Builtin services are disabled according to "
            "ServerOptions.has_builtin_services";
    }
    // For trackme reporting
    SetTrackMeAddress(butil::EndPoint(butil::my_ip(), http_port));
    revert_server.release();
    return 0;
}

int Server::Start(const butil::EndPoint& endpoint, const ServerOptions* opt) {
    return StartInternal(
        endpoint.ip, PortRange(endpoint.port, endpoint.port), opt);
}

int Server::Start(const char* ip_port_str, const ServerOptions* opt) {
    butil::EndPoint point;
    if (str2endpoint(ip_port_str, &point) != 0 &&
        hostname2endpoint(ip_port_str, &point) != 0) {
        LOG(ERROR) << "Invalid address=`" << ip_port_str << '\'';
        return -1;
    }
    return Start(point, opt);
}

int Server::Start(int port, const ServerOptions* opt) {
    if (port < 0 || port > 65535) {
        LOG(ERROR) << "Invalid port=" << port;
        return -1;
    }
    return Start(butil::EndPoint(butil::IP_ANY, port), opt);
}

int Server::Start(const char* ip_str, PortRange port_range,
                  const ServerOptions *opt) {
    butil::ip_t ip;
    if (butil::str2ip(ip_str, &ip) != 0 &&
        butil::hostname2ip(ip_str, &ip) != 0) {
        LOG(ERROR) << "Invalid address=`" << ip_str << '\'';
        return -1;
    }
    return StartInternal(ip, port_range, opt);
}

int Server::Start(PortRange port_range, const ServerOptions* opt) {
    return StartInternal(butil::IP_ANY, port_range, opt);
}

int Server::Stop(int timeout_ms) {
    if (_status != RUNNING) {
        return -1;
    }
    _status = STOPPING;

    LOG(INFO) << "Server[" << version() << "] is going to quit";

    if (_am) {
        _am->StopAccept(timeout_ms);
    }
    if (_internal_am) {
        // TODO: calculate timeout?
        _internal_am->StopAccept(timeout_ms);
    }
    return 0;
}

// NOTE: Join() can happen before Stop().
int Server::Join() {
    if (_status != RUNNING && _status != STOPPING) {
        return -1;
    }
    if (_am) {
        _am->Join();
    }
    if (_internal_am) {
        _internal_am->Join();
    }

    if (_session_local_data_pool) {
        // We can't delete the pool right here because there's a bvar watching
        // this pool in _derivative_thread which does not quit yet.
        _session_local_data_pool->Reset(NULL);
    }

    if (_keytable_pool) {
        // Destroy _keytable_pool to delete keytables inside. This has to be
        // done here (before leaving Join) because it's legal for users to
        // delete bthread keys after Join which makes related objects
        // in KeyTables undeletable anymore and leaked.
        CHECK_EQ(0, bthread_keytable_pool_destroy(_keytable_pool));
        // TODO: Can't delete _keytable_pool which may be accessed by
        // still-running bthreads (created by the server). The memory is
        // leaked but servers are unlikely to be started/stopped frequently,
        // the leak is acceptable in most scenarios.
        _keytable_pool = NULL;
    }

    // Delete tls_key as well since we don't need it anymore.
    if (_tl_options.tls_key != INVALID_BTHREAD_KEY) {
        CHECK_EQ(0, bthread_key_delete(_tl_options.tls_key));
        _tl_options.tls_key = INVALID_BTHREAD_KEY;
    }

    // Have to join _derivative_thread, which may assume that server is running
    // and services in server are not mutated, otherwise data race happens
    // between Add/RemoveService after Join() and the thread.
    if (_derivative_thread != INVALID_BTHREAD) {
        bthread_stop(_derivative_thread);
        bthread_join(_derivative_thread, NULL);
        _derivative_thread = INVALID_BTHREAD;
    }

    g_running_server_count.fetch_sub(1, butil::memory_order_relaxed);
    _status = READY;
    return 0;
}

int Server::AddServiceInternal(google::protobuf::Service* service,
                               bool is_builtin_service,
                               const ServiceOptions& svc_opt) {
    if (NULL == service) {
        LOG(ERROR) << "Parameter[service] is NULL!";
        return -1;
    }
    const google::protobuf::ServiceDescriptor* sd = service->GetDescriptor();
    if (sd->method_count() == 0) {
        LOG(ERROR) << "service=" << sd->full_name()
                   << " does not have any method.";
        return -1;
    }

    if (InitializeOnce() != 0) {
        LOG(ERROR) << "Fail to initialize Server[" << version() << ']';
        return -1;
    }
    if (status() != READY) {
        LOG(ERROR) << "Can't add service=" << sd->full_name() << " to Server["
                   << version() << "] which is " << status_str(status());
        return -1;
    }

    if (_fullname_service_map.seek(sd->full_name()) != NULL) {
        LOG(ERROR) << "service=" << sd->full_name() << " already exists";
        return -1;
    }
    ServiceProperty* old_ss = _service_map.seek(sd->name());
    if (old_ss != NULL) {
        // names conflict.
        LOG(ERROR) << "Conflict service name between "
                   << sd->full_name() << " and "
                   << old_ss->service_name();
        return -1;
    }

    // defined `option (idl_support) = true' or not.
    const bool is_idl_support = sd->file()->options().GetExtension(idl_support);

    Tabbed* tabbed = dynamic_cast<Tabbed*>(service);
    for (int i = 0; i < sd->method_count(); ++i) {
        const google::protobuf::MethodDescriptor* md = sd->method(i);
        MethodProperty mp;
        mp.is_builtin_service = is_builtin_service;
        mp.own_method_status = true;
        mp.params.is_tabbed = !!tabbed;
        mp.params.allow_default_url = svc_opt.allow_default_url;
        mp.params.allow_http_body_to_pb = svc_opt.allow_http_body_to_pb;
        mp.params.pb_bytes_to_base64 = svc_opt.pb_bytes_to_base64;
        mp.service = service;
        mp.method = md;
        mp.status = new MethodStatus;
        _method_map[md->full_name()] = mp;
        if (is_idl_support && sd->name() != sd->full_name()/*has ns*/) {
            MethodProperty mp2 = mp;
            mp2.own_method_status = false;
            // have to map service_name + method_name as well because ubrpc
            // does not send the namespace before service_name.
            std::string full_name_wo_ns;
            full_name_wo_ns.reserve(sd->name().size() + 1 + md->name().size());
            full_name_wo_ns.append(sd->name());
            full_name_wo_ns.push_back('.');
            full_name_wo_ns.append(md->name());
            if (_method_map.seek(full_name_wo_ns) == NULL) {
                _method_map[full_name_wo_ns] = mp2;
            } else {
                LOG(ERROR) << '`' << full_name_wo_ns << "' already exists";
                RemoveMethodsOf(service);
                return -1;
            }
        }
    }

    const ServiceProperty ss = {
        is_builtin_service, svc_opt.ownership, service, NULL };
    _fullname_service_map[sd->full_name()] = ss;
    _service_map[sd->name()] = ss;
    if (is_builtin_service) {
        ++_builtin_service_count;
    } else {
        if (_first_service == NULL) {
            _first_service = service;
        }
    }

    butil::StringPiece restful_mappings = svc_opt.restful_mappings;
    restful_mappings.trim_spaces();
    if (!restful_mappings.empty()) {
        // Parse the mappings.
        std::vector<RestfulMapping> mappings;
        if (!ParseRestfulMappings(restful_mappings, &mappings)) {
            LOG(ERROR) << "Fail to parse mappings `" << restful_mappings << '\'';
            RemoveService(service);
            return -1;
        }
        if (mappings.empty()) {
            // we already trimmed at the beginning, this is impossible.
            LOG(ERROR) << "Impossible: Nothing in restful_mappings";
            RemoveService(service);
            return -1;
        }

        // Due the flexibility of URL matching, it's almost impossible to
        // dispatch all kinds of URL to different methods *efficiently* just
        // inside the HTTP protocol impl. We would like to match most-
        // frequently-used URLs(/Service/Method) fastly and match more complex
        // URLs inside separate functions.
        // The trick is adding some entries inside the service maps without
        // real services, mapping from the first component in the URL to a
        // RestfulMap which does the complex matchings. For example:
        //   "/v1/send => SendFn, /v1/recv => RecvFn, /v2/check => CheckFn"
        // We'll create 2 entries in service maps (_fullname_service_map and
        // _service_map) mapping from "v1" and "v2" to 2 different RestfulMap
        // respectively. When the URL is accessed, we extract the first
        // component, find the RestfulMap and do url matchings. Regular url
        // handling is not affected.
        for (size_t i = 0; i < mappings.size(); ++i) {
            const std::string full_method_name =
                sd->full_name() + "." + mappings[i].method_name;
            MethodProperty* mp = _method_map.seek(full_method_name);
            if (mp == NULL) {
                LOG(ERROR) << "Unknown method=`" << full_method_name << '\'';
                RemoveService(service);
                return -1;
            }

            const std::string& svc_name = mappings[i].path.service_name;
            if (svc_name.empty()) {
                if (_global_restful_map == NULL) {
                    _global_restful_map = new RestfulMap("");
                }
                MethodProperty::OpaqueParams params;
                params.is_tabbed = !!tabbed;
                params.allow_default_url = svc_opt.allow_default_url;
                params.allow_http_body_to_pb = svc_opt.allow_http_body_to_pb;
                params.pb_bytes_to_base64 = svc_opt.pb_bytes_to_base64;
                if (!_global_restful_map->AddMethod(
                        mappings[i].path, service, params,
                        mappings[i].method_name, mp->status)) {
                    LOG(ERROR) << "Fail to map `" << mappings[i].path
                               << "' to `" << full_method_name << '\'';
                    RemoveService(service);
                    return -1;
                }
                if (mp->http_url == NULL) {
                    mp->http_url = new std::string(mappings[i].path.to_string());
                } else {
                    if (!mp->http_url->empty()) {
                        mp->http_url->append(" @");
                    }
                    mp->http_url->append(mappings[i].path.to_string());
                }
                continue;
            }
            ServiceProperty* sp = _fullname_service_map.seek(svc_name);
            ServiceProperty* sp2 = _service_map.seek(svc_name);
            if (((!!sp) != (!!sp2)) ||
                (sp != NULL && sp->service != sp2->service)) {
                LOG(ERROR) << "Impossible: _fullname_service and _service_map are"
                        " inconsistent before inserting " << svc_name;
                RemoveService(service);
                return -1;
            }
            RestfulMap* m = NULL;
            if (sp == NULL) {
                m = new RestfulMap(mappings[i].path.service_name);
            } else {
                m = sp->restful_map;
            }
            MethodProperty::OpaqueParams params;
            params.is_tabbed = !!tabbed;
            params.allow_default_url = svc_opt.allow_default_url;
            params.allow_http_body_to_pb = svc_opt.allow_http_body_to_pb;
            params.pb_bytes_to_base64 = svc_opt.pb_bytes_to_base64;
            if (!m->AddMethod(mappings[i].path, service, params,
                              mappings[i].method_name, mp->status)) {
                LOG(ERROR) << "Fail to map `" << mappings[i].path << "' to `"
                           << sd->full_name() << '.' << mappings[i].method_name
                           << '\'';
                if (sp == NULL) {
                    delete m;
                }
                RemoveService(service);
                return -1;
            }
            if (mp->http_url == NULL) {
                mp->http_url = new std::string(mappings[i].path.to_string());
            } else {
                if (!mp->http_url->empty()) {
                    mp->http_url->append(" @");
                }
                mp->http_url->append(mappings[i].path.to_string());
            }
            if (sp == NULL) {
                ServiceProperty ss =
                    { false, SERVER_DOESNT_OWN_SERVICE, NULL, m };
                _fullname_service_map[svc_name] = ss;
                _service_map[svc_name] = ss;
                ++_virtual_service_count;
            }
        }
    }

    if (tabbed) {
        if (_tab_info_list == NULL) {
            _tab_info_list = new TabInfoList;
        }
        const size_t last_size = _tab_info_list->size();
        tabbed->GetTabInfo(_tab_info_list);
        const size_t cur_size = _tab_info_list->size();
        for (size_t i = last_size; i != cur_size; ++i) {
            const TabInfo& info = (*_tab_info_list)[i];
            if (!info.valid()) {
                LOG(ERROR) << "Invalid TabInfo: path=" << info.path
                           << " tab_name=" << info.tab_name;
                _tab_info_list->resize(last_size);
                RemoveService(service);
                return -1;
            }
        }
    }
    return 0;
}

ServiceOptions::ServiceOptions()
    : ownership(SERVER_DOESNT_OWN_SERVICE)
    , allow_default_url(false)
    , allow_http_body_to_pb(true)
#ifdef BAIDU_INTERNAL
    , pb_bytes_to_base64(false)
#else
    , pb_bytes_to_base64(true)
#endif
    {}

int Server::AddService(google::protobuf::Service* service,
                       ServiceOwnership ownership) {
    ServiceOptions options;
    options.ownership = ownership;
    return AddServiceInternal(service, false, options);
}

int Server::AddService(google::protobuf::Service* service,
                       ServiceOwnership ownership,
                       const butil::StringPiece& restful_mappings,
                       bool allow_default_url) {
    ServiceOptions options;
    options.ownership = ownership;
    // TODO: This is weird
    options.restful_mappings = restful_mappings.as_string();
    options.allow_default_url = allow_default_url;
    return AddServiceInternal(service, false, options);
}

int Server::AddService(google::protobuf::Service* service,
                       const ServiceOptions& options) {
    return AddServiceInternal(service, false, options);
}

int Server::AddBuiltinService(google::protobuf::Service* service) {
    ServiceOptions options;
    options.ownership = SERVER_OWNS_SERVICE;
    return AddServiceInternal(service, true, options);
}

void Server::RemoveMethodsOf(google::protobuf::Service* service) {
    const google::protobuf::ServiceDescriptor* sd = service->GetDescriptor();
    const bool is_idl_support = sd->file()->options().GetExtension(idl_support);
    std::string full_name_wo_ns;
    for (int i = 0; i < sd->method_count(); ++i) {
        const google::protobuf::MethodDescriptor* md = sd->method(i);
        MethodProperty* mp = _method_map.seek(md->full_name());
        if (is_idl_support) {
            full_name_wo_ns.clear();
            full_name_wo_ns.reserve(sd->name().size() + 1 + md->name().size());
            full_name_wo_ns.append(sd->name());
            full_name_wo_ns.push_back('.');
            full_name_wo_ns.append(md->name());
            _method_map.erase(full_name_wo_ns);
        }
        if (mp == NULL) {
            LOG(ERROR) << "Fail to find method=" << md->full_name();
            continue;
        }
        if (mp->http_url) {
            butil::StringSplitter at_sp(mp->http_url->c_str(), '@');
            for (; at_sp; ++at_sp) {
                butil::StringPiece path(at_sp.field(), at_sp.length());
                path.trim_spaces();
                butil::StringSplitter slash_sp(
                    path.data(), path.data() + path.size(), '/');
                if (slash_sp == NULL) {
                    LOG(ERROR) << "Invalid http_url=" << *mp->http_url;
                    break;
                }
                butil::StringPiece v_svc_name(slash_sp.field(), slash_sp.length());
                const ServiceProperty* vsp = FindServicePropertyByName(v_svc_name);
                if (vsp == NULL) {
                    if (_global_restful_map) {
                        std::string path_str;
                        path.CopyToString(&path_str);
                        if (_global_restful_map->RemoveByPathString(path_str)) {
                            continue;
                        }
                    }
                    LOG(ERROR) << "Impossible: service=" << v_svc_name
                               << " for restful_map does not exist";
                    break;
                }
                std::string path_str;
                path.CopyToString(&path_str);
                if (!vsp->restful_map->RemoveByPathString(path_str)) {
                    LOG(ERROR) << "Fail to find path=" << path
                               << " in restful_map of service=" << v_svc_name;
                }
            }
            delete mp->http_url;
        }

        if (mp->own_method_status) {
            delete mp->status;
        }
        _method_map.erase(md->full_name());
    }
}

int Server::RemoveService(google::protobuf::Service* service) {
    if (NULL == service) {
        LOG(ERROR) << "Parameter[service] is NULL";
        return -1;
    }
    if (status() != READY) {
        LOG(ERROR) << "Can't remove service="
                   << service->GetDescriptor()->full_name() << " from Server["
                   << version() << "] which is " << status_str(status());
        return -1;
    }

    const google::protobuf::ServiceDescriptor* sd = service->GetDescriptor();
    ServiceProperty* ss = _fullname_service_map.seek(sd->full_name());
    if (ss == NULL) {
        RPC_VLOG << "Fail to find service=" << sd->full_name().c_str();
        return -1;
    }
    RemoveMethodsOf(service);
    if (ss->ownership == SERVER_OWNS_SERVICE) {
        delete ss->service;
    }
    const bool is_builtin_service = ss->is_builtin_service;
    _fullname_service_map.erase(sd->full_name());
    _service_map.erase(sd->name());

    // Note: ss is invalidated.
    if (is_builtin_service) {
        --_builtin_service_count;
    } else {
        if (_first_service == service) {
            _first_service = NULL;
        }
    }
    return 0;
}

void Server::ClearServices() {
    if (status() != READY) {
        LOG_IF(ERROR, status() != UNINITIALIZED)
            << "Can't clear services from Server[" << version()
            << "] which is " << status_str(status());
        return;
    }
    for (ServiceMap::const_iterator it = _fullname_service_map.begin();
         it != _fullname_service_map.end(); ++it) {
        if (it->second.ownership == SERVER_OWNS_SERVICE) {
            delete it->second.service;
        }
        delete it->second.restful_map;
    }
    for (MethodMap::const_iterator it = _method_map.begin();
         it != _method_map.end(); ++it) {
        if (it->second.own_method_status) {
            delete it->second.status;
        }
        delete it->second.http_url;
    }
    _fullname_service_map.clear();
    _service_map.clear();
    _method_map.clear();
    _builtin_service_count = 0;
    _virtual_service_count = 0;
    _first_service = NULL;
}

google::protobuf::Service* Server::FindServiceByFullName(
    const butil::StringPiece& full_name) const {
    ServiceProperty* ss = _fullname_service_map.seek(full_name);
    return (ss ? ss->service : NULL);
}

google::protobuf::Service* Server::FindServiceByName(
    const butil::StringPiece& name) const {
    ServiceProperty* ss = _service_map.seek(name);
    return (ss ? ss->service : NULL);
}

void Server::GetStat(ServerStatistics* stat) const {
    stat->connection_count = 0;
    if (_am) {
        stat->connection_count += _am->ConnectionCount();
    }
    if (_internal_am) {
        stat->connection_count += _internal_am->ConnectionCount();
    }
    stat->user_service_count = service_count();
    stat->builtin_service_count = builtin_service_count();
}

void Server::ListServices(std::vector<google::protobuf::Service*> *services) {
    if (!services) {
        return;
    }
    services->clear();
    services->reserve(service_count());
    for (ServiceMap::const_iterator it = _fullname_service_map.begin();
         it != _fullname_service_map.end(); ++it) {
        if (it->second.is_user_service()) {
            services->push_back(it->second.service);
        }
    }
}

void Server::GenerateVersionIfNeeded() {
    if (!_version.empty()) {
        return;
    }
    int extra_count = !!_options.nshead_service + !!_options.rtmp_service +
        !!_options.thrift_service + !!_options.redis_service;
    _version.reserve((extra_count + service_count()) * 20);
    for (ServiceMap::const_iterator it = _fullname_service_map.begin();
         it != _fullname_service_map.end(); ++it) {
        if (it->second.is_user_service()) {
            if (!_version.empty()) {
                _version.push_back('+');
            }
            _version.append(butil::class_name_str(*it->second.service));
        }
    }
    if (_options.nshead_service) {
        if (!_version.empty()) {
            _version.push_back('+');
        }
        _version.append(butil::class_name_str(*_options.nshead_service));
    }

#ifdef ENABLE_THRIFT_FRAMED_PROTOCOL
    if (_options.thrift_service) {
        if (!_version.empty()) {
            _version.push_back('+');
        }
        _version.append(butil::class_name_str(*_options.thrift_service));
    }
#endif

    if (_options.rtmp_service) {
        if (!_version.empty()) {
            _version.push_back('+');
        }
        _version.append(butil::class_name_str(*_options.rtmp_service));
    }

    if (_options.redis_service) {
        if (!_version.empty()) {
            _version.push_back('+');
        }
        _version.append(butil::class_name_str(*_options.redis_service));
    }
}

static std::string ExpandPath(const std::string &path) {
    if (path.empty()) {
        return std::string();
    }
    std::string ret;
    wordexp_t p;
    wordexp(path.c_str(), &p, 0);
    CHECK_EQ(p.we_wordc, 1u);
    if (p.we_wordc == 1) {
        ret = p.we_wordv[0];
    }
    wordfree(&p);
    return ret;
}

void Server::PutPidFileIfNeeded() {
    _options.pid_file = ExpandPath(_options.pid_file);
    if (_options.pid_file.empty()) {
        return;
    }
    RPC_VLOG << "pid_file = " << _options.pid_file;
    // Recursively create directory
    for (size_t pos = _options.pid_file.find('/'); pos != std::string::npos;
            pos = _options.pid_file.find('/', pos + 1)) {
        std::string dir_name =_options.pid_file.substr(0, pos + 1);
        int rc = mkdir(dir_name.c_str(),
                       S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP);
        if (rc != 0 && errno != EEXIST
#if defined(OS_MACOSX)
        && errno != EISDIR
#endif
        ) {
            PLOG(WARNING) << "Fail to create " << dir_name;
            _options.pid_file.clear();
            return;
        }
    }
    int fd = open(_options.pid_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        LOG(WARNING) << "Fail to open " << _options.pid_file;
        _options.pid_file.clear();
        return;
    }
    char buf[32];
    int nw = snprintf(buf, sizeof(buf), "%lld", (long long)getpid());
    CHECK_EQ(nw, write(fd, buf, nw));
    CHECK_EQ(0, close(fd));
}

void Server::RunUntilAskedToQuit() {
    while (!IsAskedToQuit()) {
        bthread_usleep(1000000L);
    }
    Stop(0/*not used now*/);
    Join();
}

void* thread_local_data() {
    const Server::ThreadLocalOptions* tl_options =
        static_cast<const Server::ThreadLocalOptions*>(bthread_get_assigned_data());
    if (tl_options == NULL) { // not in server threads.
        return NULL;
    }
    if (BAIDU_UNLIKELY(tl_options->thread_local_data_factory == NULL)) {
        CHECK(false) << "The protocol impl. may not set tls correctly";
        return NULL;
    }
    void* data = bthread_getspecific(tl_options->tls_key);
    if (data == NULL) {
        data = tl_options->thread_local_data_factory->CreateData();
        if (data != NULL) {
            CHECK_EQ(0, bthread_setspecific(tl_options->tls_key, data));
        }
    }
    return data;
}

inline void tabs_li(std::ostream& os, const char* link,
                    const char* tab_name, const char* current_tab_name) {
    os << "<li id='" << link << '\'';
    if (strcmp(current_tab_name, tab_name) == 0) {
        os << " class='current'";
    }
    os << '>' << tab_name << "</li>\n";
}

void Server::PrintTabsBody(std::ostream& os,
                           const char* current_tab_name) const {
    os << "<ul class='tabs-menu'>\n";
    if (_tab_info_list) {
        for (size_t i = 0; i < _tab_info_list->size(); ++i) {
            const TabInfo& info = (*_tab_info_list)[i];
            tabs_li(os, info.path.c_str(), info.tab_name.c_str(),
                    current_tab_name);
        }
    }
    os << "<li id='https://github.com/brpc/brpc/blob/master/docs/cn/builtin_service.md' "
        "class='help'>?</li>\n</ul>\n"
        "<div style='height:40px;'></div>";  // placeholder
}

static pthread_mutex_t g_dummy_server_mutex = PTHREAD_MUTEX_INITIALIZER;
static Server* g_dummy_server = NULL;

int StartDummyServerAt(int port, ProfilerLinker) {
    if (port < 0 || port >= 65536) {
        LOG(ERROR) << "Invalid port=" << port;
        return -1;
    }
    if (g_dummy_server == NULL) {  // (1)
        BAIDU_SCOPED_LOCK(g_dummy_server_mutex);
        if (g_dummy_server == NULL) {
            Server* dummy_server = new Server;
            dummy_server->set_version(butil::string_printf(
                        "DummyServerOf(%s)", GetProgramName()));
            ServerOptions options;
            options.num_threads = 0;
            if (dummy_server->Start(port, &options) != 0) {
                LOG(ERROR) << "Fail to start dummy_server at port=" << port;
                return -1;
            }
            // (1) may see uninitialized dummy_server due to relaxed memory
            // fencing, but we only expose a function to test existence
            // of g_dummy_server, everything should be fine.
            g_dummy_server = dummy_server;
            return 0;
        }
    }
    LOG(ERROR) << "Already have dummy_server at port="
               << g_dummy_server->listen_address().port;
    return -1;
}

bool IsDummyServerRunning() {
    return g_dummy_server != NULL;
}

const Server::MethodProperty*
Server::FindMethodPropertyByFullName(const butil::StringPiece&fullname) const  {
    return _method_map.seek(fullname);
}

const Server::MethodProperty*
Server::FindMethodPropertyByFullName(const butil::StringPiece& service_name/*full*/,
                                     const butil::StringPiece& method_name) const {
    const size_t fullname_len = service_name.size() + 1 + method_name.size();
    if (fullname_len <= 256) {
        // Avoid allocation in most cases.
        char buf[fullname_len];
        memcpy(buf, service_name.data(), service_name.size());
        buf[service_name.size()] = '.';
        memcpy(buf + service_name.size() + 1, method_name.data(), method_name.size());
        return FindMethodPropertyByFullName(butil::StringPiece(buf, fullname_len));
    } else {
        std::string full_method_name;
        full_method_name.reserve(fullname_len);
        full_method_name.append(service_name.data(), service_name.size());
        full_method_name.push_back('.');
        full_method_name.append(method_name.data(), method_name.size());
        return FindMethodPropertyByFullName(full_method_name);
    }
}

const Server::MethodProperty*
Server::FindMethodPropertyByNameAndIndex(const butil::StringPiece& service_name,
                                         int method_index) const {
    const Server::ServiceProperty* sp = FindServicePropertyByName(service_name);
    if (NULL == sp) {
        return NULL;
    }
    const google::protobuf::ServiceDescriptor* sd = sp->service->GetDescriptor();
    if (method_index < 0 || method_index >= sd->method_count()) {
        return NULL;
    }
    const google::protobuf::MethodDescriptor* method = sd->method(method_index);
    return FindMethodPropertyByFullName(method->full_name());
}

const Server::ServiceProperty*
Server::FindServicePropertyByFullName(const butil::StringPiece& fullname) const {
    return _fullname_service_map.seek(fullname);
}

const Server::ServiceProperty*
Server::FindServicePropertyByName(const butil::StringPiece& name) const {
    return _service_map.seek(name);
}

int Server::AddCertificate(const CertInfo& cert) {
    if (!_options.has_ssl_options()) {
        LOG(ERROR) << "ServerOptions.ssl_options is not configured yet";
        return -1;
    }
    std::string cert_key(cert.certificate);
    cert_key.append(cert.private_key);
    if (_ssl_ctx_map.seek(cert_key) != NULL) {
        LOG(WARNING) << cert << " already exists";
        return 0;
    }

    SSLContext ssl_ctx;
    ssl_ctx.filters = cert.sni_filters;
    ssl_ctx.ctx = std::make_shared<SocketSSLContext>();
    SSL_CTX* raw_ctx = CreateServerSSLContext(cert.certificate, cert.private_key,
                                              _options.ssl_options(), &ssl_ctx.filters);
    if (raw_ctx == NULL) {
        return -1;
    }
    ssl_ctx.ctx->raw_ctx = raw_ctx;

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
    SSL_CTX_set_tlsext_servername_callback(ssl_ctx.ctx->raw_ctx, SSLSwitchCTXByHostname);
    SSL_CTX_set_tlsext_servername_arg(ssl_ctx.ctx->raw_ctx, this);
#endif

    if (!_reload_cert_maps.Modify(AddCertMapping, ssl_ctx)) {
        LOG(ERROR) << "Fail to add mappings into _reload_cert_maps";
        return -1;
    }
    _ssl_ctx_map[cert_key] = ssl_ctx;
    return 0;
}

bool Server::AddCertMapping(CertMaps& bg, const SSLContext& ssl_ctx) {
    if (!bg.cert_map.initialized()
        && bg.cert_map.init(INITIAL_CERT_MAP) != 0) {
        LOG(ERROR) << "Fail to init _cert_map";
        return false;
    }
    if (!bg.wildcard_cert_map.initialized()
        && bg.wildcard_cert_map.init(INITIAL_CERT_MAP) != 0) {
        LOG(ERROR) << "Fail to init _wildcard_cert_map";
        return false;
    }

    for (size_t i = 0; i < ssl_ctx.filters.size(); ++i) {
        const char* hostname = ssl_ctx.filters[i].c_str();
        CertMap* cmap = NULL;
        if (strncmp(hostname, "*.", 2) == 0) {
            cmap = &(bg.wildcard_cert_map);
            hostname += 2;
        } else {
            cmap = &(bg.cert_map);
        }
        if (cmap->seek(hostname) == NULL) {
            cmap->insert(hostname, ssl_ctx.ctx);
        } else {
            LOG(WARNING) << "Duplicate certificate hostname=" << hostname;
        }
    }
    return true;
}

int Server::RemoveCertificate(const CertInfo& cert) {
    if (!_options.has_ssl_options()) {
        LOG(ERROR) << "ServerOptions.ssl_options is not configured yet";
        return -1;
    }
    std::string cert_key(cert.certificate);
    cert_key.append(cert.private_key);
    SSLContext* ssl_ctx = _ssl_ctx_map.seek(cert_key);
    if (ssl_ctx == NULL) {
        LOG(WARNING) << cert << " doesn't exist";
        return 0;
    }
    if (ssl_ctx->ctx == _default_ssl_ctx) {
        LOG(WARNING) << "Cannot remove: " << cert
                     << " since it's the default certificate";
        return -1;
    }

    if (!_reload_cert_maps.Modify(RemoveCertMapping, *ssl_ctx)) {
        LOG(ERROR) << "Fail to remove mappings from _reload_cert_maps";
        return -1;
    }

    _ssl_ctx_map.erase(cert_key);
    return 0;
}

bool Server::RemoveCertMapping(CertMaps& bg, const SSLContext& ssl_ctx) {
    for (size_t i = 0; i < ssl_ctx.filters.size(); ++i) {
        const char* hostname = ssl_ctx.filters[i].c_str();
        CertMap* cmap = NULL;
        if (strncmp(hostname, "*.", 2) == 0) {
            cmap = &(bg.wildcard_cert_map);
            hostname += 2;
        } else {
            cmap = &(bg.cert_map);
        }
        std::shared_ptr<SocketSSLContext>* ctx = cmap->seek(hostname);
        if (ctx != NULL && *ctx == ssl_ctx.ctx) {
            cmap->erase(hostname);
        }
    }
    return true;
}

int Server::ResetCertificates(const std::vector<CertInfo>& certs) {
    if (!_options.has_ssl_options()) {
        LOG(ERROR) << "ServerOptions.ssl_options is not configured yet";
        return -1;
    }

    SSLContextMap tmp_map;
    if (tmp_map.init(INITIAL_CERT_MAP) != 0) {
        LOG(ERROR) << "Fail to initialize tmp_map";
        return -1;
    }

    // Add default certficiate into tmp_map first since it can't be reloaded
    std::string default_cert_key =
        _options.ssl_options().default_cert.certificate
        + _options.ssl_options().default_cert.private_key;
    tmp_map[default_cert_key] = _ssl_ctx_map[default_cert_key];

    for (size_t i = 0; i < certs.size(); ++i) {
        std::string cert_key(certs[i].certificate);
        cert_key.append(certs[i].private_key);
        if (tmp_map.seek(cert_key) != NULL) {
            LOG(WARNING) << certs[i] << " already exists";
            return 0;
        }

        SSLContext ssl_ctx;
        ssl_ctx.filters = certs[i].sni_filters;
        ssl_ctx.ctx = std::make_shared<SocketSSLContext>();
        ssl_ctx.ctx->raw_ctx = CreateServerSSLContext(
            certs[i].certificate, certs[i].private_key,
            _options.ssl_options(), &ssl_ctx.filters);
        if (ssl_ctx.ctx->raw_ctx == NULL) {
            return -1;
        }

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
        SSL_CTX_set_tlsext_servername_callback(ssl_ctx.ctx->raw_ctx, SSLSwitchCTXByHostname);
        SSL_CTX_set_tlsext_servername_arg(ssl_ctx.ctx->raw_ctx, this);
#endif
        tmp_map[cert_key] = ssl_ctx;
    }

    if (!_reload_cert_maps.Modify(ResetCertMappings, tmp_map)) {
        return -1;
    }

    _ssl_ctx_map.swap(tmp_map);
    return 0;
}

bool Server::ResetCertMappings(CertMaps& bg, const SSLContextMap& ctx_map) {
    if (!bg.cert_map.initialized()
        && bg.cert_map.init(INITIAL_CERT_MAP) != 0) {
        LOG(ERROR) << "Fail to init _cert_map";
        return false;
    }
    if (!bg.wildcard_cert_map.initialized()
        && bg.wildcard_cert_map.init(INITIAL_CERT_MAP) != 0) {
        LOG(ERROR) << "Fail to init _wildcard_cert_map";
        return false;
    }
    bg.cert_map.clear();
    bg.wildcard_cert_map.clear();

    for (SSLContextMap::const_iterator it =
                 ctx_map.begin(); it != ctx_map.end(); ++it) {
        const SSLContext& ssl_ctx = it->second;
        for (size_t i = 0; i < ssl_ctx.filters.size(); ++i) {
            const char* hostname = ssl_ctx.filters[i].c_str();
            CertMap* cmap = NULL;
            if (strncmp(hostname, "*.", 2) == 0) {
                cmap = &(bg.wildcard_cert_map);
                hostname += 2;
            } else {
                cmap = &(bg.cert_map);
            }
            if (cmap->seek(hostname) == NULL) {
                cmap->insert(hostname, ssl_ctx.ctx);
            } else {
                LOG(WARNING) << "Duplicate certificate hostname=" << hostname;
            }
        }
    }
    return true;
}

void Server::FreeSSLContexts() {
    _ssl_ctx_map.clear();
    _reload_cert_maps.Modify(ClearCertMapping);
    _default_ssl_ctx = NULL;
}

bool Server::ClearCertMapping(CertMaps& bg) {
    bg.cert_map.clear();
    bg.wildcard_cert_map.clear();
    return true;
}

int Server::ResetMaxConcurrency(int max_concurrency) {
    if (!IsRunning()) {
        LOG(WARNING) << "ResetMaxConcurrency is only allowd for a Running Server";
        return -1;
    }
    // Assume that modifying int32 is atomical in X86
    _options.max_concurrency = max_concurrency;
    return 0;
}

AdaptiveMaxConcurrency& Server::MaxConcurrencyOf(MethodProperty* mp) {
    if (IsRunning()) {
        LOG(WARNING) << "MaxConcurrencyOf is only allowd before Server started";
        return g_default_max_concurrency_of_method;
    }
    if (mp->status == NULL) {
        LOG(ERROR) << "method=" << mp->method->full_name()
                   << " does not support max_concurrency";
        _failed_to_set_max_concurrency_of_method = true;
        return g_default_max_concurrency_of_method;
    }
    return mp->max_concurrency;
}

int Server::MaxConcurrencyOf(const MethodProperty* mp) const {
    if (IsRunning()) {
        LOG(WARNING) << "MaxConcurrencyOf is only allowd before Server started";
        return g_default_max_concurrency_of_method;
    }
    if (mp == NULL || mp->status == NULL) {
        return 0;
    }
    return mp->max_concurrency;
}

AdaptiveMaxConcurrency& Server::MaxConcurrencyOf(const butil::StringPiece& full_method_name) {
    MethodProperty* mp = _method_map.seek(full_method_name);
    if (mp == NULL) {
        LOG(ERROR) << "Fail to find method=" << full_method_name;
        _failed_to_set_max_concurrency_of_method = true;
        return g_default_max_concurrency_of_method;
    }
    return MaxConcurrencyOf(mp);
}

int Server::MaxConcurrencyOf(const butil::StringPiece& full_method_name) const {
    return MaxConcurrencyOf(_method_map.seek(full_method_name));
}

AdaptiveMaxConcurrency& Server::MaxConcurrencyOf(const butil::StringPiece& full_service_name,
                              const butil::StringPiece& method_name) {
    MethodProperty* mp = const_cast<MethodProperty*>(
        FindMethodPropertyByFullName(full_service_name, method_name));
    if (mp == NULL) {
        LOG(ERROR) << "Fail to find method=" << full_service_name
                   << '/' << method_name;
        _failed_to_set_max_concurrency_of_method = true;
        return g_default_max_concurrency_of_method;
    }
    return MaxConcurrencyOf(mp);
}

int Server::MaxConcurrencyOf(const butil::StringPiece& full_service_name,
                             const butil::StringPiece& method_name) const {
    return MaxConcurrencyOf(FindMethodPropertyByFullName(
                                full_service_name, method_name));
}

AdaptiveMaxConcurrency& Server::MaxConcurrencyOf(google::protobuf::Service* service,
                              const butil::StringPiece& method_name) {
    return MaxConcurrencyOf(service->GetDescriptor()->full_name(), method_name);
}

int Server::MaxConcurrencyOf(google::protobuf::Service* service,
                             const butil::StringPiece& method_name) const {
    return MaxConcurrencyOf(service->GetDescriptor()->full_name(), method_name);
}

#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
int Server::SSLSwitchCTXByHostname(struct ssl_st* ssl,
                                   int* al, Server* server) {
    (void)al;
    const char* hostname = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
    bool strict_sni = server->_options.ssl_options().strict_sni;
    if (hostname == NULL) {
        return strict_sni ? SSL_TLSEXT_ERR_ALERT_FATAL : SSL_TLSEXT_ERR_NOACK;
    }

    butil::DoublyBufferedData<CertMaps>::ScopedPtr s;
    if (server->_reload_cert_maps.Read(&s) != 0) {
        return SSL_TLSEXT_ERR_ALERT_FATAL;
    }

    std::shared_ptr<SocketSSLContext>* pctx = s->cert_map.seek(hostname);
    if (pctx == NULL) {
        const char* dot = hostname;
        for (; *dot != '\0'; ++dot) {
            if (*dot == '.') {
                ++dot;
                break;
            }
        }
        if (*dot != '\0') {
            pctx = s->wildcard_cert_map.seek(dot);
        }
    }
    if (pctx == NULL) {
        if (strict_sni) {
            return SSL_TLSEXT_ERR_ALERT_FATAL;
        }
        // Use default SSL_CTX which is the current one
        return SSL_TLSEXT_ERR_OK;
    }

    // Switch SSL_CTX to the one with correct hostname
    SSL_set_SSL_CTX(ssl, (*pctx)->raw_ctx);
    return SSL_TLSEXT_ERR_OK;
}
#endif // SSL_CTRL_SET_TLSEXT_HOSTNAME

}  // namespace brpc
