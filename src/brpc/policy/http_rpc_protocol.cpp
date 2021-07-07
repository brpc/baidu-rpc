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


#include <google/protobuf/descriptor.h>             // MethodDescriptor
#include <gflags/gflags.h>
#include <json2pb/pb_to_json.h>                    // ProtoMessageToJson
#include <json2pb/json_to_pb.h>                    // JsonToProtoMessage

#include "brpc/policy/http_rpc_protocol.h"
#include "butil/unique_ptr.h"                       // std::unique_ptr
#include "butil/string_splitter.h"                  // StringMultiSplitter
#include "butil/string_printf.h"
#include "butil/time.h"
#include "butil/sys_byteorder.h"
#include "brpc/compress.h"
#include "brpc/errno.pb.h"                     // ENOSERVICE, ENOMETHOD
#include "brpc/controller.h"                   // Controller
#include "brpc/server.h"                       // Server
#include "brpc/details/server_private_accessor.h"
#include "brpc/span.h"
#include "brpc/socket.h"                       // Socket
#include "brpc/http_status_code.h"             // HTTP_STATUS_*
#include "brpc/details/controller_private_accessor.h"
#include "brpc/builtin/index_service.h"        // IndexService
#include "brpc/policy/gzip_compress.h"
#include "brpc/policy/http2_rpc_protocol.h"
#include "brpc/details/usercode_backup_pool.h"
#include "brpc/grpc.h"

extern "C" {
void bthread_assign_data(void* data);
}

namespace brpc {

int is_failed_after_queries(const http_parser* parser);
int is_failed_after_http_version(const http_parser* parser);
DECLARE_bool(http_verbose);
DECLARE_int32(http_verbose_max_body_length);
// Defined in grpc.cpp
int64_t ConvertGrpcTimeoutToUS(const std::string* grpc_timeout);

namespace policy {

DEFINE_int32(http_max_error_length, 2048, "Max printed length of a http error");

DEFINE_int32(http_body_compress_threshold, 512, "Not compress http body when "
             "it's less than so many bytes.");

DEFINE_string(http_header_of_user_ip, "", "http requests sent by proxies may "
              "set the client ip in http headers. When this flag is non-empty, "
              "brpc will read ip:port from the specified header for "
              "authorization and set Controller::remote_side()");

DEFINE_bool(pb_enum_as_number, false,
            "[Not recommended] Convert enums in "
            "protobuf to json as numbers, affecting both client-side and "
            "server-side");

DEFINE_string(request_id_header, "x-request-id", "The http header to mark a session");

// Read user address from the header specified by -http_header_of_user_ip
static bool GetUserAddressFromHeaderImpl(const HttpHeader& headers,
                                         butil::EndPoint* user_addr) {
    const std::string* user_addr_str =
        headers.GetHeader(FLAGS_http_header_of_user_ip);
    if (user_addr_str == NULL) {
        return false;
    }
    if (user_addr_str->find(':') == std::string::npos) {
        if (butil::str2ip(user_addr_str->c_str(), &user_addr->ip) != 0) {
            LOG(WARNING) << "Fail to parse ip from " << *user_addr_str;
            return false;
        }
        user_addr->port = 0;
    } else {
        if (butil::str2endpoint(user_addr_str->c_str(), user_addr) != 0) {
            LOG(WARNING) << "Fail to parse ip:port from " << *user_addr_str;
            return false;
        }
    }
    return true;
}

inline bool GetUserAddressFromHeader(const HttpHeader& headers,
                                     butil::EndPoint* user_addr) {
    if (FLAGS_http_header_of_user_ip.empty()) {
        return false;
    }
    return GetUserAddressFromHeaderImpl(headers, user_addr);
}

CommonStrings::CommonStrings()
    : ACCEPT("accept")
    , DEFAULT_ACCEPT("*/*")
    , USER_AGENT("user-agent")
    , DEFAULT_USER_AGENT("brpc/1.0 curl/7.0")
    , CONTENT_TYPE("content-type")
    , CONTENT_TYPE_TEXT("text/plain")
    , CONTENT_TYPE_JSON("application/json")
    , CONTENT_TYPE_PROTO("application/proto")
    , CONTENT_TYPE_SPRING_PROTO("application/x-protobuf")
    , ERROR_CODE("x-bd-error-code")
    , AUTHORIZATION("authorization")
    , ACCEPT_ENCODING("accept-encoding")
    , CONTENT_ENCODING("content-encoding")
    , GZIP("gzip")
    , CONNECTION("connection")
    , KEEP_ALIVE("keep-alive")
    , CLOSE("close")
    , LOG_ID("log-id")
    , DEFAULT_METHOD("default_method")
    , NO_METHOD("no_method")
    , H2_SCHEME(":scheme")
    , H2_SCHEME_HTTP("http")
    , H2_SCHEME_HTTPS("https")
    , H2_AUTHORITY(":authority")
    , H2_PATH(":path")
    , H2_STATUS(":status")
    , STATUS_200("200")
    , H2_METHOD(":method")
    , METHOD_GET("GET")
    , METHOD_POST("POST")
    , TE("te")
    , TRAILERS("trailers")
    , GRPC_ENCODING("grpc-encoding")
    , GRPC_ACCEPT_ENCODING("grpc-accept-encoding")
    , GRPC_ACCEPT_ENCODING_VALUE("identity,gzip")
    , GRPC_STATUS("grpc-status")
    , GRPC_MESSAGE("grpc-message")
    , GRPC_TIMEOUT("grpc-timeout")
{}

static CommonStrings* common = NULL;
static pthread_once_t g_common_strings_once = PTHREAD_ONCE_INIT;
static void CreateCommonStrings() {
    common = new CommonStrings;
}
// Called in global.cpp
int InitCommonStrings() {
    return pthread_once(&g_common_strings_once, CreateCommonStrings);
}
static const int ALLOW_UNUSED force_creation_of_common = InitCommonStrings();
const CommonStrings* get_common_strings() { return common; }

HttpContentType ParseContentType(butil::StringPiece ct, bool* is_grpc_ct) {
    // According to http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.7
    //   media-type  = type "/" subtype *( ";" parameter )
    //   type        = token
    //   subtype     = token

    const butil::StringPiece prefix = "application/";
    if (!ct.starts_with(prefix)) {
        return HTTP_CONTENT_OTHERS;
    }
    ct.remove_prefix(prefix.size());

    if (ct.starts_with("grpc")) {
        if (ct.size() == (size_t)4 || ct[4] == ';') {
            if (is_grpc_ct) {
                *is_grpc_ct = true;
            }
            // assume that the default content type for grpc is proto.
            return HTTP_CONTENT_PROTO;
        } else if (ct[4] == '+') {
            ct.remove_prefix(5);
            if (is_grpc_ct) {
                *is_grpc_ct = true;
            }
        }
        // else don't change ct. Note that "grpcfoo" is a valid but non-grpc
        // content-type in the sense of format.
    }

    HttpContentType type = HTTP_CONTENT_OTHERS;
    if (ct.starts_with("json")) {
        type = HTTP_CONTENT_JSON;
        ct.remove_prefix(4);
    } else if (ct.starts_with("proto")) {
        type = HTTP_CONTENT_PROTO;
        ct.remove_prefix(5);
    } else if (ct.starts_with("x-protobuf")) {
        type = HTTP_CONTENT_PROTO;
        ct.remove_prefix(10);
    } else {
        return HTTP_CONTENT_OTHERS;
    }
    return (ct.empty() || ct.front() == ';') ? type : HTTP_CONTENT_OTHERS;
}

static void PrintMessage(const butil::IOBuf& inbuf,
                         bool request_or_response,
                         bool has_content) {
    butil::IOBuf buf1 = inbuf;
    butil::IOBuf buf2;
    char str[48];
    if (request_or_response) {
        snprintf(str, sizeof(str), "[ HTTP REQUEST @%s ]", butil::my_ip_cstr());
    } else {
        snprintf(str, sizeof(str), "[ HTTP RESPONSE @%s ]", butil::my_ip_cstr());
    }
    buf2.append(str);
    size_t last_size;
    do {
        buf2.append("\r\n> ");
        last_size = buf2.size();
    } while (buf1.cut_until(&buf2, "\r\n") == 0);
    if (buf2.size() == last_size) {
        buf2.pop_back(2);  // remove "> "
    }
    if (!has_content) {
        LOG(INFO) << '\n' << buf2 << buf1;
    } else {
        LOG(INFO) << '\n' << buf2 << butil::ToPrintableString(buf1, FLAGS_http_verbose_max_body_length);
    }
}

static void AddGrpcPrefix(butil::IOBuf* body, bool compressed) {
    char buf[5];
    buf[0] = (compressed ? 1 : 0);
    *(uint32_t*)(buf + 1) = butil::HostToNet32(body->size());
    butil::IOBuf tmp_buf;
    tmp_buf.append(buf, sizeof(buf));
    tmp_buf.append(butil::IOBuf::Movable(*body));
    body->swap(tmp_buf);
}

static bool RemoveGrpcPrefix(butil::IOBuf* body, bool* compressed) {
    if (body->empty()) {
        *compressed = false;
        return true;
    }
    const size_t sz = body->size();
    if (sz < (size_t)5) {
        return false;
    }
    char buf[5];
    body->cutn(buf, sizeof(buf));
    *compressed = buf[0];
    const size_t message_length = butil::NetToHost32(*(uint32_t*)(buf + 1));
    return (message_length + 5 == sz);
}

void ProcessHttpResponse(InputMessageBase* msg) {
    const int64_t start_parse_us = butil::cpuwide_time_us();
    DestroyingPtr<HttpContext> imsg_guard(static_cast<HttpContext*>(msg));
    Socket* socket = imsg_guard->socket();
    uint64_t cid_value;
    const bool is_http2 = imsg_guard->header().is_http2();
    if (is_http2) {
        H2StreamContext* h2_sctx = static_cast<H2StreamContext*>(msg);
        cid_value = h2_sctx->correlation_id();
    } else {
        cid_value = socket->correlation_id();
    }
    if (cid_value == 0) {
        LOG(WARNING) << "Fail to find correlation_id from " << *socket;
        return;
    }
    const bthread_id_t cid = { cid_value };
    Controller* cntl = NULL;
    const int rc = bthread_id_lock(cid, (void**)&cntl);
    if (rc != 0) {
        LOG_IF(ERROR, rc != EINVAL && rc != EPERM)
            << "Fail to lock correlation_id=" << cid << ": " << berror(rc);
        return;
    }

    ControllerPrivateAccessor accessor(cntl);

    Span* span = accessor.span();
    if (span) {
        span->set_base_real_us(msg->base_real_us());
        span->set_received_us(msg->received_us());
        // TODO: changing when imsg_guard->read_body_progressively() is true
        span->set_response_size(imsg_guard->parsed_length());
        span->set_start_parse_us(start_parse_us);
    }

    HttpHeader* res_header = &cntl->http_response();
    res_header->Swap(imsg_guard->header());
    butil::IOBuf& res_body = imsg_guard->body();
    CHECK(cntl->response_attachment().empty());
    const int saved_error = cntl->ErrorCode();

    bool is_grpc_ct = false;
    const HttpContentType content_type =
        ParseContentType(res_header->content_type(), &is_grpc_ct);
    const bool is_grpc = (is_http2 && is_grpc_ct);
    bool grpc_compressed = false;  // only valid when is_grpc is true.
    
    do {
        if (!is_http2) {
            // If header has "Connection: close", close the connection.
            const std::string* conn_cmd = res_header->GetHeader(common->CONNECTION);
            if (conn_cmd != NULL && 0 == strcasecmp(conn_cmd->c_str(), "close")) {
                // Server asked to close the connection.
                if (imsg_guard->read_body_progressively()) {
                    // Close the socket when reading completes.
                    socket->read_will_be_progressive(CONNECTION_TYPE_SHORT);
                } else {
                    socket->SetFailed();
                }
            }
        } else if (is_grpc) {
            if (!RemoveGrpcPrefix(&res_body, &grpc_compressed)) {
                cntl->SetFailed(ERESPONSE, "Invalid gRPC response");
                break;
            }
            const std::string* grpc_status = res_header->GetHeader(common->GRPC_STATUS);
            if (grpc_status) {
                // TODO: More strict parsing
                GrpcStatus status = (GrpcStatus)strtol(grpc_status->data(), NULL, 10);
                if (status != GRPC_OK) {
                    const std::string* grpc_message =
                        res_header->GetHeader(common->GRPC_MESSAGE);
                    if (grpc_message) {
                        std::string message_decoded;
                        PercentDecode(*grpc_message, &message_decoded);
                        cntl->SetFailed(GrpcStatusToErrorCode(status), "%s",
                                        message_decoded.c_str());
                    } else {
                        cntl->SetFailed(GrpcStatusToErrorCode(status), "%s",
                                        GrpcStatusToString(status));
                    }
                    break;
                }
            }
        }

        if (imsg_guard->read_body_progressively()) {
            // Set RPA if needed
            accessor.set_readable_progressive_attachment(imsg_guard.get());
            const int sc = res_header->status_code();
            if (sc < 200 || sc >= 300) {
                // Even if the body is for streaming purpose, a non-OK status
                // code indicates that the body is probably the error text
                // which is helpful for debugging.
                // content may be binary data, so the size limit is a must.
                std::string body_str;
                res_body.copy_to(
                    &body_str, std::min((int)res_body.size(),
                                        FLAGS_http_max_error_length));
                cntl->SetFailed(EHTTP, "HTTP/%d.%d %d %s: %.*s",
                                res_header->major_version(),
                                res_header->minor_version(),
                                static_cast<int>(res_header->status_code()),
                                res_header->reason_phrase(),
                                (int)body_str.size(), body_str.c_str());
            } else if (cntl->response() != NULL &&
                       cntl->response()->GetDescriptor()->field_count() != 0) {
                cntl->SetFailed(ERESPONSE, "A protobuf response can't be parsed"
                                " from progressively-read HTTP body");
            }
            break;
        }
        
        // Fail RPC if status code is an error in http sense.
        // ErrorCode of RPC is unified to EHTTP.
        const int sc = res_header->status_code();
        if (sc < 200 || sc >= 300) {
            std::string err = butil::string_printf(
                    "HTTP/%d.%d %d %s",
                    res_header->major_version(),
                    res_header->minor_version(),
                    static_cast<int>(res_header->status_code()),
                    res_header->reason_phrase());
            if (!res_body.empty()) {
                // Use content as error text if it's present. Notice that
                // content may be binary data, so the size limit is a must.
                err.append(": ");
                res_body.append_to(
                    &err, std::min((int)res_body.size(),
                                        FLAGS_http_max_error_length));
            }
            cntl->SetFailed(EHTTP, "%s", err.c_str());
            if (cntl->response() == NULL ||
                cntl->response()->GetDescriptor()->field_count() == 0) {
                // A http call. Http users may need the body(containing a html,
                // json etc) even if the http call was failed. This is different
                // from protobuf services where responses are undefined when RPC
                // was failed.
                cntl->response_attachment().swap(res_body);
            }
            break;
        }
        if (cntl->response() == NULL ||
            cntl->response()->GetDescriptor()->field_count() == 0) {
            // a http call, content is the "real response".
            cntl->response_attachment().swap(res_body);
            break;
        }

        const std::string* encoding = NULL;
        if (is_grpc) {
            if (grpc_compressed) {
                encoding = res_header->GetHeader(common->GRPC_ENCODING);
                if (encoding == NULL) {
                    cntl->SetFailed(ERESPONSE, "Fail to find header `grpc-encoding'"
                                    " in compressed gRPC response");
                    break;
                }
            }
        } else {
            encoding = res_header->GetHeader(common->CONTENT_ENCODING);
        }
        if (encoding != NULL && *encoding == common->GZIP) {
            TRACEPRINTF("Decompressing response=%lu",
                        (unsigned long)res_body.size());
            butil::IOBuf uncompressed;
            if (!policy::GzipDecompress(res_body, &uncompressed)) {
                cntl->SetFailed(ERESPONSE, "Fail to un-gzip response body");
                break;
            }
            res_body.swap(uncompressed);
        }
        if (content_type == HTTP_CONTENT_PROTO) {
            if (!ParsePbFromIOBuf(cntl->response(), res_body)) {
                cntl->SetFailed(ERESPONSE, "Fail to parse content");
                break;
            }
        } else if (content_type == HTTP_CONTENT_JSON) {
            // message body is json
            butil::IOBufAsZeroCopyInputStream wrapper(res_body);
            std::string err;
            json2pb::Json2PbOptions options;
            options.base64_to_bytes = cntl->has_pb_bytes_to_base64();
            if (!json2pb::JsonToProtoMessage(&wrapper, cntl->response(), options, &err)) {
                cntl->SetFailed(ERESPONSE, "Fail to parse content, %s", err.c_str());
                break;
            }
        } else {
            cntl->SetFailed(ERESPONSE,
                            "Unknown content-type=%s when response is not NULL",
                            res_header->content_type().c_str());
            break;
        }
    } while (0);

    // Unlocks correlation_id inside. Revert controller's
    // error code if it version check of `cid' fails
    imsg_guard.reset();
    accessor.OnResponse(cid, saved_error);
}

void SerializeHttpRequest(butil::IOBuf* /*not used*/,
                          Controller* cntl,
                          const google::protobuf::Message* pbreq) {
    HttpHeader& hreq = cntl->http_request();
    const bool is_http2 = (cntl->request_protocol() == PROTOCOL_H2);
    bool is_grpc = false;
    ControllerPrivateAccessor accessor(cntl);
    if (!accessor.protocol_param().empty() && hreq.content_type().empty()) {
        const std::string& param = accessor.protocol_param();
        if (param.find('/') == std::string::npos) {
            std::string& s = hreq.mutable_content_type();
            s.reserve(12 + param.size());
            s.append("application/");
            s.append(param);
        } else {
            hreq.set_content_type(param);
        }
    }
    if (pbreq != NULL) {
        // If request is not NULL, message body will be serialized proto/json,
        if (!pbreq->IsInitialized()) {
            return cntl->SetFailed(
                EREQUEST, "Missing required fields in request: %s",
                pbreq->InitializationErrorString().c_str());
        }
        if (!cntl->request_attachment().empty()) {
            return cntl->SetFailed(EREQUEST, "request_attachment must be empty "
                                   "when request is not NULL");
        }
        HttpContentType content_type = HTTP_CONTENT_OTHERS;
        if (hreq.content_type().empty()) {
            // Set content-type if user did not.
            // Note that http1.x defaults to json and h2 defaults to pb.
            if (is_http2) {
                content_type = HTTP_CONTENT_PROTO;
                hreq.set_content_type(common->CONTENT_TYPE_PROTO);
            } else {
                content_type = HTTP_CONTENT_JSON;
                hreq.set_content_type(common->CONTENT_TYPE_JSON);
            }
        } else {
            bool is_grpc_ct = false;
            content_type = ParseContentType(hreq.content_type(),
                                            &is_grpc_ct);
            is_grpc = (is_http2 && is_grpc_ct);
        }

        butil::IOBufAsZeroCopyOutputStream wrapper(&cntl->request_attachment());
        if (content_type == HTTP_CONTENT_PROTO) {
            // Serialize content as protobuf
            if (!pbreq->SerializeToZeroCopyStream(&wrapper)) {
                cntl->request_attachment().clear();
                return cntl->SetFailed(EREQUEST, "Fail to serialize %s",
                                       pbreq->GetTypeName().c_str());
            }
        } else if (content_type == HTTP_CONTENT_JSON) {
            std::string err;
            json2pb::Pb2JsonOptions opt;
            opt.bytes_to_base64 = cntl->has_pb_bytes_to_base64();
            opt.jsonify_empty_array = cntl->has_pb_jsonify_empty_array();
            opt.always_print_primitive_fields = cntl->has_always_print_primitive_fields();

            opt.enum_option = (FLAGS_pb_enum_as_number
                               ? json2pb::OUTPUT_ENUM_BY_NUMBER
                               : json2pb::OUTPUT_ENUM_BY_NAME);
            if (!json2pb::ProtoMessageToJson(*pbreq, &wrapper, opt, &err)) {
                cntl->request_attachment().clear();
                return cntl->SetFailed(
                    EREQUEST, "Fail to convert request to json, %s", err.c_str());
            }
        } else {
            return cntl->SetFailed(
                EREQUEST, "Cannot serialize pb request according to content_type=%s",
                hreq.content_type().c_str());
        }
    } else {
        // Use request_attachment.
        // TODO: Checking required fields of http header.
    }
    // Make RPC fail if uri() is not OK (previous SetHttpURL/operator= failed)
    if (!hreq.uri().status().ok()) {
        return cntl->SetFailed(EREQUEST, "%s",
                        hreq.uri().status().error_cstr());
    }
    bool grpc_compressed = false;
    if (cntl->request_compress_type() != COMPRESS_TYPE_NONE) {
        if (cntl->request_compress_type() != COMPRESS_TYPE_GZIP) {
            return cntl->SetFailed(EREQUEST, "http does not support %s",
                            CompressTypeToCStr(cntl->request_compress_type()));
        }
        const size_t request_size = cntl->request_attachment().size();
        if (request_size >= (size_t)FLAGS_http_body_compress_threshold) {
            TRACEPRINTF("Compressing request=%lu", (unsigned long)request_size);
            butil::IOBuf compressed;
            if (GzipCompress(cntl->request_attachment(), &compressed, NULL)) {
                cntl->request_attachment().swap(compressed);
                if (is_grpc) {
                    grpc_compressed = true;
                    hreq.SetHeader(common->GRPC_ENCODING, common->GZIP);
                } else {
                    hreq.SetHeader(common->CONTENT_ENCODING, common->GZIP);
                }
            } else {
                cntl->SetFailed("Fail to gzip the request body, skip compressing");
            }
        }
    }

    // Fill log-id if user set it.
    if (cntl->has_log_id()) {
        hreq.SetHeader(common->LOG_ID,
                       butil::string_printf("%llu", (unsigned long long)cntl->log_id()));
    }
    if (!cntl->request_id().empty()) {
        hreq.SetHeader(FLAGS_request_id_header, cntl->request_id());
    }

    if (!is_http2) {
        // HTTP before 1.1 needs to set keep-alive explicitly.
        if (hreq.before_http_1_1() &&
            cntl->connection_type() != CONNECTION_TYPE_SHORT &&
            hreq.GetHeader(common->CONNECTION) == NULL) {
            hreq.SetHeader(common->CONNECTION, common->KEEP_ALIVE);
        }
    } else {
        cntl->set_stream_creator(get_h2_global_stream_creator());
        if (is_grpc) {
            /*
            hreq.SetHeader(common->GRPC_ACCEPT_ENCODING,
                              common->GRPC_ACCEPT_ENCODING_VALUE);
            */
            // TODO: do we need this?
            hreq.SetHeader(common->TE, common->TRAILERS);
            if (cntl->timeout_ms() >= 0) {
                hreq.SetHeader(common->GRPC_TIMEOUT,
                        butil::string_printf("%" PRId64 "m", cntl->timeout_ms()));
            }
            // Append compressed and length before body
            AddGrpcPrefix(&cntl->request_attachment(), grpc_compressed);
        }
    }

    // Set url to /ServiceName/MethodName when we're about to call protobuf
    // services (indicated by non-NULL method).
    const google::protobuf::MethodDescriptor* method = cntl->method();
    if (method != NULL) {
        hreq.set_method(HTTP_METHOD_POST);
        std::string path;
        path.reserve(2 + method->service()->full_name().size()
                     + method->name().size());
        path.push_back('/');
        path.append(method->service()->full_name());
        path.push_back('/');
        path.append(method->name());
        hreq.uri().set_path(path);
    }

    Span* span = accessor.span();
    if (span) {
        hreq.SetHeader("x-bd-trace-id", butil::string_printf(
                           "%llu", (unsigned long long)span->trace_id()));
        hreq.SetHeader("x-bd-span-id", butil::string_printf(
                           "%llu", (unsigned long long)span->span_id()));
        hreq.SetHeader("x-bd-parent-span-id", butil::string_printf(
                           "%llu", (unsigned long long)span->parent_span_id()));
    }
}

void PackHttpRequest(butil::IOBuf* buf,
                     SocketMessage**,
                     uint64_t correlation_id,
                     const google::protobuf::MethodDescriptor*,
                     Controller* cntl,
                     const butil::IOBuf& /*unused*/,
                     const Authenticator* auth) {
    if (cntl->connection_type() == CONNECTION_TYPE_SINGLE) {
        return cntl->SetFailed(EREQUEST, "http can't work with CONNECTION_TYPE_SINGLE");
    }
    ControllerPrivateAccessor accessor(cntl);
    HttpHeader* header = &cntl->http_request();
    if (auth != NULL && header->GetHeader(common->AUTHORIZATION) == NULL) {
        std::string auth_data;
        if (auth->GenerateCredential(&auth_data) != 0) {
            return cntl->SetFailed(EREQUEST, "Fail to GenerateCredential");
        }
        header->SetHeader(common->AUTHORIZATION, auth_data);
    }

    // Store `correlation_id' into Socket since http server
    // may not echo back this field. But we send it anyway.
    accessor.get_sending_socket()->set_correlation_id(correlation_id);

    MakeRawHttpRequest(buf, header, cntl->remote_side(),
                       &cntl->request_attachment());
    if (FLAGS_http_verbose) {
        PrintMessage(*buf, true, true);
    }
}

inline bool SupportGzip(Controller* cntl) {
    const std::string* encodings =
        cntl->http_request().GetHeader(common->ACCEPT_ENCODING);
    return (encodings && encodings->find(common->GZIP) != std::string::npos);
}

class HttpResponseSender {
friend class HttpResponseSenderAsDone;
public:
    HttpResponseSender()
        : _method_status(NULL), _received_us(0), _h2_stream_id(-1) {}
    HttpResponseSender(Controller* cntl/*own*/)
        : _cntl(cntl), _method_status(NULL), _received_us(0), _h2_stream_id(-1) {}
    HttpResponseSender(HttpResponseSender&& s)
        : _cntl(std::move(s._cntl))
        , _req(std::move(s._req))
        , _res(std::move(s._res))
        , _method_status(std::move(s._method_status))
        , _received_us(s._received_us)
        , _h2_stream_id(s._h2_stream_id) {
    }
    ~HttpResponseSender();

    void own_request(google::protobuf::Message* req) { _req.reset(req); }
    void own_response(google::protobuf::Message* res) { _res.reset(res); }
    void set_method_status(MethodStatus* ms) { _method_status = ms; }
    void set_received_us(int64_t t) { _received_us = t; }
    void set_h2_stream_id(int id) { _h2_stream_id = id; }

private:
    std::unique_ptr<Controller, LogErrorTextAndDelete> _cntl;
    std::unique_ptr<google::protobuf::Message> _req;
    std::unique_ptr<google::protobuf::Message> _res;
    MethodStatus* _method_status;
    int64_t _received_us;
    int _h2_stream_id;
};

class HttpResponseSenderAsDone : public google::protobuf::Closure {
public:
    HttpResponseSenderAsDone(HttpResponseSender* s) : _sender(std::move(*s)) {}
    void Run() override {
        _sender._cntl->after_resp_fn(_sender._req.get(), _sender._res.get());
        delete this;
    }
private:
    HttpResponseSender _sender;
};

HttpResponseSender::~HttpResponseSender() {
    Controller* cntl = _cntl.get();
    if (cntl == NULL) {
        return;
    }
    ControllerPrivateAccessor accessor(cntl);
    Span* span = accessor.span();
    if (span) {
        span->set_start_send_us(butil::cpuwide_time_us());
    }
    ConcurrencyRemover concurrency_remover(_method_status, cntl, _received_us);
    Socket* socket = accessor.get_sending_socket();
    const google::protobuf::Message* res = _res.get();
    
    if (cntl->IsCloseConnection()) {
        socket->SetFailed();
        return;
    }

    const HttpHeader* req_header = &cntl->http_request();
    HttpHeader* res_header = &cntl->http_response();
    res_header->set_version(req_header->major_version(),
                            req_header->minor_version());

    const std::string* content_type_str = &res_header->content_type();
    if (content_type_str->empty()) {
        // Use request's content_type if response's is not set.
        content_type_str = &req_header->content_type();
        res_header->set_content_type(*content_type_str);
    }
    // Notice that HTTP1 can have a header named `grpc-encoding' as well
    // which should be treated as an user-defined header and ignored by
    // the framework.
    bool is_grpc_ct = false;
    const HttpContentType content_type = ParseContentType(*content_type_str, &is_grpc_ct);
    const bool is_http2 = req_header->is_http2();
    const bool is_grpc = (is_http2 && is_grpc_ct);

    // Convert response to json/proto if needed.
    // Notice: Not check res->IsInitialized() which should be checked in the
    // conversion function.
    if (res != NULL &&
        cntl->response_attachment().empty() &&
        // ^ user did not fill the body yet.
        res->GetDescriptor()->field_count() > 0 &&
        // ^ a pb service
        !cntl->Failed()) {
        // ^ pb response in failed RPC is undefined, no need to convert.
        
        butil::IOBufAsZeroCopyOutputStream wrapper(&cntl->response_attachment());
        if (content_type == HTTP_CONTENT_PROTO) {
            if (!res->SerializeToZeroCopyStream(&wrapper)) {
                cntl->SetFailed(ERESPONSE, "Fail to serialize %s", res->GetTypeName().c_str());
            }
        } else {
            std::string err;
            json2pb::Pb2JsonOptions opt;
            opt.bytes_to_base64 = cntl->has_pb_bytes_to_base64();
            opt.jsonify_empty_array = cntl->has_pb_jsonify_empty_array();
            opt.always_print_primitive_fields = cntl->has_always_print_primitive_fields();
            opt.enum_option = (FLAGS_pb_enum_as_number
                               ? json2pb::OUTPUT_ENUM_BY_NUMBER
                               : json2pb::OUTPUT_ENUM_BY_NAME);
            if (!json2pb::ProtoMessageToJson(*res, &wrapper, opt, &err)) {
                cntl->SetFailed(ERESPONSE, "Fail to convert response to json, %s", err.c_str());
            }
        }
    }

    // In HTTP 0.9, the server always closes the connection after sending the
    // response. The client must close its end of the connection after
    // receiving the response.
    // In HTTP 1.0, the server always closes the connection after sending the
    // response UNLESS the client sent a Connection: keep-alive request header
    // and the server sent a Connection: keep-alive response header. If no
    // such response header exists, the client must close its end of the
    // connection after receiving the response.
    // In HTTP 1.1, the server does not close the connection after sending
    // the response UNLESS the client sent a Connection: close request header,
    // or the server sent a Connection: close response header. If such a
    // response header exists, the client must close its end of the connection
    // after receiving the response.
    if (!is_http2) {
        const std::string* res_conn = res_header->GetHeader(common->CONNECTION);
        if (res_conn == NULL || strcasecmp(res_conn->c_str(), "close") != 0) {
            const std::string* req_conn =
                req_header->GetHeader(common->CONNECTION);
            if (req_header->before_http_1_1()) {
                if (req_conn != NULL &&
                    strcasecmp(req_conn->c_str(), "keep-alive") == 0) {
                    res_header->SetHeader(common->CONNECTION, common->KEEP_ALIVE);
                }
            } else {
                if (req_conn != NULL &&
                    strcasecmp(req_conn->c_str(), "close") == 0) {
                    res_header->SetHeader(common->CONNECTION, common->CLOSE);
                }
            }
        } // else user explicitly set Connection:close, clients of
        // HTTP 1.1/1.0/0.9 should all close the connection.
    } else if (is_grpc) {
        // status code is always 200 according to grpc protocol
        res_header->set_status_code(HTTP_STATUS_OK);
    }
    
    bool grpc_compressed = false;
    if (cntl->Failed()) {
        cntl->response_attachment().clear();
        if (!is_grpc) {
            // Set status-code with default value(converted from error code)
            // if user did not set it.
            if (res_header->status_code() == HTTP_STATUS_OK) {
                res_header->set_status_code(ErrorCodeToStatusCode(cntl->ErrorCode()));
            }
            // Fill ErrorCode into header
            res_header->SetHeader(common->ERROR_CODE,
                                  butil::string_printf("%d", cntl->ErrorCode()));

            // Fill body with ErrorText.
            // user may compress the output and change content-encoding. However
            // body is error-text right now, remove the header.
            res_header->RemoveHeader(common->CONTENT_ENCODING);
            res_header->set_content_type(common->CONTENT_TYPE_TEXT);
            cntl->response_attachment().append(cntl->ErrorText());
        }
    } else if (cntl->has_progressive_writer()) {
        // Transfer-Encoding is supported since HTTP/1.1
        if (res_header->major_version() < 2 && !res_header->before_http_1_1()) {
            res_header->SetHeader("Transfer-Encoding", "chunked");
        }
        if (!cntl->response_attachment().empty()) {
            LOG(ERROR) << "response_attachment(size="
                       << cntl->response_attachment().size() << ") will be"
                " ignored when CreateProgressiveAttachment() was called";
        }
        // not set_content to enable chunked mode.
    } else if (cntl->response_compress_type() == COMPRESS_TYPE_GZIP) {
        const size_t response_size = cntl->response_attachment().size();
        if (response_size >= (size_t)FLAGS_http_body_compress_threshold
            && (is_http2 || SupportGzip(cntl))) {
            TRACEPRINTF("Compressing response=%lu", (unsigned long)response_size);
            butil::IOBuf tmpbuf;
            if (GzipCompress(cntl->response_attachment(), &tmpbuf, NULL)) {
                cntl->response_attachment().swap(tmpbuf);
                if (is_grpc) {
                    grpc_compressed = true;
                    res_header->SetHeader(common->GRPC_ENCODING, common->GZIP);
                } else {
                    res_header->SetHeader(common->CONTENT_ENCODING, common->GZIP);
                }
            } else {
                LOG(ERROR) << "Fail to gzip the http response, skip compression.";
            }
        }
    } else {
        // TODO(gejun): Support snappy (grpc)
        LOG_IF(ERROR, cntl->response_compress_type() != COMPRESS_TYPE_NONE)
            << "Unknown compress_type=" << cntl->response_compress_type()
            << ", skip compression.";
    }

    int rc = -1;
    // Have the risk of unlimited pending responses, in which case, tell
    // users to set max_concurrency.
    Socket::WriteOptions wopt;
    wopt.ignore_eovercrowded = true;
    if (is_http2) {
        if (is_grpc) {
            // Append compressed and length before body
            AddGrpcPrefix(&cntl->response_attachment(), grpc_compressed);
        }
        SocketMessagePtr<H2UnsentResponse> h2_response(
                H2UnsentResponse::New(cntl, _h2_stream_id, is_grpc));
        if (h2_response == NULL) {
            LOG(ERROR) << "Fail to make http2 response";
            errno = EINVAL;
            rc = -1;
        } else {
            if (FLAGS_http_verbose) {
                LOG(INFO) << '\n' << *h2_response;
            }
            if (span) {
                span->set_response_size(h2_response->EstimatedByteSize());
            }
            rc = socket->Write(h2_response, &wopt);
        }
    } else {
        butil::IOBuf* content = NULL;
        if (cntl->Failed() || !cntl->has_progressive_writer()) {
            content = &cntl->response_attachment();
        }
        butil::IOBuf res_buf;
        MakeRawHttpResponse(&res_buf, res_header, content);
        if (FLAGS_http_verbose) {
            PrintMessage(res_buf, false, !!content);
        }
        if (span) {
            span->set_response_size(res_buf.size());
        }
        rc = socket->Write(&res_buf, &wopt);
    }

    if (rc != 0) {
        // EPIPE is common in pooled connections + backup requests.
        const int errcode = errno;
        PLOG_IF(WARNING, errcode != EPIPE) << "Fail to write into " << *socket;
        cntl->SetFailed(errcode, "Fail to write into %s", socket->description().c_str());
        return;
    }
    if (span) {
        // TODO: this is not sent
        span->set_sent_us(butil::cpuwide_time_us());
    }
}

// Normalize the sub string of `uri_path' covered by `splitter' and
// put it into `unresolved_path'
static void FillUnresolvedPath(std::string* unresolved_path,
                               const std::string& uri_path,
                               butil::StringSplitter& splitter) {
    if (unresolved_path == NULL) {
        return;
    }
    if (!splitter) {
        unresolved_path->clear();
        return;
    }
    // Normalize unresolve_path.
    const size_t path_len =
        uri_path.c_str() + uri_path.size() - splitter.field();
    unresolved_path->reserve(path_len);
    unresolved_path->clear();
    for (butil::StringSplitter slash_sp(
             splitter.field(), splitter.field() + path_len, '/');
         slash_sp != NULL; ++slash_sp) {
        if (!unresolved_path->empty()) {
            unresolved_path->push_back('/');
        }
        unresolved_path->append(slash_sp.field(), slash_sp.length());
    }
}

inline const Server::MethodProperty*
FindMethodPropertyByURIImpl(const std::string& uri_path, const Server* server,
                            std::string* unresolved_path) {
    ServerPrivateAccessor wrapper(server);
    butil::StringSplitter splitter(uri_path.c_str(), '/');
    // Show index page for empty URI
    if (NULL == splitter) {
        return wrapper.FindMethodPropertyByFullName(
            IndexService::descriptor()->full_name(), common->DEFAULT_METHOD);
    }
    butil::StringPiece service_name(splitter.field(), splitter.length());
    const bool full_service_name =
        (service_name.find('.') != butil::StringPiece::npos);
    const Server::ServiceProperty* const sp = 
        (full_service_name ?
         wrapper.FindServicePropertyByFullName(service_name) :
         wrapper.FindServicePropertyByName(service_name));
    if (NULL == sp) {
        // normal for urls matching _global_restful_map
        return NULL;
    }
    // Find restful methods by uri.
    if (sp->restful_map) {
        ++splitter;
        butil::StringPiece left_path;
        if (splitter) {
            // The -1 is for including /, always safe because of ++splitter
            left_path.set(splitter.field() - 1, uri_path.c_str() +
                          uri_path.size() - splitter.field() + 1);
        }
        return sp->restful_map->FindMethodProperty(left_path, unresolved_path);
    }
    if (!full_service_name) {
        // Change to service's fullname.
        service_name = sp->service->GetDescriptor()->full_name();
    }

    // Regard URI as [service_name]/[method_name]
    const Server::MethodProperty* mp = NULL;
    butil::StringPiece method_name;
    if (++splitter != NULL) {
        method_name.set(splitter.field(), splitter.length());
        // Copy splitter rather than modifying it directly since it's used
        // in later branches.
        mp = wrapper.FindMethodPropertyByFullName(service_name, method_name);
        if (mp) {
            ++splitter; // skip method name
            FillUnresolvedPath(unresolved_path, uri_path, splitter);
            return mp;
        }
    }
    
    // Try [service_name]/default_method
    mp = wrapper.FindMethodPropertyByFullName(service_name, common->DEFAULT_METHOD);
    if (mp) {
        FillUnresolvedPath(unresolved_path, uri_path, splitter);
        return mp;
    }

    // Call BadMethodService::no_method for service_name-only URL.
    if (method_name.empty()) {
        return wrapper.FindMethodPropertyByFullName(
            BadMethodService::descriptor()->full_name(), common->NO_METHOD);
    }

    // Called an existing service w/o default_method with an unknown method.
    return NULL;
}

// Used in UT, don't be static
const Server::MethodProperty*
FindMethodPropertyByURI(const std::string& uri_path, const Server* server,
                        std::string* unresolved_path) {
    const Server::MethodProperty* mp =
        FindMethodPropertyByURIImpl(uri_path, server, unresolved_path);
    if (mp != NULL) {
        if (mp->http_url != NULL && !mp->params.allow_default_url) {
            // the restful method is accessed from its
            // default url (SERVICE/METHOD) which should be rejected.
            return NULL;
        }
        return mp;
    }
    // uri_path cannot match any methods with exact service_name. Match
    // the fuzzy patterns in global restful map which often matches
    // extension names. Say "*.txt => get_text_file, *.mp4 => download_mp4".
    ServerPrivateAccessor accessor(server);
    if (accessor.global_restful_map()) {
        return accessor.global_restful_map()->FindMethodProperty(
            uri_path, unresolved_path);
    }
    return NULL;
}

ParseResult ParseHttpMessage(butil::IOBuf *source, Socket *socket,
                             bool read_eof, const void* /*arg*/) {
    HttpContext* http_imsg = 
        static_cast<HttpContext*>(socket->parsing_context());
    if (http_imsg == NULL) {
        if (read_eof || source->empty()) {
            // 1. read_eof: Read EOF after intact HTTP messages, a common case.
            //    Notice that errors except NOT_ENOUGH_DATA can't be returned
            //    otherwise the Socket will be SetFailed() and messages just
            //    in ProcessHttpXXX() may be dropped.
            // 2. source->empty(): also common, InputMessage tries parse
            //    handlers until error is met. If a message was consumed,
            //    source is likely to be empty.
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }
        http_imsg = new (std::nothrow) HttpContext(socket->is_read_progressive());
        if (http_imsg == NULL) {
            LOG(FATAL) << "Fail to new HttpContext";
            return MakeParseError(PARSE_ERROR_NO_RESOURCE);
        }
        // Parsing http is costly, parsing an incomplete http message from the
        // beginning repeatedly should be avoided, otherwise the cost may reach
        // O(n^2) in the worst case. Save incomplete http messages in sockets
        // to prevent re-parsing. The message will be released when it is
        // completed or destroyed along with the socket.
        socket->reset_parsing_context(http_imsg);
    }
    ssize_t rc = 0;
    if (read_eof) {
        // Send EOF to HttpContext, check comments in http_message.h
        rc = http_imsg->ParseFromArray(NULL, 0);
    } else {
        // Empty `source' is sliently ignored and 0 is returned, check
        // comments in http_message.h
        rc = http_imsg->ParseFromIOBuf(*source);
    }
    if (http_imsg->is_stage2()) {
        // The header part is already parsed as an intact HTTP message
        // to the ProcessHttpXXX. Here parses the body part.
        if (rc >= 0) {
            source->pop_front(rc);
            if (http_imsg->Completed()) {
                // Already returned the message before, don't return again.
                CHECK_EQ(http_imsg, socket->release_parsing_context());
                // NOTE: calling http_imsg->Destroy() is wrong which can only
                // be called from ProcessHttpXXX
                http_imsg->RemoveOneRefForStage2();
                socket->OnProgressiveReadCompleted();
                return MakeMessage(NULL);
            } else {
                return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
            }
        } else {
            // Fail to parse the body. Since headers were parsed successfully,
            // the message is assumed to be HTTP, stop trying other protocols.
            const char* err = http_errno_description(
                HTTP_PARSER_ERRNO(&http_imsg->parser()));
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG, err);
        }
    } else if (rc >= 0) {
        // Normal or stage1 of progressive-read http message.
        source->pop_front(rc);
        if (http_imsg->Completed()) {
            CHECK_EQ(http_imsg, socket->release_parsing_context());
            const ParseResult result = MakeMessage(http_imsg);
            if (socket->is_read_progressive()) {
                socket->OnProgressiveReadCompleted();
            }
            return result;
        } else if (socket->is_read_progressive() &&
                   http_imsg->stage() >= HTTP_ON_HEADERS_COMPLETE) {
            // header part of a progressively-read http message is complete,
            // go on to ProcessHttpXXX w/o waiting for full body.
            http_imsg->AddOneRefForStage2(); // released when body is fully read
            return MakeMessage(http_imsg);
        } else {
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        }
    } else if (!socket->CreatedByConnect()) {
        // Note: If the parser fails at query-string/fragment/the-following
        // -"HTTP/x.y", the message is very likely to be in http format (not
        // other protocols registered after http). We send 400 back to client
        // which is more informational than just closing the connection (may
        // cause OP's alarms if the remote side is baidu's nginx). To do this,
        // We make InputMessenger do nothing by cheating it with
        // PARSE_ERROR_NOT_ENOUGH_DATA and remove the addtitional ref of the
        // socket so that it will be recycled when the response is written.
        // We can't use SetFailed which interrupts the writing.
        // Tricky: Socket::ReleaseAdditionalReference() does not remove the
        // internal fd from epoll thus we can still get EPOLLIN and read
        // in more data. If the second read happens, parsing_context()
        // should return the same InputMessage that we see now because we
        // don't reset_parsing_context(NULL) in this branch, and following
        // ParseFromXXX should return -1 immediately because of the non-zero
        // parser.http_errno, and ReleaseAdditionalReference() here should
        // return -1 to prevent us from sending another 400.
        if (is_failed_after_queries(&http_imsg->parser())) {
            int rc = socket->ReleaseAdditionalReference();
            if (rc < 0) {
                // Already released, leave the socket to be recycled
                // by itself.
                return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
            } else if (rc > 0) {
                LOG(ERROR) << "Impossible: Recycled!";
                return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
            }
            // Send 400 back.
            butil::IOBuf bad_req;
            HttpHeader header;
            header.set_status_code(HTTP_STATUS_BAD_REQUEST);
            MakeRawHttpRequest(&bad_req, &header, socket->remote_side(), NULL);
            Socket::WriteOptions wopt;
            wopt.ignore_eovercrowded = true;
            socket->Write(&bad_req, &wopt);
            return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
        } else {
            return MakeParseError(PARSE_ERROR_TRY_OTHERS);
        }
    } else {
        if (is_failed_after_http_version(&http_imsg->parser())) {
            return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG,
                                  "invalid http response");
        }
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }
}

bool VerifyHttpRequest(const InputMessageBase* msg) {
    Server* server = (Server*)msg->arg();
    Socket* socket = msg->socket();
    
    HttpContext* http_request = (HttpContext*)msg;
    const Authenticator* auth = server->options().auth;
    if (NULL == auth) {
        // Fast pass
        return true;
    }
    const Server::MethodProperty* mp = FindMethodPropertyByURI(
        http_request->header().uri().path(), server, NULL);
    if (mp != NULL &&
        mp->is_builtin_service &&
        mp->service->GetDescriptor() != BadMethodService::descriptor()) {
        // BuiltinService doesn't need authentication
        // TODO: Fix backdoor that sends BuiltinService at first
        // and then sends other requests without authentication
        return true;
    }

    const std::string *authorization 
        = http_request->header().GetHeader("Authorization");
    if (authorization == NULL) {
        return false;
    }
    butil::EndPoint user_addr;
    if (!GetUserAddressFromHeader(http_request->header(), &user_addr)) {
        user_addr = socket->remote_side();
    }
    return auth->VerifyCredential(*authorization, user_addr,
                                  socket->mutable_auth_context()) == 0;
}


// Defined in baidu_rpc_protocol.cpp
void EndRunningCallMethodInPool(
    ::google::protobuf::Service* service,
    const ::google::protobuf::MethodDescriptor* method,
    ::google::protobuf::RpcController* controller,
    const ::google::protobuf::Message* request,
    ::google::protobuf::Message* response,
    ::google::protobuf::Closure* done);

void ProcessHttpRequest(InputMessageBase *msg) {
    const int64_t start_parse_us = butil::cpuwide_time_us();
    DestroyingPtr<HttpContext> imsg_guard(static_cast<HttpContext*>(msg));
    SocketUniquePtr socket_guard(imsg_guard->ReleaseSocket());
    Socket* socket = socket_guard.get();
    const Server* server = static_cast<const Server*>(msg->arg());
    ScopedNonServiceError non_service_error(server);

    Controller* cntl = new (std::nothrow) Controller;
    if (NULL == cntl) {
        LOG(FATAL) << "Fail to new Controller";
        return;
    }
    HttpResponseSender resp_sender(cntl);
    resp_sender.set_received_us(msg->received_us());

    const bool is_http2 = imsg_guard->header().is_http2();
    if (is_http2) {
        H2StreamContext* h2_sctx = static_cast<H2StreamContext*>(msg);
        resp_sender.set_h2_stream_id(h2_sctx->stream_id());
    }

    ControllerPrivateAccessor accessor(cntl);
    HttpHeader& req_header = cntl->http_request();
    imsg_guard->header().Swap(req_header);
    butil::IOBuf& req_body = imsg_guard->body();

    butil::EndPoint user_addr;
    if (!GetUserAddressFromHeader(req_header, &user_addr)) {
        user_addr = socket->remote_side();
    }
    ServerPrivateAccessor server_accessor(server);
    const bool security_mode = server->options().security_mode() &&
                               socket->user() == server_accessor.acceptor();
    accessor.set_server(server)
        .set_security_mode(security_mode)
        .set_peer_id(socket->id())
        .set_remote_side(user_addr)
        .set_local_side(socket->local_side())
        .set_auth_context(socket->auth_context())
        .set_request_protocol(PROTOCOL_HTTP)
        .set_begin_time_us(msg->received_us())
        .move_in_server_receiving_sock(socket_guard);
    
    // Read log-id. errno may be set when input to strtoull overflows.
    // atoi/atol/atoll don't support 64-bit integer and can't be used.
    const std::string* log_id_str = req_header.GetHeader(common->LOG_ID);
    if (log_id_str) {
        char* logid_end = NULL;
        errno = 0;
        uint64_t logid = strtoull(log_id_str->c_str(), &logid_end, 10);
        if (*logid_end || errno) {
            LOG(ERROR) << "Invalid " << common->LOG_ID << '=' 
                       << *log_id_str << " in http request";
        } else {
            cntl->set_log_id(logid);
        }
    }

    const std::string* request_id = req_header.GetHeader(FLAGS_request_id_header);
    if (request_id) {
        cntl->set_request_id(*request_id);
    }

    // Tag the bthread with this server's key for
    // thread_local_data().
    if (server->thread_local_options().thread_local_data_factory) {
        bthread_assign_data((void*)&server->thread_local_options());
    }

    Span* span = NULL;
    const std::string& path = req_header.uri().path();
    const std::string* trace_id_str = req_header.GetHeader("x-bd-trace-id");
    if (IsTraceable(trace_id_str)) {
        uint64_t trace_id = 0;
        if (trace_id_str) {
            trace_id = strtoull(trace_id_str->c_str(), NULL, 10);
        }
        uint64_t span_id = 0;
        const std::string* span_id_str = req_header.GetHeader("x-bd-span-id");
        if (span_id_str) {
            span_id = strtoull(span_id_str->c_str(), NULL, 10);
        }
        uint64_t parent_span_id = 0;
        const std::string* parent_span_id_str =
            req_header.GetHeader("x-bd-parent-span-id");
        if (parent_span_id_str) {
            parent_span_id = strtoull(parent_span_id_str->c_str(), NULL, 10);
        }
        span = Span::CreateServerSpan(
            path, trace_id, span_id, parent_span_id, msg->base_real_us());
        accessor.set_span(span);
        span->set_log_id(cntl->log_id());
        span->set_remote_side(user_addr);
        span->set_received_us(msg->received_us());
        span->set_start_parse_us(start_parse_us);
        span->set_protocol(is_http2 ? PROTOCOL_H2 : PROTOCOL_HTTP);
        span->set_request_size(imsg_guard->parsed_length());
    }
    
    if (!server->IsRunning()) {
        cntl->SetFailed(ELOGOFF, "Server is stopping");
        return;
    }

    if (server->options().http_master_service) {
        // If http_master_service is on, just call it.
        google::protobuf::Service* svc = server->options().http_master_service;
        const google::protobuf::MethodDescriptor* md =
            svc->GetDescriptor()->FindMethodByName(common->DEFAULT_METHOD);
        if (md == NULL) {
            cntl->SetFailed(ENOMETHOD, "No default_method in http_master_service");
            return;
        }
        accessor.set_method(md);
        cntl->request_attachment().swap(req_body);
        google::protobuf::Closure* done = new HttpResponseSenderAsDone(&resp_sender);
        if (span) {
            span->ResetServerSpanName(md->full_name());
            span->set_start_callback_us(butil::cpuwide_time_us());
            span->AsParent();
        }
        // `cntl', `req' and `res' will be deleted inside `done'
        return svc->CallMethod(md, cntl, NULL, NULL, done);
    }
    
    const Server::MethodProperty* const sp =
        FindMethodPropertyByURI(path, server, &req_header._unresolved_path);
    if (NULL == sp) {
        if (security_mode) {
            std::string escape_path;
            WebEscape(path, &escape_path);
            cntl->SetFailed(ENOMETHOD, "Fail to find method on `%s'", escape_path.c_str());
        } else {
            cntl->SetFailed(ENOMETHOD, "Fail to find method on `%s'", path.c_str());
        }
        return;
    } else if (sp->service->GetDescriptor() == BadMethodService::descriptor()) {
        BadMethodRequest breq;
        BadMethodResponse bres;
        butil::StringSplitter split(path.c_str(), '/');
        breq.set_service_name(std::string(split.field(), split.length()));
        sp->service->CallMethod(sp->method, cntl, &breq, &bres, NULL);
        return;
    }
    // Switch to service-specific error.
    non_service_error.release();
    MethodStatus* method_status = sp->status;
    resp_sender.set_method_status(method_status);
    if (method_status) {
        int rejected_cc = 0;
        if (!method_status->OnRequested(&rejected_cc)) {
            cntl->SetFailed(ELIMIT, "Rejected by %s's ConcurrencyLimiter, concurrency=%d",
                            sp->method->full_name().c_str(), rejected_cc);
            return;
        }
    }
    
    if (span) {
        span->ResetServerSpanName(sp->method->full_name());
    }
    // NOTE: accesses to builtin services are not counted as part of
    // concurrency, therefore are not limited by ServerOptions.max_concurrency.
    if (!sp->is_builtin_service && !sp->params.is_tabbed) {
        if (socket->is_overcrowded()) {
            cntl->SetFailed(EOVERCROWDED, "Connection to %s is overcrowded",
                            butil::endpoint2str(socket->remote_side()).c_str());
            return;
        }
        if (!server_accessor.AddConcurrency(cntl)) {
            cntl->SetFailed(ELIMIT, "Reached server's max_concurrency=%d",
                            server->options().max_concurrency);
            return;
        }
        if (FLAGS_usercode_in_pthread && TooManyUserCode()) {
            cntl->SetFailed(ELIMIT, "Too many user code to run when"
                            " -usercode_in_pthread is on");
            return;
        }
    } else if (security_mode) {
        cntl->SetFailed(EPERM, "Not allowed to access builtin services, try "
                        "ServerOptions.internal_port=%d instead if you're in"
                        " internal network", server->options().internal_port);
        return;
    }

    google::protobuf::Service* svc = sp->service;
    const google::protobuf::MethodDescriptor* method = sp->method;
    accessor.set_method(method);
    google::protobuf::Message* req = svc->GetRequestPrototype(method).New();
    resp_sender.own_request(req);
    google::protobuf::Message* res = svc->GetResponsePrototype(method).New();
    resp_sender.own_response(res);

    if (__builtin_expect(!req || !res, 0)) {
        PLOG(FATAL) << "Fail to new req or res";
        cntl->SetFailed("Fail to new req or res");
        return;
    }
    if (sp->params.allow_http_body_to_pb &&
        method->input_type()->field_count() > 0) {
        // A protobuf service. No matter if Content-type is set to
        // applcation/json or body is empty, we have to treat body as a json
        // and try to convert it to pb, which guarantees that a protobuf
        // service is always accessed with valid requests.
        if (req_body.empty()) {
            // Treat empty body specially since parsing it results in error
            if (!req->IsInitialized()) {
                cntl->SetFailed(EREQUEST, "%s needs to be created from a"
                                " non-empty json, it has required fields.",
                                req->GetDescriptor()->full_name().c_str());
                return;
            } // else all fields of the request are optional.
        } else {
            bool is_grpc_ct = false;
            const HttpContentType content_type =
                ParseContentType(req_header.content_type(), &is_grpc_ct);
            const std::string* encoding = NULL;
            if (is_http2) {
                if (is_grpc_ct) {
                    bool grpc_compressed = false;
                    if (!RemoveGrpcPrefix(&req_body, &grpc_compressed)) {
                        cntl->SetFailed(ERESPONSE, "Invalid gRPC response");
                        return;
                    }
                    if (grpc_compressed) {
                        encoding = req_header.GetHeader(common->GRPC_ENCODING);
                        if (encoding == NULL) {
                            cntl->SetFailed(
                                EREQUEST, "Fail to find header `grpc-encoding'"
                                " in compressed gRPC request");
                            return;
                        }
                    }
                    int64_t timeout_value_us =
                        ConvertGrpcTimeoutToUS(req_header.GetHeader(common->GRPC_TIMEOUT));
                    if (timeout_value_us >= 0) {
                        accessor.set_deadline_us(
                                butil::gettimeofday_us() + timeout_value_us);
                    }
                }
            } else {
                encoding = req_header.GetHeader(common->CONTENT_ENCODING);
            }
            if (encoding != NULL && *encoding == common->GZIP) {
                TRACEPRINTF("Decompressing request=%lu",
                            (unsigned long)req_body.size());
                butil::IOBuf uncompressed;
                if (!policy::GzipDecompress(req_body, &uncompressed)) {
                    cntl->SetFailed(EREQUEST, "Fail to un-gzip request body");
                    return;
                }
                req_body.swap(uncompressed);
            }
            if (content_type == HTTP_CONTENT_PROTO) {
                if (!ParsePbFromIOBuf(req, req_body)) {
                    cntl->SetFailed(EREQUEST, "Fail to parse http body as %s",
                                    req->GetDescriptor()->full_name().c_str());
                    return;
                }
            } else {
                butil::IOBufAsZeroCopyInputStream wrapper(req_body);
                std::string err;
                json2pb::Json2PbOptions options;
                options.base64_to_bytes = sp->params.pb_bytes_to_base64;
                cntl->set_pb_bytes_to_base64(sp->params.pb_bytes_to_base64);
                if (!json2pb::JsonToProtoMessage(&wrapper, req, options, &err)) {
                    cntl->SetFailed(EREQUEST, "Fail to parse http body as %s, %s",
                                    req->GetDescriptor()->full_name().c_str(), err.c_str());
                    return;
                }
            }
        }
    } else {
        // A http server, just keep content as it is.
        cntl->request_attachment().swap(req_body);
    }

    google::protobuf::Closure* done = new HttpResponseSenderAsDone(&resp_sender);
    imsg_guard.reset();  // optional, just release resourse ASAP

    if (span) {
        span->set_start_callback_us(butil::cpuwide_time_us());
        span->AsParent();
    }
    if (!FLAGS_usercode_in_pthread) {
        return svc->CallMethod(method, cntl, req, res, done);
    }
    if (BeginRunningUserCode()) {
        svc->CallMethod(method, cntl, req, res, done);
        return EndRunningUserCodeInPlace();
    } else {
        return EndRunningCallMethodInPool(svc, method, cntl, req, res, done);
    }
}

bool ParseHttpServerAddress(butil::EndPoint* point, const char* server_addr_and_port) {
    std::string scheme;
    std::string host;
    int port = -1;
    if (ParseURL(server_addr_and_port, &scheme, &host, &port) != 0) {
        LOG(ERROR) << "Invalid address=`" << server_addr_and_port << '\'';
        return false;
    }
    if (scheme.empty() || scheme == "http") {
        if (port < 0) {
            port = 80;
        }
    } else if (scheme == "https") {
        if (port < 0) {
            port = 443;
        }
    } else {
        LOG(ERROR) << "Invalid scheme=`" << scheme << '\'';
        return false;
    }
    if (str2endpoint(host.c_str(), port, point) != 0 &&
        hostname2endpoint(host.c_str(), port, point) != 0) {
        LOG(ERROR) << "Invalid host=" << host << " port=" << port;
        return false;
    }
    return true;
}

const std::string& GetHttpMethodName(
    const google::protobuf::MethodDescriptor*,
    const Controller* cntl) {
    return cntl->http_request().uri().path();
}

}  // namespace policy
} // namespace brpc
