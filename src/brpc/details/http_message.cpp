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


#include <cstdlib>

#include <string>                               // std::string
#include <iostream>
#include <gflags/gflags.h>
#include "butil/macros.h"
#include "butil/logging.h"                       // LOG
#include "butil/scoped_lock.h"
#include "butil/endpoint.h"
#include "butil/base64.h"
#include "bthread/bthread.h"                    // bthread_usleep
#include "brpc/log.h"
#include "brpc/reloadable_flags.h"
#include "brpc/details/http_message.h"

namespace brpc {

DEFINE_bool(http_verbose, false,
            "[DEBUG] Print EVERY http request/response");
DEFINE_int32(http_verbose_max_body_length, 512,
             "[DEBUG] Max body length printed when -http_verbose is on");
DECLARE_int64(socket_max_unwritten_bytes);

// Implement callbacks for http parser

int HttpMessage::on_message_begin(http_parser *parser) {
    HttpMessage *http_message = (HttpMessage *)parser->data;
    http_message->_stage = HTTP_ON_MESSAGE_BEGIN;
    return 0;
}

// For request
int HttpMessage::on_url(http_parser *parser, 
                        const char *at, const size_t length) {
    HttpMessage *http_message = (HttpMessage *)parser->data;
    http_message->_stage = HTTP_ON_URL;
    http_message->_url.append(at, length);
    return 0;
}

// For response
int HttpMessage::on_status(http_parser *parser, const char *, const size_t) {
    HttpMessage *http_message = (HttpMessage *)parser->data;
    http_message->_stage = HTTP_ON_STATUS;
    // According to https://www.w3.org/Protocols/rfc2616/rfc2616-sec6.html
    // Client is not required to examine or display the Reason-Phrase
    return 0;
}

// http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
// Multiple message-header fields with the same field-name MAY be present in a
// message if and only if the entire field-value for that header field is
// defined as a comma-separated list [i.e., #(values)]. It MUST be possible to
// combine the multiple header fields into one "field-name: field-value" pair,
// without changing the semantics of the message, by appending each subsequent
// field-value to the first, each separated by a comma. The order in which
// header fields with the same field-name are received is therefore significant
// to the interpretation of the combined field value, and thus a proxy MUST NOT
// change the order of these field values when a message is forwarded. 
int HttpMessage::on_header_field(http_parser *parser,
                                 const char *at, const size_t length) {
    HttpMessage *http_message = (HttpMessage *)parser->data;
    if (http_message->_stage != HTTP_ON_HEADER_FIELD) {
        http_message->_stage = HTTP_ON_HEADER_FIELD;
        http_message->_cur_header.clear();
    }
    http_message->_cur_header.append(at, length);
    return 0;
}

int HttpMessage::on_header_value(http_parser *parser,
                                 const char *at, const size_t length) {
    HttpMessage *http_message = (HttpMessage *)parser->data;
    bool first_entry = false;
    if (http_message->_stage != HTTP_ON_HEADER_VALUE) {
        http_message->_stage = HTTP_ON_HEADER_VALUE;
        first_entry = true;
        if (http_message->_cur_header.empty()) {
            LOG(ERROR) << "Header name is empty";
            return -1;
        }
        http_message->_cur_value =
            &http_message->header().GetOrAddHeader(http_message->_cur_header);
        if (http_message->_cur_value && !http_message->_cur_value->empty()) {
            http_message->_cur_value->push_back(',');
        }
    }
    if (http_message->_cur_value) {
        http_message->_cur_value->append(at, length);
    }
    if (FLAGS_http_verbose) {
        butil::IOBufBuilder* vs = http_message->_vmsgbuilder;
        if (vs == NULL) {
            vs = new butil::IOBufBuilder;
            http_message->_vmsgbuilder = vs;
            if (parser->type == HTTP_REQUEST) {
                *vs << "[ HTTP REQUEST @" << butil::my_ip() << " ]\n< "
                    << HttpMethod2Str((HttpMethod)parser->method) << ' '
                    << http_message->_url << " HTTP/" << parser->http_major
                    << '.' << parser->http_minor;
            } else {
                // NOTE: http_message->header().status_code() may not be set yet.
                *vs << "[ HTTP RESPONSE @" << butil::my_ip() << " ]\n< HTTP/"
                    << parser->http_major
                    << '.' << parser->http_minor << ' ' << parser->status_code
                    << ' ' << HttpReasonPhrase(parser->status_code);
            }
        }
        if (first_entry) {
            *vs << "\n< " << http_message->_cur_header << ": ";
        }
        vs->write(at, length);
    }
    return 0;
}

int HttpMessage::on_headers_complete(http_parser *parser) {
    HttpMessage *http_message = (HttpMessage *)parser->data;
    http_message->_stage = HTTP_ON_HEADERS_COMPLETE;
    http_message->header().set_content_length(parser->content_length);
    // Move content-type into the member field.
    const std::string* content_type = http_message->header().GetHeader("content-type");
    if (content_type) {
        http_message->header().set_content_type(*content_type);
        http_message->header().RemoveHeader("content-type");
    }
    if (parser->http_major > 1) {
        // NOTE: this checking is a MUST because ProcessHttpResponse relies
        // on it to cast InputMessageBase* into different types.
        LOG(WARNING) << "Invalid major_version=" << parser->http_major;
        parser->http_major = 1;
    }
    http_message->header().set_version(parser->http_major, parser->http_minor);
    // Only for response
    // http_parser may set status_code to 0 when the field is not needed,
    // e.g. in a request. In principle status_code is undefined in a request,
    // but to be consistent and not surprise users, we set it to OK as well.
    http_message->header().set_status_code(
        !parser->status_code ? HTTP_STATUS_OK : parser->status_code);
    // Only for request
    // method is 0(which is DELETE) for response as well. Since users are
    // unlikely to check method of a response, we don't do anything.
    http_message->header().set_method(static_cast<HttpMethod>(parser->method));
    if (parser->type == HTTP_REQUEST &&
        http_message->header().uri().SetHttpURL(http_message->_url) != 0) {
        LOG(ERROR) << "Fail to parse url=`" << http_message->_url << '\'';
        return -1;
    }
    //rfc2616-sec5.2
    //1. If Request-URI is an absoluteURI, the host is part of the Request-URI.
    //Any Host header field value in the request MUST be ignored.
    //2. If the Request-URI is not an absoluteURI, and the request includes a
    //Host header field, the host is determined by the Host header field value.
    //3. If the host as determined by rule 1 or 2 is not a valid host on the
    //server, the responce MUST be a 400 error messsage.
    URI & uri = http_message->header().uri();
    if (uri._host.empty()) {
        const std::string* host_header = http_message->header().GetHeader("host");
        if (host_header != NULL) {
            uri.SetHostAndPort(*host_header);
        }
    }
    return 0;
}

int HttpMessage::UnlockAndFlushToBodyReader(std::unique_lock<butil::Mutex>& mu) {
    if (_body.empty()) {
        mu.unlock();
        return 0;
    }
    butil::IOBuf body_seen = _body.movable();
    ProgressiveReader* r = _body_reader;
    mu.unlock();
    for (size_t i = 0; i < body_seen.backing_block_num(); ++i) {
        butil::StringPiece blk = body_seen.backing_block(i);
        butil::Status st = r->OnReadOnePart(blk.data(), blk.size());
        if (!st.ok()) {
            mu.lock();
            _body_reader = NULL;
            mu.unlock();
            r->OnEndOfMessage(st);
            return -1;
        }
    }
    return 0;
}

int HttpMessage::on_body_cb(http_parser *parser,
                            const char *at, const size_t length) {
    return static_cast<HttpMessage*>(parser->data)->OnBody(at, length);
}

int HttpMessage::on_message_complete_cb(http_parser *parser) {
    return static_cast<HttpMessage*>(parser->data)->OnMessageComplete();
}

int HttpMessage::OnBody(const char *at, const size_t length) {
    if (_vmsgbuilder) {
        if (_stage != HTTP_ON_BODY) {
            // only add prefix at first entry.
            *_vmsgbuilder << "\n<\n";
        }
        if (_read_body_progressively &&
            // If the status_code is non-OK, the body is likely to be the error
            // description which is very helpful for debugging. Otherwise
            // the body is probably streaming data which is too long to print.
            header().status_code() == HTTP_STATUS_OK) {
            LOG(INFO) << '\n' << _vmsgbuilder->buf();
            delete _vmsgbuilder;
            _vmsgbuilder = NULL;
        } else {
            if (_vbodylen < (size_t)FLAGS_http_verbose_max_body_length) {
                int plen = std::min(length, (size_t)FLAGS_http_verbose_max_body_length
                                    - _vbodylen);
                std::string str = butil::ToPrintableString(
                    at, plen, std::numeric_limits<size_t>::max());
                _vmsgbuilder->write(str.data(), str.size());
            }
            _vbodylen += length;
        }
    }
    if (_stage != HTTP_ON_BODY) {
        _stage = HTTP_ON_BODY;
    }
    if (!_read_body_progressively) {
        // Normal read.
        // TODO: The input data is from IOBuf as well, possible to append
        // data w/o copying.
        _body.append(at, length);
        return 0;
    }
    // Progressive read.
    std::unique_lock<butil::Mutex> mu(_body_mutex);
    ProgressiveReader* r = _body_reader;
    while (r == NULL) {
        // When _body is full, the sleep-waiting may block parse handler
        // of the protocol. A more efficient solution is to remove the
        // socket from epoll and add it back when the _body is not full,
        // which requires a set of complicated "pause" and "unpause"
        // asynchronous API. We just leave the job to bthread right now
        // to make everything work.
        if ((int64_t)_body.size() <= FLAGS_socket_max_unwritten_bytes) {
            _body.append(at, length);
            return 0;
        }
        mu.unlock();
        bthread_usleep(10000/*10ms*/);
        mu.lock();
        r = _body_reader;
    }
    // Safe to operate _body_reader outside lock because OnBody() is
    // guaranteed to be called by only one thread.
    if (UnlockAndFlushToBodyReader(mu) != 0) {
        return -1;
    }
    butil::Status st = r->OnReadOnePart(at, length);
    if (st.ok()) {
        return 0;
    }
    mu.lock();
    _body_reader = NULL;
    mu.unlock();
    r->OnEndOfMessage(st);
    return -1;
}

int HttpMessage::OnMessageComplete() {
    if (_vmsgbuilder) {
        if (_vbodylen > (size_t)FLAGS_http_verbose_max_body_length) {
            *_vmsgbuilder << "\n<skipped " << _vbodylen
                - (size_t)FLAGS_http_verbose_max_body_length << " bytes>";
        }
        LOG(INFO) << '\n' << _vmsgbuilder->buf();
        delete _vmsgbuilder;
        _vmsgbuilder = NULL;
    }
    _cur_header.clear();
    _cur_value = NULL;
    if (!_read_body_progressively) {
        // Normal read.
        _stage = HTTP_ON_MESSAGE_COMPLETE;
        return 0;
    }
    // Progressive read.
    std::unique_lock<butil::Mutex> mu(_body_mutex);
    _stage = HTTP_ON_MESSAGE_COMPLETE;
    if (_body_reader != NULL) {
        // Solve the case: SetBodyReader quit at ntry=MAX_TRY with non-empty
        // _body and the remaining _body is just the last part.
        // Make sure _body is emptied.
        if (UnlockAndFlushToBodyReader(mu) != 0) {
            return -1;
        }
        mu.lock();
        ProgressiveReader* r = _body_reader;
        _body_reader = NULL;
        mu.unlock();
        r->OnEndOfMessage(butil::Status());
    }
    return 0;
}

class FailAllRead : public ProgressiveReader {
public:
    // @ProgressiveReader
    butil::Status OnReadOnePart(const void* /*data*/, size_t /*length*/) {
        return butil::Status(-1, "Trigger by FailAllRead at %s:%d",
                            __FILE__, __LINE__);
    }
    void OnEndOfMessage(const butil::Status&) {}
};

static FailAllRead* s_fail_all_read = NULL;
static pthread_once_t s_fail_all_read_once = PTHREAD_ONCE_INIT;
static void CreateFailAllRead() { s_fail_all_read = new FailAllRead; }

void HttpMessage::SetBodyReader(ProgressiveReader* r) {
    if (!_read_body_progressively) {
        return r->OnEndOfMessage(
            butil::Status(EPERM, "Call SetBodyReader on HttpMessage with"
                         " read_body_progressively=false"));
    }
    const int MAX_TRY = 3;
    int ntry = 0;
    do {
        std::unique_lock<butil::Mutex> mu(_body_mutex);
        if (_body_reader != NULL) {
            mu.unlock();
            return r->OnEndOfMessage(
                butil::Status(EPERM, "SetBodyReader is called more than once"));
        }
        if (_body.empty()) {
            if (_stage <= HTTP_ON_BODY) {
                _body_reader = r;
                return;
            } else {  // The body is complete and successfully consumed.
                mu.unlock();
                return r->OnEndOfMessage(butil::Status());
            }
        } else if (_stage <= HTTP_ON_BODY && ++ntry >= MAX_TRY) {
            // Stop making _body empty after we've tried several times.
            // If _stage is greater than HTTP_ON_BODY, neither OnBody() nor
            // OnMessageComplete() will be called in future, we have to spin
            // another time to empty _body.
            _body_reader = r;
            return;
        }
        butil::IOBuf body_seen = _body.movable();
        mu.unlock();
        for (size_t i = 0; i < body_seen.backing_block_num(); ++i) {
            butil::StringPiece blk = body_seen.backing_block(i);
            butil::Status st = r->OnReadOnePart(blk.data(), blk.size());
            if (!st.ok()) {
                r->OnEndOfMessage(st);
                // Make OnBody() or OnMessageComplete() fail on next call to
                // close the socket. If the message was already complete, the
                // socket will not be closed.
                pthread_once(&s_fail_all_read_once, CreateFailAllRead);
                r = s_fail_all_read;
                ntry = MAX_TRY;
                break;
            }
        }
    } while (true);
}

const http_parser_settings g_parser_settings = {
    &HttpMessage::on_message_begin,
    &HttpMessage::on_url,
    &HttpMessage::on_status,
    &HttpMessage::on_header_field,
    &HttpMessage::on_header_value,
    &HttpMessage::on_headers_complete,
    &HttpMessage::on_body_cb,
    &HttpMessage::on_message_complete_cb
};

HttpMessage::HttpMessage(bool read_body_progressively)
    : _parsed_length(0)
    , _stage(HTTP_ON_MESSAGE_BEGIN)
    , _read_body_progressively(read_body_progressively)
    , _body_reader(NULL)
    , _cur_value(NULL)
    , _vmsgbuilder(NULL)
    , _vbodylen(0) {
    http_parser_init(&_parser, HTTP_BOTH);
    _parser.data = this;
}

HttpMessage::~HttpMessage() {
    if (_body_reader) {
        ProgressiveReader* saved_body_reader = _body_reader;
        _body_reader = NULL;
        // Successfully ended message is ended in OnMessageComplete() or
        // SetBodyReader() and _body_reader should be null-ed. Non-null
        // _body_reader here just means the socket is broken before completion
        // of the message.
        saved_body_reader->OnEndOfMessage(
            butil::Status(ECONNRESET, "The socket was broken"));
    }
}

ssize_t HttpMessage::ParseFromArray(const char *data, const size_t length) {
    if (Completed()) {
        if (length == 0) {
            return 0;
        }
        LOG(ERROR) << "Append data(len=" << length
                   << ") to already-completed message";
        return -1;
    }
    const size_t nprocessed =
        http_parser_execute(&_parser, &g_parser_settings, data, length);
    if (_parser.http_errno != 0) {
        // May try HTTP on other formats, failure is norm.
        RPC_VLOG << "Fail to parse http message, parser=" << _parser
                 << ", buf=`" << butil::StringPiece(data, length) << '\'';
        return -1;
    } 
    _parsed_length += nprocessed;
    return nprocessed;
}

ssize_t HttpMessage::ParseFromIOBuf(const butil::IOBuf &buf) {
    if (Completed()) {
        if (buf.empty()) {
            return 0;
        }
        LOG(ERROR) << "Append data(len=" << buf.size()
                   << ") to already-completed message";
        return -1;
    }
    size_t nprocessed = 0;
    for (size_t i = 0; i < buf.backing_block_num(); ++i) {
        butil::StringPiece blk = buf.backing_block(i);
        if (blk.empty()) {
            // length=0 will be treated as EOF by http_parser, must skip.
            continue;
        }
        nprocessed += http_parser_execute(
            &_parser, &g_parser_settings, blk.data(), blk.size());
        if (_parser.http_errno != 0) {
            // May try HTTP on other formats, failure is norm.
            RPC_VLOG << "Fail to parse http message, parser=" << _parser
                     << ", buf=" << butil::ToPrintable(buf);
            return -1;
        }
        if (Completed()) {
            break;
        }
    }
    _parsed_length += nprocessed;
    return (ssize_t)nprocessed;
}

static void DescribeHttpParserFlags(std::ostream& os, unsigned int flags) {
    if (flags & F_CHUNKED) {
        os << "F_CHUNKED|";
    }
    if (flags & F_CONNECTION_KEEP_ALIVE) {
        os << "F_CONNECTION_KEEP_ALIVE|";
    }
    if (flags & F_CONNECTION_CLOSE) {
        os << "F_CONNECTION_CLOSE|";
    }
    if (flags & F_TRAILING) {
        os << "F_TRAILING|";
    }
    if (flags & F_UPGRADE) {
        os << "F_UPGRADE|";
    }
    if (flags & F_SKIPBODY) {
        os << "F_SKIPBODY|";
    }
}

std::ostream& operator<<(std::ostream& os, const http_parser& parser) {
    os << "{type=" << http_parser_type_name((http_parser_type)parser.type)
       << " flags=`";
    DescribeHttpParserFlags(os, parser.flags);
    os << "' state=" << http_parser_state_name(parser.state)
       << " header_state=" << http_parser_header_state_name(
           parser.header_state)
       << " http_errno=`" << http_errno_description(
           (http_errno)parser.http_errno)
       << "' index=" << parser.index
       << " nread=" << parser.nread
       << " content_length=" << parser.content_length
       << " http_major=" << parser.http_major
       << " http_minor=" << parser.http_minor;
    if (parser.type == HTTP_RESPONSE || parser.type == HTTP_BOTH) {
        os << " status_code=" << parser.status_code;
    }
    if (parser.type == HTTP_REQUEST || parser.type == HTTP_BOTH) {
        os << " method=" << HttpMethod2Str((HttpMethod)parser.method);
    }
    os << " data=" << parser.data
       << '}';
    return os;
}

#define BRPC_CRLF "\r\n"

// Request format
// Request       = Request-Line              ; Section 5.1
//                 *(( general-header        ; Section 4.5
//                  | request-header         ; Section 5.3
//                  | entity-header ) CRLF)  ; Section 7.1
//                  CRLF
//                 [ message-body ]          ; Section 4.3
// Request-Line   = Method SP Request-URI SP HTTP-Version CRLF
// Method         = "OPTIONS"                ; Section 9.2
//                | "GET"                    ; Section 9.3
//                | "HEAD"                   ; Section 9.4
//                | "POST"                   ; Section 9.5
//                | "PUT"                    ; Section 9.6
//                | "DELETE"                 ; Section 9.7
//                | "TRACE"                  ; Section 9.8
//                | "CONNECT"                ; Section 9.9
//                | extension-method
// extension-method = token
void MakeRawHttpRequest(butil::IOBuf* request,
                        HttpHeader* h,
                        const butil::EndPoint& remote_side,
                        const butil::IOBuf* content) {
    butil::IOBufBuilder os;
    os << HttpMethod2Str(h->method()) << ' ';
    const URI& uri = h->uri();
    uri.PrintWithoutHost(os); // host is sent by "Host" header.
    os << " HTTP/" << h->major_version() << '.'
       << h->minor_version() << BRPC_CRLF;
    if (h->method() != HTTP_METHOD_GET) {
        h->RemoveHeader("Content-Length");
        // Never use "Content-Length" set by user.
        os << "Content-Length: " << (content ? content->length() : 0)
           << BRPC_CRLF;
    }
    //rfc 7230#section-5.4:
    //A client MUST send a Host header field in all HTTP/1.1 request
    //messages. If the authority component is missing or undefined for
    //the target URI, then a client MUST send a Host header field with an
    //empty field-value.
    //rfc 7231#sec4.3:
    //the request-target consists of only the host name and port number of 
    //the tunnel destination, seperated by a colon. For example,
    //Host: server.example.com:80
    if (h->GetHeader("host") == NULL) {
        os << "Host: ";
        if (!uri.host().empty()) {
            os << uri.host();
            if (uri.port() >= 0) {
                os << ':' << uri.port();
            }
        } else if (remote_side.port != 0) {
            os << remote_side;
        }
        os << BRPC_CRLF;
    }
    if (!h->content_type().empty()) {
        os << "Content-Type: " << h->content_type()
           << BRPC_CRLF;
    }
    for (HttpHeader::HeaderIterator it = h->HeaderBegin();
         it != h->HeaderEnd(); ++it) {
        os << it->first << ": " << it->second << BRPC_CRLF;
    }
    if (h->GetHeader("Accept") == NULL) {
        os << "Accept: */*" BRPC_CRLF;
    }
    // The fake "curl" user-agent may let servers return plain-text results.
    if (h->GetHeader("User-Agent") == NULL) {
        os << "User-Agent: brpc/1.0 curl/7.0" BRPC_CRLF;
    }
    const std::string& user_info = h->uri().user_info();
    if (!user_info.empty() && h->GetHeader("Authorization") == NULL) {
        // NOTE: just assume user_info is well formatted, namely
        // "<user_name>:<password>". Users are very unlikely to add extra
        // characters in this part and even if users did, most of them are
        // invalid and rejected by http_parser_parse_url().
        std::string encoded_user_info;
        butil::Base64Encode(user_info, &encoded_user_info);
        os << "Authorization: Basic " << encoded_user_info << BRPC_CRLF;
    }
    os << BRPC_CRLF;  // CRLF before content
    os.move_to(*request);
    if (h->method() != HTTP_METHOD_GET && content) {
        request->append(*content);
    }
}

// Response format
// Response     = Status-Line               ; Section 6.1
//                *(( general-header        ; Section 4.5
//                 | response-header        ; Section 6.2
//                 | entity-header ) CRLF)  ; Section 7.1
//                CRLF
//                [ message-body ]          ; Section 7.2
// Status-Line = HTTP-Version SP Status-Code SP Reason-Phrase CRLF
void MakeRawHttpResponse(butil::IOBuf* response,
                         HttpHeader* h,
                         butil::IOBuf* content) {
    butil::IOBufBuilder os;
    os << "HTTP/" << h->major_version() << '.'
       << h->minor_version() << ' ' << h->status_code()
       << ' ' << h->reason_phrase() << BRPC_CRLF;
    if (content) {
        h->RemoveHeader("Content-Length");
        // Never use "Content-Length" set by user.
        // Always set Content-Length since lighttpd requires the header to be
        // set to 0 for empty content.
        os << "Content-Length: " << content->length() << BRPC_CRLF;
    }
    if (!h->content_type().empty()) {
        os << "Content-Type: " << h->content_type()
           << BRPC_CRLF;
    }
    for (HttpHeader::HeaderIterator it = h->HeaderBegin();
         it != h->HeaderEnd(); ++it) {
        os << it->first << ": " << it->second << BRPC_CRLF;
    }
    os << BRPC_CRLF;  // CRLF before content
    os.move_to(*response);
    if (content) {
        response->append(butil::IOBuf::Movable(*content));
    }
}
#undef BRPC_CRLF

} // namespace brpc
