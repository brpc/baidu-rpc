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


#ifndef  BRPC_HTTP_HEADER_H
#define  BRPC_HTTP_HEADER_H

#include "butil/strings/string_piece.h"  // StringPiece
#include "butil/containers/case_ignored_flat_map.h"
#include "brpc/uri.h"              // URI
#include "brpc/http_method.h"      // HttpMethod
#include "brpc/http_status_code.h"
#include "brpc/http2.h"

// To rpc developers: DON'T put impl. details here, use opaque pointers instead.


namespace brpc {
class InputMessageBase;
namespace policy {
void ProcessHttpRequest(InputMessageBase *msg);
class H2StreamContext;
}

// Non-body part of a HTTP message.
class HttpHeader {
public:
    typedef butil::CaseIgnoredFlatMap<std::string> HeaderMap;
    typedef HeaderMap::const_iterator HeaderIterator;

    HttpHeader();

    // Exchange internal fields with another HttpHeader.
    void Swap(HttpHeader &rhs);

    // Reset internal fields as if they're just default-constructed.
    void Clear();

    // Get http version, 1.1 by default.
    int major_version() const { return _version.first; }
    int minor_version() const { return _version.second; }
    // Change the http version
    void set_version(int http_major, int http_minor)
    { _version = std::make_pair(http_major, http_minor); }

    // True if version of http is earlier than 1.1
    bool before_http_1_1() const
    { return (major_version() * 10000 +  minor_version()) <= 10000; }

    // True if the message is from HTTP2.
    bool is_http2() const { return major_version() == 2; }

    // Get/set "Content-Type". Notice that you can't get "Content-Type"
    // via GetHeader().
    // possible values: "text/plain", "application/json" ...
    const std::string& content_type() const { return _content_type; }
    void set_content_type(const std::string& type) { _content_type = type; }
    void set_content_type(const char* type) { _content_type = type; }
    std::string& mutable_content_type() { return _content_type; }
    
    // Get value of a header which is case-insensitive according to:
    //   https://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
    // Namely, GetHeader("log-id"), GetHeader("Log-Id"), GetHeader("LOG-ID")
    // point to the same value.
    // Return pointer to the value, NULL on not found.
    // NOTE: Not work for "Content-Type", call content_type() instead.
    const std::string* GetHeader(const char* key) const
    { return _headers.seek(key); }
    const std::string* GetHeader(const std::string& key) const
    { return _headers.seek(key); }

    // Set value of a header.
    // NOTE: Not work for "Content-Type", call set_content_type() instead.
    void SetHeader(const std::string& key, const std::string& value)
    { GetOrAddHeader(key) = value; }

    // Remove a header.
    void RemoveHeader(const char* key) { _headers.erase(key); }
    void RemoveHeader(const std::string& key) { _headers.erase(key); }

    // Append value to a header. If the header already exists, separate
    // old value and new value with comma(,) according to:
    //   https://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html#sec4.2
    void AppendHeader(const std::string& key, const butil::StringPiece& value);
    
    // Get header iterators which are invalidated after calling AppendHeader()
    HeaderIterator HeaderBegin() const { return _headers.begin(); }
    HeaderIterator HeaderEnd() const { return _headers.end(); }
    // #headers
    size_t HeaderCount() const { return _headers.size(); }

    // Get the URI object, check src/brpc/uri.h for details.
    const URI& uri() const { return _uri; }
    URI& uri() { return _uri; }

    // Get/set http method.
    HttpMethod method() const { return _method; }
    void set_method(const HttpMethod method) { _method = method; }

    // Get/set status-code and reason-phrase. Notice that the const char*
    // returned by reason_phrase() will be invalidated after next call to
    // set_status_code().
    int status_code() const { return _status_code; }
    const char* reason_phrase() const;
    void set_status_code(int status_code);

    // The URL path removed with matched prefix.
    // NOTE: always normalized and NOT started with /.
    //
    // Accessing HttpService.Echo
    //   [URL]                               [unresolved_path]
    //   "/HttpService/Echo"                 ""
    //   "/HttpService/Echo/Foo"             "Foo"
    //   "/HttpService/Echo/Foo/Bar"         "Foo/Bar"
    //   "/HttpService//Echo///Foo//"        "Foo"
    //
    // Accessing FileService.default_method:
    //   [URL]                               [unresolved_path]
    //   "/FileService"                      ""
    //   "/FileService/123.txt"              "123.txt"
    //   "/FileService/mydir/123.txt"        "mydir/123.txt"
    //   "/FileService//mydir///123.txt//"   "mydir/123.txt"
    const std::string& unresolved_path() const { return _unresolved_path; }

    uint64_t content_length() const { return _content_length; }
    void set_content_length(uint64_t length) { _content_length = length; }

private:
friend class HttpMessage;
friend class HttpMessageSerializer;
friend class policy::H2StreamContext;
friend void policy::ProcessHttpRequest(InputMessageBase *msg);

    std::string& GetOrAddHeader(const std::string& key) {
        if (!_headers.initialized()) {
            _headers.init(29);
        }
        return _headers[key];
    }

    HeaderMap _headers;
    URI _uri;
    int _status_code;
    HttpMethod _method;
    std::string _content_type;
    std::string _unresolved_path;
    std::pair<int, int> _version;
    uint64_t _content_length;
};

const HttpHeader& DefaultHttpHeader();

} // namespace brpc


#endif  //BRPC_HTTP_HEADER_H
