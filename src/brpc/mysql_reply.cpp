// Copyright (c) 2019 Baidu, Inc.
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

// Authors: Yang,Liming (yangliming01@baidu.com)

#include <ios>
#include "brpc/mysql_reply.h"
#include "brpc/mysql_common.h"

namespace brpc {

#define MY_ERROR_RET(expr, ret_code) \
    do {                             \
        if ((expr) == true) {        \
            return (ret_code);       \
        }                            \
    } while (0)

#define MY_ALLOC_CHECK(expr) MY_ERROR_RET(!(expr), PARSE_ERROR_ABSOLUTELY_WRONG)
#define MY_PARSE_CHECK(expr) MY_ERROR_RET(((expr) != PARSE_OK), PARSE_ERROR_NOT_ENOUGH_DATA)
template <class Type>
inline bool my_alloc_check(butil::Arena* arena, const size_t n, Type*& pointer) {
    if (pointer == NULL) {
        pointer = (Type*)arena->allocate(sizeof(Type) * n);
        if (pointer == NULL) {
            return false;
        }
        for (size_t i = 0; i < n; ++i) {
            new (pointer + i) Type;
        }
    }
    return true;
}

struct MysqlHeader {
    uint32_t payload_size;
    uint32_t seq;
};

const char* MysqlRspTypeToString(MysqlRspType type) {
    switch (type) {
        case MYSQL_RSP_OK:
            return "ok";
        case MYSQL_RSP_ERROR:
            return "error";
        case MYSQL_RSP_RESULTSET:
            return "resultset";
        case MYSQL_RSP_EOF:
            return "eof";
        case MYSQL_RSP_AUTH:
            return "auth";
        default:
            return "Unknown Response Type";
    }
}

// check if the buf is contain a full package
inline bool is_full_package(const butil::IOBuf& buf) {
    uint8_t header[mysql_header_size];
    const uint8_t* p = (const uint8_t*)buf.fetch(header, sizeof(header));
    if (p == NULL) {
        return false;
    }
    uint32_t payload_size = mysql_uint3korr(p);
    if (buf.size() < payload_size + mysql_header_size) {
        return false;
    }
    return true;
}
// if is eof package
inline bool is_an_eof(const butil::IOBuf& buf) {
    uint8_t tmp[5];
    const uint8_t* p = (const uint8_t*)buf.fetch(tmp, sizeof(tmp));
    if (p == NULL) {
        return false;
    }
    uint8_t type = p[4];
    if (type == MYSQL_RSP_EOF) {
        return true;
    } else {
        return false;
    }
}
// parse header
inline bool parse_header(butil::IOBuf& buf, MysqlHeader* value) {
    if (!is_full_package(buf)) {
        return false;
    }
    {
        uint8_t tmp[3];
        buf.cutn(tmp, sizeof(tmp));
        value->payload_size = mysql_uint3korr(tmp);
    }
    {
        uint8_t tmp;
        buf.cut1((char*)&tmp);
        value->seq = tmp;
    }
    return true;
}
// use this carefully, we depending on parse_header for checking IOBuf contain full package
inline uint64_t parse_encode_length(butil::IOBuf& buf) {
    if (buf.size() == 0) {
        return 0;
    }

    uint64_t value = 0;
    uint8_t f = 0;
    buf.cut1((char*)&f);
    if (f <= 250) {
        value = f;
    } else if (f == 251) {
        value = 0;
    } else if (f == 252) {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        value = mysql_uint2korr(tmp);
    } else if (f == 253) {
        uint8_t tmp[3];
        buf.cutn(tmp, sizeof(tmp));
        value = mysql_uint3korr(tmp);
    } else if (f == 254) {
        uint8_t tmp[8];
        buf.cutn(tmp, sizeof(tmp));
        value = mysql_uint8korr(tmp);
    }
    return value;
}

ParseError MysqlReply::ConsumePartialIOBuf(
    butil::IOBuf& buf, butil::Arena* arena, bool is_auth, bool is_prepare, bool* more_results) {
    *more_results = false;
    uint8_t header[mysql_header_size + 1];  // use the extra byte to judge message type
    const uint8_t* p = (const uint8_t*)buf.fetch(header, sizeof(header));
    if (p == NULL) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }
    uint8_t type = (_type == MYSQL_RSP_UNKNOWN) ? p[mysql_header_size] : (uint8_t)_type;
    if (is_auth && type != 0x00 && type != 0xFF) {
        _type = MYSQL_RSP_AUTH;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, _data.auth));
        MY_PARSE_CHECK(_data.auth->Parse(buf, arena));
        return PARSE_OK;
    }
    if (type == 0x00 && !is_prepare) {
        _type = MYSQL_RSP_OK;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, _data.ok));
        MY_PARSE_CHECK(_data.ok->Parse(buf, arena));
        *more_results = _data.ok->status() & MYSQL_SERVER_MORE_RESULTS_EXISTS;
    } else if (type == 0xFF) {
        _type = MYSQL_RSP_ERROR;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, _data.error));
        MY_PARSE_CHECK(_data.error->Parse(buf, arena));
    } else if (type == 0xFE) {
        _type = MYSQL_RSP_EOF;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, _data.eof));
        MY_PARSE_CHECK(_data.eof->Parse(buf));
        *more_results = _data.eof->status() & MYSQL_SERVER_MORE_RESULTS_EXISTS;
    } else if (type >= 0x01 && type <= 0xFA) {
        _type = MYSQL_RSP_RESULTSET;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, _data.result_set));
        MY_PARSE_CHECK(_data.result_set->Parse(buf, arena, is_prepare));
        *more_results = _data.result_set->_eof2.status() & MYSQL_SERVER_MORE_RESULTS_EXISTS;
    } else if (type == 0x00) {
        _type = MYSQL_RSP_PREPARE_OK;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, _data.prepare_ok));
        MY_PARSE_CHECK(_data.prepare_ok->Parse(buf, arena));
    } else {
        LOG(ERROR) << "Unknown Response Type";
        return PARSE_ERROR_ABSOLUTELY_WRONG;
    }
    return PARSE_OK;
}

void MysqlReply::Print(std::ostream& os) const {
    if (_type == MYSQL_RSP_AUTH) {
        const Auth& auth = *_data.auth;
        os << "\nprotocol:" << (unsigned)auth._protocol << "\nversion:" << auth._version
           << "\nthread_id:" << auth._thread_id << "\nsalt:" << auth._salt
           << "\ncapacity:" << auth._capability << "\nlanguage:" << (unsigned)auth._language
           << "\nstatus:" << auth._status << "\nextended_capacity:" << auth._extended_capability
           << "\nauth_plugin_length:" << auth._auth_plugin_length << "\nsalt2:" << auth._salt2
           << "\nauth_plugin:" << auth._auth_plugin;
    } else if (_type == MYSQL_RSP_OK) {
        const Ok& ok = *_data.ok;
        os << "\naffect_row:" << ok._affect_row << "\nindex:" << ok._index
           << "\nstatus:" << ok._status << "\nwarning:" << ok._warning << "\nmessage:" << ok._msg;
    } else if (_type == MYSQL_RSP_ERROR) {
        const Error& err = *_data.error;
        os << "\nerrcode:" << err._errcode << "\nstatus:" << err._status
           << "\nmessage:" << err._msg;
    } else if (_type == MYSQL_RSP_RESULTSET) {
        const ResultSet& r = *_data.result_set;
        os << "\nheader.column_number:" << r._header._column_number;
        for (uint64_t i = 0; i < r._header._column_number; ++i) {
            os << "\ncolumn[" << i << "].catalog:" << r._columns[i]._catalog << "\ncolumn[" << i
               << "].database:" << r._columns[i]._database << "\ncolumn[" << i
               << "].table:" << r._columns[i]._table << "\ncolumn[" << i
               << "].origin_table:" << r._columns[i]._origin_table << "\ncolumn[" << i
               << "].name:" << r._columns[i]._name << "\ncolumn[" << i
               << "].origin_name:" << r._columns[i]._origin_name << "\ncolumn[" << i
               << "].collation:" << (uint16_t)r._columns[i]._collation << "\ncolumn[" << i
               << "].length:" << r._columns[i]._length << "\ncolumn[" << i
               << "].type:" << (unsigned)r._columns[i]._type << "\ncolumn[" << i
               << "].flag:" << (unsigned)r._columns[i]._flag << "\ncolumn[" << i
               << "].decimal:" << (unsigned)r._columns[i]._decimal;
        }
        os << "\neof1.warning:" << r._eof1._warning;
        os << "\neof1.status:" << r._eof1._status;
        int n = 0;
        for (const Row* row = r._first->_next; row != r._last->_next; row = row->_next) {
            os << "\nrow(" << n++ << "):";
            for (uint64_t j = 0; j < r._header._column_number; ++j) {
                if (row->field(j).is_nil()) {
                    os << "NULL\t";
                    continue;
                }
                switch (row->field(j)._type) {
                    case MYSQL_FIELD_TYPE_TINY:
                        if (r._columns[j]._flag & MYSQL_UNSIGNED_FLAG) {
                            os << row->field(j).tiny();
                        } else {
                            os << row->field(j).stiny();
                        }
                        break;
                    case MYSQL_FIELD_TYPE_SHORT:
                    case MYSQL_FIELD_TYPE_YEAR:
                        if (r._columns[j]._flag & MYSQL_UNSIGNED_FLAG) {
                            os << row->field(j).small();
                        } else {
                            os << row->field(j).ssmall();
                        }
                        break;
                    case MYSQL_FIELD_TYPE_INT24:
                    case MYSQL_FIELD_TYPE_LONG:
                        if (r._columns[j]._flag & MYSQL_UNSIGNED_FLAG) {
                            os << row->field(j).integer();
                        } else {
                            os << row->field(j).sinteger();
                        }
                        break;
                    case MYSQL_FIELD_TYPE_LONGLONG:
                        if (r._columns[j]._flag & MYSQL_UNSIGNED_FLAG) {
                            os << row->field(j).bigint();
                        } else {
                            os << row->field(j).sbigint();
                        }
                        break;
                    case MYSQL_FIELD_TYPE_FLOAT:
                        os << row->field(j).float32();
                        break;
                    case MYSQL_FIELD_TYPE_DOUBLE:
                        os << row->field(j).float64();
                        break;
                    case MYSQL_FIELD_TYPE_DECIMAL:
                    case MYSQL_FIELD_TYPE_NEWDECIMAL:
                    case MYSQL_FIELD_TYPE_VARCHAR:
                    case MYSQL_FIELD_TYPE_BIT:
                    case MYSQL_FIELD_TYPE_ENUM:
                    case MYSQL_FIELD_TYPE_SET:
                    case MYSQL_FIELD_TYPE_TINY_BLOB:
                    case MYSQL_FIELD_TYPE_MEDIUM_BLOB:
                    case MYSQL_FIELD_TYPE_LONG_BLOB:
                    case MYSQL_FIELD_TYPE_BLOB:
                    case MYSQL_FIELD_TYPE_VAR_STRING:
                    case MYSQL_FIELD_TYPE_STRING:
                    case MYSQL_FIELD_TYPE_GEOMETRY:
                    case MYSQL_FIELD_TYPE_JSON:
                    case MYSQL_FIELD_TYPE_TIME:
                    case MYSQL_FIELD_TYPE_DATE:
                    case MYSQL_FIELD_TYPE_NEWDATE:
                    case MYSQL_FIELD_TYPE_TIMESTAMP:
                    case MYSQL_FIELD_TYPE_DATETIME:
                        os << row->field(j).string();
                        break;
                    case MYSQL_FIELD_TYPE_NULL:
                        os << "NULL";
                        break;
                    default:
                        os << "Unknown field type";
                }
                os << "\t";
            }
        }
        os << "\neof2.warning:" << r._eof2._warning;
        os << "\neof2.status:" << r._eof2._status;
    } else if (_type == MYSQL_RSP_EOF) {
        const Eof& e = *_data.eof;
        os << "\nwarning:" << e._warning << "\nstatus:" << e._status;
    } else if (_type == MYSQL_RSP_PREPARE_OK) {
        const PrepareOk& prep = *_data.prepare_ok;
        os << "\nstmt_id:" << prep._stmt_id << "\ncolumn_number:" << prep._column_number
           << "\nparam_number:" << prep._param_number;
        for (uint16_t i = 0; i < prep._param_number; ++i) {
            os << "\nparam[" << i << "].catalog:" << prep._params[i]._catalog << "\nparam[" << i
               << "].database:" << prep._params[i]._database << "\nparam[" << i
               << "].table:" << prep._params[i]._table << "\nparam[" << i
               << "].origin_table:" << prep._params[i]._origin_table << "\nparam[" << i
               << "].name:" << prep._params[i]._name << "\nparam[" << i
               << "].origin_name:" << prep._params[i]._origin_name << "\nparam[" << i
               << "].collation:" << (uint16_t)prep._params[i]._collation << "\nparam[" << i
               << "].length:" << prep._params[i]._length << "\nparam[" << i
               << "].type:" << (unsigned)prep._params[i]._type << "\nparam[" << i
               << "].flag:" << (unsigned)prep._params[i]._flag << "\nparam[" << i
               << "].decimal:" << (unsigned)prep._params[i]._decimal;
        }
        for (uint16_t i = 0; i < prep._column_number; ++i) {
            os << "\ncolumn[" << i << "].catalog:" << prep._columns[i]._catalog << "\ncolumn[" << i
               << "].database:" << prep._columns[i]._database << "\ncolumn[" << i
               << "].table:" << prep._columns[i]._table << "\ncolumn[" << i
               << "].origin_table:" << prep._columns[i]._origin_table << "\ncolumn[" << i
               << "].name:" << prep._columns[i]._name << "\ncolumn[" << i
               << "].origin_name:" << prep._columns[i]._origin_name << "\ncolumn[" << i
               << "].collation:" << (uint16_t)prep._columns[i]._collation << "\ncolumn[" << i
               << "].length:" << prep._columns[i]._length << "\ncolumn[" << i
               << "].type:" << (unsigned)prep._columns[i]._type << "\ncolumn[" << i
               << "].flag:" << (unsigned)prep._columns[i]._flag << "\ncolumn[" << i
               << "].decimal:" << (unsigned)prep._columns[i]._decimal;
        }
    } else {
        os << "Unknown response type";
    }
}

ParseError MysqlReply::Auth::Parse(butil::IOBuf& buf, butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    const std::string delim(1, 0x00);
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }
    buf.cut1((char*)&_protocol);
    {
        butil::IOBuf version;
        buf.cut_until(&version, delim);
        char* d = NULL;
        MY_ALLOC_CHECK(my_alloc_check(arena, version.size(), d));
        version.copy_to(d);
        _version.set(d, version.size());
    }
    {
        uint8_t tmp[4];
        buf.cutn(tmp, sizeof(tmp));
        _thread_id = mysql_uint4korr(tmp);
    }
    {
        butil::IOBuf salt;
        buf.cut_until(&salt, delim);
        char* d = NULL;
        MY_ALLOC_CHECK(my_alloc_check(arena, salt.size(), d));
        salt.copy_to(d);
        _salt.set(d, salt.size());
    }
    {
        uint8_t tmp[2];
        buf.cutn(&tmp, sizeof(tmp));
        _capability = mysql_uint2korr(tmp);
    }
    buf.cut1((char*)&_language);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _status = mysql_uint2korr(tmp);
    }
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _extended_capability = mysql_uint2korr(tmp);
    }
    buf.cut1((char*)&_auth_plugin_length);
    buf.pop_front(10);
    {
        butil::IOBuf salt2;
        buf.cut_until(&salt2, delim);
        char* d = NULL;
        MY_ALLOC_CHECK(my_alloc_check(arena, salt2.size(), d));
        salt2.copy_to(d);
        _salt2.set(d, salt2.size());
    }
    {
        char* d = NULL;
        MY_ALLOC_CHECK(my_alloc_check(arena, _auth_plugin_length, d));
        buf.cutn(d, _auth_plugin_length);
        _auth_plugin.set(d, _auth_plugin_length);
    }
    buf.clear();  // consume all buf
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::ResultSetHeader::Parse(butil::IOBuf& buf) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }
    uint64_t old_size, new_size;
    old_size = buf.size();
    _column_number = parse_encode_length(buf);
    new_size = buf.size();
    if (old_size - new_size < header.payload_size) {
        _extra_msg = parse_encode_length(buf);
    } else {
        _extra_msg = 0;
    }
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Column::Parse(butil::IOBuf& buf, butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }

    uint64_t len = parse_encode_length(buf);
    char* catalog = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, catalog));
    buf.cutn(catalog, len);
    _catalog.set(catalog, len);

    len = parse_encode_length(buf);
    char* database = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, database));
    buf.cutn(database, len);
    _database.set(database, len);

    len = parse_encode_length(buf);
    char* table = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, table));
    buf.cutn(table, len);
    _table.set(table, len);

    len = parse_encode_length(buf);
    char* origin_table = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, origin_table));
    buf.cutn(origin_table, len);
    _origin_table.set(origin_table, len);

    len = parse_encode_length(buf);
    char* name = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, name));
    buf.cutn(name, len);
    _name.set(name, len);

    len = parse_encode_length(buf);
    char* origin_name = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, origin_name));
    buf.cutn(origin_name, len);
    _origin_name.set(origin_name, len);
    buf.pop_front(1);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _collation = (MysqlCollation)mysql_uint2korr(tmp);
    }
    {
        uint8_t tmp[4];
        buf.cutn(tmp, sizeof(tmp));
        _length = mysql_uint4korr(tmp);
    }
    buf.cut1((char*)&_type);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _flag = (MysqlFieldFlag)mysql_uint2korr(tmp);
    }
    buf.cut1((char*)&_decimal);
    buf.pop_front(2);
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Ok::Parse(butil::IOBuf& buf, butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }

    uint64_t old_size, new_size;
    old_size = buf.size();
    buf.pop_front(1);

    _affect_row = parse_encode_length(buf);
    _index = parse_encode_length(buf);
    buf.cutn(&_status, 2);
    buf.cutn(&_warning, 2);

    new_size = buf.size();
    if (old_size - new_size < header.payload_size) {
        const int64_t len = header.payload_size - (old_size - new_size);
        char* msg = NULL;
        MY_ALLOC_CHECK(my_alloc_check(arena, len, msg));
        buf.cutn(msg, len);
        _msg.set(msg, len);
        // buf.pop_front(1);  // Null
    }
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Eof::Parse(butil::IOBuf& buf) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }
    buf.pop_front(1);
    buf.cutn(&_warning, 2);
    buf.cutn(&_status, 2);
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Error::Parse(butil::IOBuf& buf, butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }
    buf.pop_front(1);  // 0xFF
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _errcode = mysql_uint2korr(tmp);
    }
    buf.pop_front(1);  // '#'
    // 5 byte server status
    char* status = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, 5, status));
    buf.cutn(status, 5);
    _status.set(status, 5);
    // error message, Null-Terminated string
    uint64_t len = header.payload_size - 9;
    char* msg = NULL;
    MY_ALLOC_CHECK(my_alloc_check(arena, len, msg));
    buf.cutn(msg, len);
    _msg.set(msg, len);
    // buf.pop_front(1);  // Null
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Row::Parse(butil::IOBuf& buf,
                                  const MysqlReply::Column* columns,
                                  uint64_t column_number,
                                  MysqlReply::Field* fields,
                                  bool binary,
                                  butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }
    if (!binary) {  // mysql text protocol
        for (uint64_t i = 0; i < column_number; ++i) {
            MY_PARSE_CHECK(fields[i].Parse(buf, columns + i, arena));
        }
    } else {  // mysql binary protocol
        uint8_t header = 0;
        buf.cut1((char*)&header);
        if (header != 0x00) {
            return PARSE_ERROR_ABSOLUTELY_WRONG;
        }
        // NULL-bitmap, [(column-count + 7 + 2) / 8 bytes]
        const uint64_t size = ((column_number + 7 + 2) >> 3);
        uint8_t null_mask[size];
        for (size_t i = 0; i < sizeof(null_mask); ++i) {
            null_mask[i] = 0;
        }
        buf.cutn(null_mask, size);
        for (uint64_t i = 0; i < column_number; ++i) {
            MY_PARSE_CHECK(fields[i].Parse(buf, columns + i, i, column_number, null_mask, arena));
        }
    }
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Field::Parse(butil::IOBuf& buf,
                                    const MysqlReply::Column* column,
                                    butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    // field type
    _type = column->_type;
    // is unsigned flag set
    _unsigned = column->_flag & MYSQL_UNSIGNED_FLAG;
    // parse encode length
    const uint64_t len = parse_encode_length(buf);
    // is it null?
    if (len == 0 && !(column->_flag & MYSQL_NOT_NULL_FLAG)) {
        _is_nil = true;
        set_parsed();
        return PARSE_OK;
    }
    // field is not null
    butil::IOBuf str;
    buf.cutn(&str, len);
    switch (_type) {
        case MYSQL_FIELD_TYPE_TINY:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.tiny;
            } else {
                std::istringstream(str.to_string()) >> _data.stiny;
            }
            break;
        case MYSQL_FIELD_TYPE_SHORT:
        case MYSQL_FIELD_TYPE_YEAR:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.small;
            } else {
                std::istringstream(str.to_string()) >> _data.ssmall;
            }
            break;
        case MYSQL_FIELD_TYPE_INT24:
        case MYSQL_FIELD_TYPE_LONG:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.integer;
            } else {
                std::istringstream(str.to_string()) >> _data.sinteger;
            }
            break;
        case MYSQL_FIELD_TYPE_LONGLONG:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                std::istringstream(str.to_string()) >> _data.bigint;
            } else {
                std::istringstream(str.to_string()) >> _data.sbigint;
            }
            break;
        case MYSQL_FIELD_TYPE_FLOAT:
            std::istringstream(str.to_string()) >> _data.float32;
            break;
        case MYSQL_FIELD_TYPE_DOUBLE:
            std::istringstream(str.to_string()) >> _data.float64;
            break;
        case MYSQL_FIELD_TYPE_DECIMAL:
        case MYSQL_FIELD_TYPE_NEWDECIMAL:
        case MYSQL_FIELD_TYPE_VARCHAR:
        case MYSQL_FIELD_TYPE_BIT:
        case MYSQL_FIELD_TYPE_ENUM:
        case MYSQL_FIELD_TYPE_SET:
        case MYSQL_FIELD_TYPE_TINY_BLOB:
        case MYSQL_FIELD_TYPE_MEDIUM_BLOB:
        case MYSQL_FIELD_TYPE_LONG_BLOB:
        case MYSQL_FIELD_TYPE_BLOB:
        case MYSQL_FIELD_TYPE_VAR_STRING:
        case MYSQL_FIELD_TYPE_STRING:
        case MYSQL_FIELD_TYPE_GEOMETRY:
        case MYSQL_FIELD_TYPE_JSON:
        case MYSQL_FIELD_TYPE_TIME:
        case MYSQL_FIELD_TYPE_DATE:
        case MYSQL_FIELD_TYPE_NEWDATE:
        case MYSQL_FIELD_TYPE_TIMESTAMP:
        case MYSQL_FIELD_TYPE_DATETIME: {
            char* d = NULL;
            MY_ALLOC_CHECK(my_alloc_check(arena, len, d));
            str.copy_to(d);
            _data.str.set(d, len);
        } break;
        default:
            LOG(ERROR) << "Unknown field type";
            set_parsed();
            return PARSE_ERROR_ABSOLUTELY_WRONG;
    }
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::Field::Parse(butil::IOBuf& buf,
                                    const MysqlReply::Column* column,
                                    uint64_t column_index,
                                    uint64_t column_number,
                                    const uint8_t* null_mask,
                                    butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    // field type
    _type = column->_type;
    // is unsigned flag set
    _unsigned = column->_flag & MYSQL_UNSIGNED_FLAG;

    if (((null_mask[(column_index + 2) >> 3] >> (column_index + 2) & 7) & 1) == 1) {
        _is_nil = true;
        set_parsed();
        return PARSE_OK;
    }

    switch (_type) {
        case MYSQL_FIELD_TYPE_TINY:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                buf.cut1((char*)&_data.tiny);
            } else {
                buf.cut1((char*)&_data.stiny);
            }
            break;
        case MYSQL_FIELD_TYPE_SHORT:
        case MYSQL_FIELD_TYPE_YEAR:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                uint8_t* p = (uint8_t*)&_data.small;
                buf.cutn(p, 2);
                _data.small = mysql_uint2korr(p);
            } else {
                uint8_t* p = (uint8_t*)&_data.ssmall;
                buf.cutn(p, 2);
                _data.ssmall = (int16_t)mysql_uint2korr(p);
            }
            break;
        case MYSQL_FIELD_TYPE_INT24:
        case MYSQL_FIELD_TYPE_LONG:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                uint8_t* p = (uint8_t*)&_data.integer;
                buf.cutn(p, 4);
                _data.integer = mysql_uint4korr(p);
            } else {
                uint8_t* p = (uint8_t*)&_data.sinteger;
                buf.cutn(p, 4);
                _data.sinteger = (int32_t)mysql_uint4korr(p);
            }
            break;
        case MYSQL_FIELD_TYPE_LONGLONG:
            if (column->_flag & MYSQL_UNSIGNED_FLAG) {
                uint8_t* p = (uint8_t*)&_data.bigint;
                buf.cutn(p, 8);
                _data.bigint = mysql_uint8korr(p);
            } else {
                uint8_t* p = (uint8_t*)&_data.sbigint;
                buf.cutn(p, 8);
                _data.sbigint = (int64_t)mysql_uint8korr(p);
            }
            break;
        case MYSQL_FIELD_TYPE_FLOAT: {
            uint8_t* p = (uint8_t*)&_data.float32;
            buf.cutn(p, 4);
            _data.float32 = (float)mysql_uint4korr(p);
        } break;
        case MYSQL_FIELD_TYPE_DOUBLE: {
            uint8_t* p = (uint8_t*)&_data.float64;
            buf.cutn(p, 8);
            _data.float64 = (double)mysql_uint8korr(p);
        } break;
        case MYSQL_FIELD_TYPE_DECIMAL:
        case MYSQL_FIELD_TYPE_NEWDECIMAL:
        case MYSQL_FIELD_TYPE_VARCHAR:
        case MYSQL_FIELD_TYPE_BIT:
        case MYSQL_FIELD_TYPE_ENUM:
        case MYSQL_FIELD_TYPE_SET:
        case MYSQL_FIELD_TYPE_TINY_BLOB:
        case MYSQL_FIELD_TYPE_MEDIUM_BLOB:
        case MYSQL_FIELD_TYPE_LONG_BLOB:
        case MYSQL_FIELD_TYPE_BLOB:
        case MYSQL_FIELD_TYPE_VAR_STRING:
        case MYSQL_FIELD_TYPE_STRING:
        case MYSQL_FIELD_TYPE_GEOMETRY:
        case MYSQL_FIELD_TYPE_JSON:
        case MYSQL_FIELD_TYPE_TIME:
        case MYSQL_FIELD_TYPE_DATE:
        case MYSQL_FIELD_TYPE_NEWDATE:
        case MYSQL_FIELD_TYPE_TIMESTAMP:
        case MYSQL_FIELD_TYPE_DATETIME: {
            const uint64_t len = parse_encode_length(buf);
            // is it null?
            if (len == 0 && !(column->_flag & MYSQL_NOT_NULL_FLAG)) {
                _is_nil = true;
                set_parsed();
                return PARSE_OK;
            }
            // field is not null
            char* d = NULL;
            MY_ALLOC_CHECK(my_alloc_check(arena, len, d));
            buf.cutn(d, len);
            _data.str.set(d, len);
        } break;
        default:
            LOG(ERROR) << "Unknown field type";
            set_parsed();
            return PARSE_ERROR_ABSOLUTELY_WRONG;
    }
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::ResultSet::Parse(butil::IOBuf& buf, butil::Arena* arena, bool binary) {
    if (is_parsed()) {
        return PARSE_OK;
    }
    // parse header
    MY_PARSE_CHECK(_header.Parse(buf));
    // parse colunms
    MY_ALLOC_CHECK(my_alloc_check(arena, _header._column_number, _columns));
    for (uint64_t i = 0; i < _header._column_number; ++i) {
        MY_PARSE_CHECK(_columns[i].Parse(buf, arena));
    }
    // parse eof1
    MY_PARSE_CHECK(_eof1.Parse(buf));
    // parse row
    std::vector<Row*> rows;
    for (;;) {
        // if not full package reread
        if (!is_full_package(buf)) {
            return PARSE_ERROR_NOT_ENOUGH_DATA;
        }
        // if eof break loops for row
        if (is_an_eof(buf)) {
            break;
        }
        // allocate memory for row and fields
        Row* row = NULL;
        Field* fields = NULL;
        MY_ALLOC_CHECK(my_alloc_check(arena, 1, row));
        MY_ALLOC_CHECK(my_alloc_check(arena, _header._column_number, fields));
        row->_fields = fields;
        row->_field_number = _header._column_number;
        _last->_next = row;
        _last = row;
        // parse row and fields
        MY_PARSE_CHECK(row->Parse(buf, _columns, _header._column_number, fields, binary, arena));
        // add row number
        ++_row_number;
    }
    // parse eof2
    MY_PARSE_CHECK(_eof2.Parse(buf));
    set_parsed();
    return PARSE_OK;
}

ParseError MysqlReply::PrepareOk::Parse(butil::IOBuf& buf, butil::Arena* arena) {
    if (is_parsed()) {
        return PARSE_OK;
    }

    MysqlHeader header;
    if (!parse_header(buf, &header)) {
        return PARSE_ERROR_NOT_ENOUGH_DATA;
    }

    buf.pop_front(1);
    {
        uint8_t tmp[4];
        buf.cutn(tmp, sizeof(tmp));
        _stmt_id = mysql_uint4korr(tmp);
    }
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _column_number = mysql_uint2korr(tmp);
    }
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _param_number = mysql_uint2korr(tmp);
    }
    buf.pop_front(1);
    {
        uint8_t tmp[2];
        buf.cutn(tmp, sizeof(tmp));
        _warning = mysql_uint2korr(tmp);
    }

    if (_param_number > 0) {
        MY_ALLOC_CHECK(my_alloc_check(arena, _param_number, _params));
        for (uint16_t i = 0; i < _param_number; ++i) {
            MY_PARSE_CHECK(_params[i].Parse(buf, arena));
        }
        Eof eof;
        eof.Parse(buf);
    }

    if (_column_number > 0) {
        MY_ALLOC_CHECK(my_alloc_check(arena, _column_number, _columns));
        for (uint16_t i = 0; i < _column_number; ++i) {
            MY_PARSE_CHECK(_columns[i].Parse(buf, arena));
        }
        Eof eof;
        eof.Parse(buf);
    }

    set_parsed();
    return PARSE_OK;
}

}  // namespace brpc
