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

// Date: Wed Nov 27 12:59:20 CST 2013

// This closed addressing hash-map puts first linked node in bucket array
// directly to save an extra memory indirection. As a result, this map yields
// close performance to raw array on nearly all operations, probably being the
// fastest hashmap for small-sized key/value ever.
//
// Performance comparisons between several maps:
//  [ value = 8 bytes ]
//  Sequentially inserting 100 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 11/108/55/58
//  Sequentially erasing 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 7/123/55/37
//  Sequentially inserting 1000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 10/92/55/54
//  Sequentially erasing 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/67/51/35
//  Sequentially inserting 10000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 10/100/66/54
//  Sequentially erasing 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/72/55/35
//  [ value = 32 bytes ]
//  Sequentially inserting 100 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 14/108/56/56
//  Sequentially erasing 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/77/53/38
//  Sequentially inserting 1000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 14/94/54/53
//  Sequentially erasing 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/66/49/36
//  Sequentially inserting 10000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 13/106/62/54
//  Sequentially erasing 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/69/53/36
//  [ value = 128 bytes ]
//  Sequentially inserting 100 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 31/182/96/96
//  Sequentially erasing 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 8/117/51/44
//  Sequentially inserting 1000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 29/191/100/97
//  Sequentially erasing 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/100/49/44
//  Sequentially inserting 10000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 30/184/113/114
//  Sequentially erasing 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/99/52/43
//  [ value = 8 bytes ]
//  Randomly inserting 100 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 11/171/108/60
//  Randomly erasing 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 8/158/126/37
//  Randomly inserting 1000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 10/159/117/54
//  Randomly erasing 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/153/135/36
//  Randomly inserting 10000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 12/223/180/55
//  Randomly erasing 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 7/237/210/48
//  [ value = 32 bytes ]
//  Randomly inserting 100 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 16/179/108/57
//  Randomly erasing 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 5/157/120/38
//  Randomly inserting 1000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 15/168/127/54
//  Randomly erasing 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 5/164/135/39
//  Randomly inserting 10000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 19/241/201/56
//  Randomly erasing 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 5/235/218/54
//  [ value = 128 bytes ]
//  Randomly inserting 100 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 35/242/154/97
//  Randomly erasing 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 7/185/119/56
//  Randomly inserting 1000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 35/262/182/99
//  Randomly erasing 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/215/157/66
//  Randomly inserting 10000 into FlatMap/std::map/butil::PooledMap/butil::hash_map takes 44/330/278/114
//  Randomly erasing 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/307/242/90
//  [ value = 8 bytes ]
//  Seeking 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 6/51/52/13
//  Seeking 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/98/82/14
//  Seeking 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/175/170/14
//  [ value = 32 bytes ]
//  Seeking 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/52/52/14
//  Seeking 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/84/82/13
//  Seeking 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/164/156/14
//  [ value = 128 bytes ]
//  Seeking 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/54/53/14
//  Seeking 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/88/90/13
//  Seeking 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/178/185/14
//  [ value = 8 bytes ]
//  Seeking 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 5/51/49/14
//  Seeking 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/86/94/14
//  Seeking 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/177/171/14
//  [ value = 32 bytes ]
//  Seeking 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/51/53/14
//  Seeking 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/98/82/13
//  Seeking 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/163/156/14
//  [ value = 128 bytes ]
//  Seeking 100 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 3/55/53/14
//  Seeking 1000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/88/89/13
//  Seeking 10000 from FlatMap/std::map/butil::PooledMap/butil::hash_map takes 4/177/185/14

#ifndef BUTIL_FLAT_MAP_H
#define BUTIL_FLAT_MAP_H

#include <stdint.h>
#include <functional>
#include <iostream>                               // std::ostream
#include "butil/type_traits.h"
#include "butil/logging.h"
#include "butil/find_cstr.h"
#include "butil/single_threaded_pool.h"            // SingleThreadedPool
#include "butil/containers/hash_tables.h"          // hash<>
#include "butil/bit_array.h"                       // bit_array_*
#include "butil/strings/string_piece.h"            // StringPiece

namespace butil {

template <typename _Map, typename _Element> class FlatMapIterator;
template <typename _Map, typename _Element> class SparseFlatMapIterator;
template <typename K, typename T> class FlatMapElement;
struct FlatMapVoid {}; // Replace void which is not constructible.
template <typename K> struct DefaultHasher;
template <typename K> struct DefaultEqualTo;

struct BucketInfo {
    size_t longest_length;
    double average_length;
};

// NOTE: Objects stored in FlatMap MUST be copyable.
template <typename _K, typename _T,
          // Compute hash code from key.
          // Use src/butil/third_party/murmurhash3 to make better distributions.
          typename _Hash = DefaultHasher<_K>,
          // Test equivalence between stored-key and passed-key.
          // stored-key is always on LHS, passed-key is always on RHS.
          typename _Equal = DefaultEqualTo<_K>,
          bool _Sparse = false>
class FlatMap {
public:
    typedef _K key_type;
    typedef _T mapped_type;
    typedef FlatMapElement<_K, _T> Element;
    typedef typename Element::value_type value_type;
    typedef typename conditional<
        _Sparse, SparseFlatMapIterator<FlatMap, value_type>,
        FlatMapIterator<FlatMap, value_type> >::type iterator;
    typedef typename conditional<
        _Sparse, SparseFlatMapIterator<FlatMap, const value_type>, 
        FlatMapIterator<FlatMap, const value_type> >::type const_iterator;
    typedef _Hash hasher;
    typedef _Equal key_equal;
    struct PositionHint {
        size_t nbucket;
        size_t offset;
        bool at_entry;
        key_type key;
    };
    
    FlatMap(const hasher& hashfn = hasher(), const key_equal& eql = key_equal());
    ~FlatMap();
    FlatMap(const FlatMap& rhs);    
    void operator=(const FlatMap& rhs);
    void swap(FlatMap & rhs);

    // Must be called to initialize this map, otherwise insert/operator[]
    // crashes, and seek/erase fails.
    // `nbucket' is the initial number of buckets. `load_factor' is the 
    // maximum value of size()*100/nbucket, if the value is reached, nbucket
    // will be doubled and all items stored will be rehashed which is costly.
    // Choosing proper values for these 2 parameters reduces costs.
    int init(size_t nbucket, u_int load_factor = 80);
    
    // Insert a pair of |key| and |value|. If size()*100/bucket_count() is 
    // more than load_factor(), a resize() will be done.
    // Returns address of the inserted value, NULL on error.
    mapped_type* insert(const key_type& key, const mapped_type& value);

    // Insert a pair of {key, value}. If size()*100/bucket_count() is
    // more than load_factor(), a resize() will be done.
    // Returns address of the inserted value, NULL on error.
    mapped_type* insert(const std::pair<key_type, mapped_type>& kv);

    // Remove |key| and the associated value
    // Returns: 1 on erased, 0 otherwise.
    // Remove all items. Allocated spaces are NOT returned by system.
    template <typename K2>
    size_t erase(const K2& key, mapped_type* old_value = NULL);

    void clear();

    // Remove all items and return all allocated spaces to system.
    void clear_and_reset_pool();
        
    // Search for the value associated with |key|
    // Returns: address of the value
    template <typename K2> mapped_type* seek(const K2& key) const;

    // Get the value associated with |key|. If |key| does not exist,
    // insert with a default-constructed value. If size()*100/bucket_count()
    // is more than load_factor, a resize will be done.
    // Returns reference of the value
    mapped_type& operator[](const key_type& key);

    // Resize this map. This is optional because resizing will be triggered by
    // insert() or operator[] if there're too many items.
    // Returns successful or not.
    bool resize(size_t nbucket);
    
    // Iterators
    iterator begin();
    iterator end();
    const_iterator begin() const;
    const_iterator end() const;

    // Iterate FlatMap inconsistently in more-than-one passes. This is used
    // in multi-threaded environment to divide the critical sections of
    // iterating big maps into smaller ones. "inconsistently" means that:
    //  * elements added during iteration may be missed.
    //  * some elements may be iterated more than once.
    //  * iteration is restarted at beginning when the map is resized.
    // Example: (copying all keys in multi-threaded environment)
    //   LOCK;
    //   size_t n = 0;
    //   for (Map::const_iterator it = map.begin(); it != map.end(); ++it) {
    //     if (++n >= 256/*max iterated one pass*/) {
    //       Map::PositionHint hint;
    //       map.save_iterator(it, &hint);
    //       n = 0;
    //       UNLOCK;
    //       LOCK;
    //       it = map.restore_iterator(hint);
    //       if (it == map.begin()) { // resized
    //         keys->clear();
    //       }
    //       if (it == map.end()) {
    //         break;
    //       }
    //     }
    //     keys->push_back(it->first);
    //   }
    //   UNLOCK;
    void save_iterator(const const_iterator&, PositionHint*) const;
    const_iterator restore_iterator(const PositionHint&) const;

    // True if init() was successfully called.
    bool initialized() const { return _buckets != NULL; }

    bool empty() const { return _size == 0; }
    size_t size() const { return _size; }
    size_t bucket_count() const { return _nbucket; }
    u_int load_factor () const { return _load_factor; }

    // Returns #nodes of longest bucket in this map. This scans all buckets.
    BucketInfo bucket_info() const;

    struct Bucket {
        explicit Bucket(const _K& k) : next(NULL)
        { new (element_spaces) Element(k); }
        Bucket(const Bucket& other) : next(NULL)
        { new (element_spaces) Element(other.element()); }
        bool is_valid() const { return next != (const Bucket*)-1UL; }
        void set_invalid() { next = (Bucket*)-1UL; }
        // NOTE: Only be called when is_valid() is true.
        Element& element() {
            void* spaces = element_spaces; // Suppress strict-aliasing
            return *reinterpret_cast<Element*>(spaces);
        }
        const Element& element() const {
            const void* spaces = element_spaces;
            return *reinterpret_cast<const Element*>(spaces);
        }
        Bucket* next;
        char element_spaces[sizeof(Element)];
    };

private:
template <typename _Map, typename _Element> friend class FlatMapIterator;
template <typename _Map, typename _Element> friend class SparseFlatMapIterator;
    // True if buckets need to be resized before holding `size' elements.
    inline bool is_too_crowded(size_t size) const
    { return size * 100 >= _nbucket * _load_factor; }
        
    size_t _size;
    size_t _nbucket;
    Bucket* _buckets;
    uint64_t* _thumbnail;
    u_int _load_factor;
    hasher _hashfn;
    key_equal _eql;
    SingleThreadedPool<sizeof(Bucket), 1024, 16> _pool;
};

template <typename _K,
          typename _Hash = DefaultHasher<_K>,
          typename _Equal = DefaultEqualTo<_K>,
          bool _Sparse = false>
class FlatSet {
public:
    typedef FlatMap<_K, FlatMapVoid, _Hash, _Equal, _Sparse> Map;
    typedef typename Map::key_type key_type;
    typedef typename Map::value_type value_type;
    typedef typename Map::Bucket Bucket;
    typedef typename Map::iterator iterator;
    typedef typename Map::const_iterator const_iterator;
    typedef typename Map::hasher hasher;
    typedef typename Map::key_equal key_equal;
    
    FlatSet(const hasher& hashfn = hasher(), const key_equal& eql = key_equal())
        : _map(hashfn, eql) {}
    void swap(FlatSet & rhs) { _map.swap(rhs._map); }

    int init(size_t nbucket, u_int load_factor = 80)
    { return _map.init(nbucket, load_factor); }

    const void* insert(const key_type& key)
    { return _map.insert(key, FlatMapVoid()); }

    template <typename K2>
    size_t erase(const K2& key) { return _map.erase(key, NULL); }

    void clear() { return _map.clear(); }
    void clear_and_reset_pool() { return _map.clear_and_reset_pool(); }

    template <typename K2>
    const void* seek(const K2& key) const { return _map.seek(key); }

    bool resize(size_t nbucket) { return _map.resize(nbucket); }
    
    iterator begin() { return _map.begin(); }
    iterator end() { return _map.end(); }
    const_iterator begin() const { return _map.begin(); }
    const_iterator end() const { return _map.end(); }

    bool initialized() const { return _map.initialized(); }
    bool empty() const { return _map.empty(); }
    size_t size() const { return _map.size(); }
    size_t bucket_count() const { return _map.bucket_count(); }
    u_int load_factor () const { return _map.load_factor(); }
    BucketInfo bucket_info() const { return _map.bucket_info(); }

private:
    Map _map;
};

template <typename _K, typename _T,
          typename _Hash = DefaultHasher<_K>,
          typename _Equal = DefaultEqualTo<_K> >
class SparseFlatMap : public FlatMap<_K, _T, _Hash, _Equal, true> {
};

template <typename _K,
          typename _Hash = DefaultHasher<_K>,
          typename _Equal = DefaultEqualTo<_K> >
class SparseFlatSet : public FlatSet<_K, _Hash, _Equal, true> {
};

// Implement FlatMapElement
template <typename K, typename T>
class FlatMapElement {
public:
    typedef std::pair<const K, T> value_type;
    // NOTE: Have to initialize _value in this way which is treated by GCC
    // specially that _value is zeroized(POD) or constructed(non-POD). Other
    // methods do not work. For example, if we put _value into the std::pair
    // and do initialization by calling _pair(k, T()), _value will be copy
    // constructed from the defaultly constructed instance(not zeroized for
    // POD) which is wrong generally.
    explicit FlatMapElement(const K& k) : _key(k), _value(T()) {}
    //                                             ^^^^^^^^^^^
    const K& first_ref() const { return _key; }
    T& second_ref() { return _value; }
    value_type& value_ref() { return *reinterpret_cast<value_type*>(this); }
    inline static const K& first_ref_from_value(const value_type& v)
    { return v.first; }
    inline static const T& second_ref_from_value(const value_type& v)
    { return v.second; }
private:
    const K _key;
    T _value;
};

template <typename K>
class FlatMapElement<K, FlatMapVoid> {
public:
    typedef const K value_type;
    explicit FlatMapElement(const K& k) : _key(k) {}
    const K& first_ref() const { return _key; }
    FlatMapVoid& second_ref() { return second_ref_from_value(_key); }
    value_type& value_ref() { return _key; }
    inline static const K& first_ref_from_value(value_type& v) { return v; }
    inline static FlatMapVoid& second_ref_from_value(value_type&) {
        static FlatMapVoid dummy;
        return dummy;
    }
private:
    K _key;
};

// Implement DefaultHasher and DefaultEqualTo
template <typename K>
struct DefaultHasher : public BUTIL_HASH_NAMESPACE::hash<K> {
};

template <>
struct DefaultHasher<std::string> {
    std::size_t operator()(const butil::StringPiece& s) const {
        std::size_t result = 0;
        for (butil::StringPiece::const_iterator
                 i = s.begin(); i != s.end(); ++i) {
            result = result * 101 + *i;
        }
        return result;
    }
    std::size_t operator()(const char* s) const {
        std::size_t result = 0;
        for (; *s; ++s) {
            result = result * 101 + *s;
        }
        return result;
    }
    std::size_t operator()(const std::string& s) const {
        std::size_t result = 0;
        for (std::string::const_iterator i = s.begin(); i != s.end(); ++i) {
            result = result * 101 + *i;
        }
        return result;        
    }
};

template <typename K>
struct DefaultEqualTo : public std::equal_to<K> {
};

template <>
struct DefaultEqualTo<std::string> {
    bool operator()(const std::string& s1, const std::string& s2) const
    { return s1 == s2; }
    bool operator()(const std::string& s1, const butil::StringPiece& s2) const
    { return s1 == s2; }
    bool operator()(const std::string& s1, const char* s2) const
    { return s1 == s2; }
};

// find_cstr and find_lowered_cstr
template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
const _T* find_cstr(const FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
                    const char* key) {
    return m.seek(key);
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
_T* find_cstr(FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
              const char* key) {
    return m.seek(key);
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
const _T* find_cstr(const FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
                    const char* key, size_t length) {
    return m.seek(butil::StringPiece(key, length));
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
_T* find_cstr(FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
              const char* key, size_t length) {
    return m.seek(butil::StringPiece(key, length));
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
const _T* find_lowered_cstr(
    const FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
    const char* key) {
    return m.seek(*tls_stringmap_temp.get_lowered_string(key));
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
_T* find_lowered_cstr(FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
                      const char* key) {
    return m.seek(*tls_stringmap_temp.get_lowered_string(key));
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
const _T* find_lowered_cstr(
    const FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
    const char* key, size_t length) {
    return m.seek(*tls_stringmap_temp.get_lowered_string(key, length));
}

template <typename _T, typename _Hash, typename _Equal, bool _Sparse>
_T* find_lowered_cstr(FlatMap<std::string, _T, _Hash, _Equal, _Sparse>& m,
                      const char* key, size_t length) {
    return m.seek(*tls_stringmap_temp.get_lowered_string(key, length));
}

}  // namespace butil

#include "butil/containers/flat_map_inl.h"

#endif  //BUTIL_FLAT_MAP_H
