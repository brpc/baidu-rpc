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

#ifndef BUTIL_FLAT_MAP_INL_H
#define BUTIL_FLAT_MAP_INL_H

namespace butil {

inline uint32_t find_next_prime(uint32_t nbucket) {
    static const unsigned long prime_list[] = {
        29ul, 
        53ul,         97ul,         193ul,       389ul,       769ul,
        1543ul,       3079ul,       6151ul,      12289ul,     24593ul,
        49157ul,      98317ul,      196613ul,    393241ul,    786433ul,
        1572869ul,    3145739ul,    6291469ul,   12582917ul,  25165843ul,
        50331653ul,   100663319ul,  201326611ul, 402653189ul, 805306457ul,
        1610612741ul, 3221225473ul, 4294967291ul
    };
    const size_t nprimes = sizeof(prime_list) / sizeof(prime_list[0]);
    for (size_t i = 0; i < nprimes; i++) {
        if (nbucket <= prime_list[i]) {
            return prime_list[i];
        }
    }
    return nbucket;
}

inline uint64_t find_power2(uint64_t b) {
    b -= 1;
    b |= (b >> 1);
    b |= (b >> 2);
    b |= (b >> 4);
    b |= (b >> 8);
    b |= (b >> 16);
    b |= (b >> 32);
    return b + 1;
}

// Using next prime is slower for 10ns on average (due to %). If quality of
// the hash code is good enough, primeness of nbucket is not important. We
// choose to trust the hash code (or user should use a better hash algorithm
// when the collisions are significant) and still stick to round-to-power-2
// solution right now.
inline size_t flatmap_round(size_t nbucket) {
#ifdef FLAT_MAP_ROUND_BUCKET_BY_USE_NEXT_PRIME    
    return find_next_prime(nbucket);
#else
    return find_power2(nbucket);
#endif
}

inline size_t flatmap_mod(size_t hash_code, size_t nbucket) {
#ifdef FLAT_MAP_ROUND_BUCKET_BY_USE_NEXT_PRIME
    return hash_code % nbucket;
#else
    return hash_code & (nbucket - 1);
#endif
}

// Iterate FlatMap
template <typename Map, typename Value> class FlatMapIterator {
public:
    typedef Value value_type;
    typedef Value& reference;
    typedef Value* pointer;
    typedef typename add_const<Value>::type ConstValue;
    typedef ConstValue& const_reference;
    typedef ConstValue* const_pointer;
    typedef std::forward_iterator_tag iterator_category;
    typedef ptrdiff_t difference_type;
    typedef typename remove_const<Value>::type NonConstValue;
    
    FlatMapIterator() : _node(NULL), _entry(NULL) {}    
    FlatMapIterator(const Map* map, size_t pos) {
        if (map->initialized()) {
            _entry = map->_buckets + pos;
            find_and_set_valid_node();
        } else {
            _node = NULL;
            _entry = NULL;
        }
    }
    FlatMapIterator(const FlatMapIterator<Map, NonConstValue>& rhs)
        : _node(rhs._node), _entry(rhs._entry) {}
    ~FlatMapIterator() {}  // required by style-checker
    
    // *this == rhs
    bool operator==(const FlatMapIterator& rhs) const
    { return _node == rhs._node; }

    // *this != rhs
    bool operator!=(const FlatMapIterator& rhs) const
    { return _node != rhs._node; }
        
    // ++ it
    FlatMapIterator& operator++() {
        if (NULL == _node->next) {
            ++_entry;
            find_and_set_valid_node();
        } else {
            _node = _node->next;
        }
        return *this;
    }

    // it ++
    FlatMapIterator operator++(int) {
        FlatMapIterator tmp = *this;
        this->operator++();
        return tmp;
    }

    reference operator*() { return _node->element().value_ref(); }
    pointer operator->() { return &_node->element().value_ref(); }
    const_reference operator*() const { return _node->element().value_ref(); }
    const_pointer operator->() const { return &_node->element().value_ref(); }

private:
friend class FlatMapIterator<Map, ConstValue>;
friend class FlatMap<typename Map::key_type, typename Map::mapped_type,
                     typename Map::hasher, typename Map::key_equal>;

    void find_and_set_valid_node() {
        for (; !_entry->is_valid(); ++_entry);
        _node = _entry;
    }
  
    typename Map::Bucket* _node;
    typename Map::Bucket* _entry;
};

// Iterate SparseFlatMap
template <typename Map, typename Value> class SparseFlatMapIterator {
public:
    typedef Value value_type;
    typedef Value& reference;
    typedef Value* pointer;
    typedef typename add_const<Value>::type ConstValue;
    typedef ConstValue& const_reference;
    typedef ConstValue* const_pointer;
    typedef std::forward_iterator_tag iterator_category;
    typedef ptrdiff_t difference_type;
    typedef typename remove_const<Value>::type NonConstValue;
    
    SparseFlatMapIterator() : _node(NULL), _pos(0), _map(NULL) {}
    SparseFlatMapIterator(const Map* map, size_t pos) {
        if (map->initialized()) {
            _map = map;
            _pos = pos;
            find_and_set_valid_node();
        } else {
            _node = NULL;
            _map = NULL;
            _pos = 0;
        }
    }
    SparseFlatMapIterator(const SparseFlatMapIterator<Map, NonConstValue>& rhs)
        : _node(rhs._node), _pos(rhs._pos), _map(rhs._map)
    {}
    ~SparseFlatMapIterator() {}  // required by style-checker
    
    // *this == rhs
    bool operator==(const SparseFlatMapIterator& rhs) const
    { return _node == rhs._node; }

    // *this != rhs
    bool operator!=(const SparseFlatMapIterator& rhs) const
    { return _node != rhs._node; }
        
    // ++ it
    SparseFlatMapIterator& operator++() {
        if (NULL == _node->next) {
            ++_pos;
            find_and_set_valid_node();
        } else {
            _node = _node->next;
        }
        return *this;
    }

    // it ++
    SparseFlatMapIterator operator++(int) {
        SparseFlatMapIterator tmp = *this;
        this->operator++();
        return tmp;
    }

    reference operator*() { return _node->element().value_ref(); }
    pointer operator->() { return &_node->element().value_ref(); }
    const_reference operator*() const { return _node->element().value_ref(); }
    const_pointer operator->() const { return &_node->element().value_ref(); }

private:
friend class SparseFlatMapIterator<Map, ConstValue>;
    
    void find_and_set_valid_node() {
        if (!_map->_buckets[_pos].is_valid()) {
            _pos = bit_array_first1(_map->_thumbnail, _pos + 1, _map->_nbucket);
        }
        _node = _map->_buckets + _pos;
    }

    typename Map::Bucket* _node;
    size_t _pos;
    const Map* _map;
};
 

template <typename _K, typename _T, typename _H, typename _E, bool _S>
FlatMap<_K, _T, _H, _E, _S>::FlatMap(const hasher& hashfn, const key_equal& eql)
    : _size(0)
    , _nbucket(0)
    , _buckets(NULL)
    , _thumbnail(NULL)
    , _load_factor(0)
    , _hashfn(hashfn)
    , _eql(eql)
{}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
FlatMap<_K, _T, _H, _E, _S>::~FlatMap() {
    clear();
    free(_buckets);
    _buckets = NULL;
    free(_thumbnail);
    _thumbnail = NULL;
    _nbucket = 0;
    _load_factor = 0;
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
FlatMap<_K, _T, _H, _E, _S>::FlatMap(const FlatMap& rhs)
    : _size(0)
    , _nbucket(0)
    , _buckets(NULL)
    , _thumbnail(NULL)
    , _load_factor(rhs._load_factor)
    , _hashfn(rhs._hashfn)
    , _eql(rhs._eql) {
    operator=(rhs);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
void
FlatMap<_K, _T, _H, _E, _S>::operator=(const FlatMap<_K, _T, _H, _E, _S>& rhs) {
    if (this == &rhs) {
        return;
    }
    // NOTE: assignment does not change _load_factor/_hashfn/_eql if |this| is
    // initialized
    clear();
    if (rhs.empty()) {
        return;
    }
    if (!initialized()) {
        _load_factor = rhs._load_factor;
    }
    if (_buckets == NULL || is_too_crowded(rhs._size)) {
        free(_buckets);
        _nbucket = rhs._nbucket;
        // note: need an extra bucket to let iterator know where buckets end
        _buckets = (Bucket*)malloc(sizeof(Bucket) * (_nbucket + 1/*note*/));
        if (NULL == _buckets) {
            LOG(ERROR) << "Fail to new _buckets";
            return;
        }
        if (_S) {
            free(_thumbnail);
            _thumbnail = bit_array_malloc(_nbucket);
            if (NULL == _thumbnail) {
                LOG(ERROR) << "Fail to new _thumbnail";
                return;
            }
            bit_array_clear(_thumbnail, _nbucket);
        }
    }
    if (_nbucket == rhs._nbucket) {
        // For equivalent _nbucket, walking through _buckets instead of using
        // iterators is more efficient.
        for (size_t i = 0; i < rhs._nbucket; ++i) {
            if (!rhs._buckets[i].is_valid()) {
                _buckets[i].set_invalid();
            } else {
                if (_S) {
                    bit_array_set(_thumbnail, i);
                }
                new (&_buckets[i]) Bucket(rhs._buckets[i]);
                Bucket* p1 = &_buckets[i];
                Bucket* p2 = rhs._buckets[i].next;
                while (p2) {
                    p1->next = new (_pool.get()) Bucket(*p2);
                    p1 = p1->next;
                    p2 = p2->next;
                }
            }
        }
        _buckets[rhs._nbucket].next = NULL;
        _size = rhs._size;
    } else {
        for (const_iterator it = rhs.begin(); it != rhs.end(); ++it) {
            operator[](Element::first_ref_from_value(*it)) = 
                Element::second_ref_from_value(*it);
        }
    }
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
int FlatMap<_K, _T, _H, _E, _S>::init(size_t nbucket, u_int load_factor) {
    if (initialized()) {
        LOG(ERROR) << "Already initialized";
        return -1;
    }
    if (load_factor < 10 || load_factor > 100) {
        LOG(ERROR) << "Invalid load_factor=" << load_factor;
        return -1;
    }
    _size = 0;
    _nbucket = flatmap_round(nbucket);
    _load_factor = load_factor;
                                
    _buckets = (Bucket*)malloc(sizeof(Bucket) * (_nbucket + 1));
    if (NULL == _buckets) {
        LOG(ERROR) << "Fail to new _buckets";
        return -1;
    }
    for (size_t i = 0; i < _nbucket; ++i) {
        _buckets[i].set_invalid();
    }
    _buckets[_nbucket].next = NULL;

    if (_S) {
        _thumbnail = bit_array_malloc(_nbucket);
        if (NULL == _thumbnail) {
            LOG(ERROR) << "Fail to new _thumbnail";
            return -1;
        }
        bit_array_clear(_thumbnail, _nbucket);
    }
    return 0;
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
void FlatMap<_K, _T, _H, _E, _S>::swap(FlatMap<_K, _T, _H, _E, _S> & rhs) {
    std::swap(rhs._size, _size);
    std::swap(rhs._nbucket, _nbucket);
    std::swap(rhs._buckets, _buckets);
    std::swap(rhs._thumbnail, _thumbnail);
    std::swap(rhs._load_factor, _load_factor);
    std::swap(rhs._hashfn, _hashfn);
    std::swap(rhs._eql, _eql);
    rhs._pool.swap(_pool);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
_T* FlatMap<_K, _T, _H, _E, _S>::insert(const key_type& key,
                                        const mapped_type& value) {
    mapped_type *p = &operator[](key);
    *p = value;
    return p;
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
_T* FlatMap<_K, _T, _H, _E, _S>::insert(const std::pair<key_type, mapped_type>& kv) {
    return insert(kv.first, kv.second);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
template <typename K2>
size_t FlatMap<_K, _T, _H, _E, _S>::erase(const K2& key, _T* old_value) {
    if (!initialized()) {
        return 0;
    }
    // TODO: Do we need auto collapsing here?
    const size_t index = flatmap_mod(_hashfn(key), _nbucket);
    Bucket& first_node = _buckets[index];
    if (!first_node.is_valid()) {
        return 0;
    }
    if (_eql(first_node.element().first_ref(), key)) {
        if (old_value) {
            *old_value = first_node.element().second_ref();
        }
        if (first_node.next == NULL) {
            first_node.element().~Element();
            first_node.set_invalid();
            if (_S) {
                bit_array_unset(_thumbnail, index);
            }
        } else {
            // A seemingly correct solution is to copy the memory of *p to
            // first_node directly like this:
            //   first_node.element().~Element();
            //   first_node = *p;
            // It works at most of the time, but is wrong generally.
            // If _T references self inside like this:
            //   Value {
            //     Value() : num(0), num_ptr(&num) {}
            //     int num;
            //     int* num_ptr;
            //   };
            // After copying, num_ptr will be invalid.
            // Calling operator= is the price that we have to pay.
            Bucket* p = first_node.next;
            first_node.next = p->next;
            const_cast<_K&>(first_node.element().first_ref()) =
                p->element().first_ref();
            first_node.element().second_ref() = p->element().second_ref();
            p->element().~Element();
            _pool.back(p);
        }
        --_size;
        return 1UL;
    }
    Bucket *p = first_node.next;
    Bucket *last_p = &first_node;
    while (p) {
        if (_eql(p->element().first_ref(), key)) {
            if (old_value) {
                *old_value = p->element().second_ref();
            }
            last_p->next = p->next;
            p->element().~Element();
            _pool.back(p);
            --_size;
            return 1UL;
        }
        last_p = p;
        p = p->next;
    }
    return 0;
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
void FlatMap<_K, _T, _H, _E, _S>::clear() {
    if (0 == _size) {
        return;
    }
    _size = 0;
    if (NULL != _buckets) {
        for (size_t i = 0; i < _nbucket; ++i) {
            Bucket& first_node = _buckets[i];
            if (first_node.is_valid()) {
                first_node.element().~Element();
                Bucket* p = first_node.next;
                while (p) {
                    Bucket* next_p = p->next;
                    p->element().~Element();
                    _pool.back(p);
                    p = next_p;
                }
                first_node.set_invalid();
            }
        }
    }
    if (NULL != _thumbnail) {
        bit_array_clear(_thumbnail, _nbucket);
    }
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
void FlatMap<_K, _T, _H, _E, _S>::clear_and_reset_pool() {
    clear();
    _pool.reset();
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
template <typename K2>
_T* FlatMap<_K, _T, _H, _E, _S>::seek(const K2& key) const {
    if (!initialized()) {
        return NULL;
    }
    Bucket& first_node = _buckets[flatmap_mod(_hashfn(key), _nbucket)];
    if (!first_node.is_valid()) {
        return NULL;
    }
    if (_eql(first_node.element().first_ref(), key)) {
        return &first_node.element().second_ref();
    }
    Bucket *p = first_node.next;
    while (p) {
        if (_eql(p->element().first_ref(), key)) {
            return &p->element().second_ref();
        }
        p = p->next;
    }
    return NULL;
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
_T& FlatMap<_K, _T, _H, _E, _S>::operator[](const key_type& key) {
    const size_t index = flatmap_mod(_hashfn(key), _nbucket);
    Bucket& first_node = _buckets[index];
    if (!first_node.is_valid()) {
        ++_size;
        if (_S) {
            bit_array_set(_thumbnail, index);
        }
        new (&first_node) Bucket(key);
        return first_node.element().second_ref();
    }
    if (_eql(first_node.element().first_ref(), key)) {
        return first_node.element().second_ref();
    }
    Bucket *p = first_node.next;
    if (NULL == p) {
        if (is_too_crowded(_size)) {
            if (resize(_nbucket + 1)) {
                return operator[](key);
            }
            // fail to resize is OK
        }
        ++_size;
        Bucket* newp = new (_pool.get()) Bucket(key);
        first_node.next = newp;
        return newp->element().second_ref();
    }
    while (1) {
        if (_eql(p->element().first_ref(), key)) {
            return p->element().second_ref();
        }
        if (NULL == p->next) {
            if (is_too_crowded(_size)) {
                if (resize(_nbucket + 1)) {
                    return operator[](key);
                }
                // fail to resize is OK
            }
            ++_size;
            Bucket* newp = new (_pool.get()) Bucket(key);
            p->next = newp;
            return newp->element().second_ref();
        }
        p = p->next;
    }
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
void FlatMap<_K, _T, _H, _E, _S>::save_iterator(
    const const_iterator& it, PositionHint* hint) const {
    hint->nbucket = _nbucket;
    hint->offset = it._entry - _buckets;
    if (it != end()) {
        hint->at_entry = (it._entry == it._node);
        hint->key = it->first;
    } else {
        hint->at_entry = false;
        hint->key = key_type();
    }
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
typename FlatMap<_K, _T, _H, _E, _S>::const_iterator
FlatMap<_K, _T, _H, _E, _S>::restore_iterator(const PositionHint& hint) const {
    if (hint.nbucket != _nbucket/*resized*/ ||
        hint.offset >= _nbucket/*invalid hint*/) {
        return begin();  // restart
    }
    Bucket& first_node = _buckets[hint.offset];
    if (hint.at_entry) {
        return const_iterator(this, hint.offset);
    }
    if (!first_node.is_valid()) {
        // All elements hashed to the entry were removed, try next entry.
        return const_iterator(this, hint.offset + 1);
    }
    Bucket *p = &first_node;
    do {
        if (_eql(p->element().first_ref(), hint.key)) {
            const_iterator it;
            it._node = p;
            it._entry = &first_node;
            return it;
        }
        p = p->next;
    } while (p);
    // Last element that we iterated (and saved in PositionHint) was removed,
    // don't know which element to start, just restart at the beginning of
    // the entry. Some elements in the entry may be revisited, which
    // shouldn't be often.
    return const_iterator(this, hint.offset);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
bool FlatMap<_K, _T, _H, _E, _S>::resize(size_t nbucket2) {
    nbucket2 = flatmap_round(nbucket2);
    if (_nbucket == nbucket2) {
        return false;
    }

    FlatMap new_map;
    if (new_map.init(nbucket2, _load_factor) != 0) {
        LOG(ERROR) << "Fail to init new_map, nbucket=" << nbucket2;
        return false;
    }
    for (iterator it = begin(); it != end(); ++it) {
        new_map[Element::first_ref_from_value(*it)] = 
            Element::second_ref_from_value(*it);
    }
    new_map.swap(*this);
    return true;
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
BucketInfo FlatMap<_K, _T, _H, _E, _S>::bucket_info() const {
    size_t max_n = 0;
    size_t nentry = 0;
    for (size_t i = 0; i < _nbucket; ++i) {
        if (_buckets[i].is_valid()) {
            size_t n = 1;
            for (Bucket* p = _buckets[i].next; p; p = p->next, ++n);
            max_n = std::max(max_n, n);
            ++nentry;
        }
    }
    const BucketInfo info = { max_n, size() / (double)nentry };
    return info;
}

inline std::ostream& operator<<(std::ostream& os, const BucketInfo& info) {
    return os << "{maxb=" << info.longest_length
              << " avgb=" << info.average_length << '}';
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
typename FlatMap<_K, _T, _H, _E, _S>::iterator FlatMap<_K, _T, _H, _E, _S>::begin() {
    return iterator(this, 0);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
typename FlatMap<_K, _T, _H, _E, _S>::iterator FlatMap<_K, _T, _H, _E, _S>::end() {
    return iterator(this, _nbucket);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
typename FlatMap<_K, _T, _H, _E, _S>::const_iterator FlatMap<_K, _T, _H, _E, _S>::begin() const {
    return const_iterator(this, 0);
}

template <typename _K, typename _T, typename _H, typename _E, bool _S>
typename FlatMap<_K, _T, _H, _E, _S>::const_iterator FlatMap<_K, _T, _H, _E, _S>::end() const {
    return const_iterator(this, _nbucket);
}

}  // namespace butil

#endif  //BUTIL_FLAT_MAP_INL_H
