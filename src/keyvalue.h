#pragma once

#include<cstdint>
#include<cstddef>

namespace buckindex {

template<typename T, typename V>
struct KeyValue
{
    T key_;
    V value_; // 8 bytes; 
              // In S-bucket, it is a pointer to bucket/segment/segment group(e.g., we can recast it to uint64_t*)
              // In D-bucket, it is the actual value

    KeyValue() {}
    KeyValue(T k, V v): key_(k), value_(v) {}
    KeyValue(const KeyValue &kv): key_(kv.key_), value_(kv.value_) {}
    T get_key() const { return key_; }

    // bool operator<(const KeyValue<T, V>& rhs) const {
    //     if (key_ != rhs.key_) return key_ < rhs.key_;
    //     return value_ < rhs.value_;
    // }

    KeyValue<T, V>& operator=(const KeyValue<T, V>& rhs) {
        key_ = rhs.key_;
        value_ = rhs.value_;
        return *this;
    }
};

template<typename T, typename V, size_t SIZE> 
class KeyListValueList { // KV list for S-Bucket
public:
    T keys_[SIZE];
    V values_[SIZE];

    KeyValue<T,V> at(int pos) const { return KeyValue<T,V>(keys_[pos], values_[pos]); }
    // std::pair<T*, V*> get_kvptr(int pos) { return std::make_pair(&keys_[pos], &values_[pos]); }
    void put(int pos, T key, V value) { keys_[pos] = key; values_[pos] = value; }
    void put(int pos, KeyValue<T,V> kv) { keys_[pos] = kv.key_; values_[pos] = kv.value_; }
};

template<typename T, typename V, size_t SIZE>
class KeyValueList { // KV list for S-Bucket
public:

    KeyValue<T, V> kvs_[SIZE];

    KeyValue<T, V> at(int pos) const { return kvs_[pos]; }
    // std::pair<T*, V*> get_kvptr(int pos) { return std::make_pair(&kvs_[pos].key_, &kvs_[pos].value_); }
    void put(int pos, T key, V value) { kvs_[pos].key_ = key; kvs_[pos].value_ = value; }
    void put(int pos, KeyValue<T,V> kv) { kvs_[pos] = kv; }
};

}
