#pragma once

#include<cstdint>
#include<cstddef>

namespace buckindex {

template<class T, class V>
struct KeyValue
{
    T key_;
    V value_; // 8 bytes; 
                    // In S-bucket, it is a pointer to bucket/segment/segment group(e.g., we can recast it to uint64_t*)
                    // In D-bucket, it is the actual value

    KeyValue(T k, V v): key_(k), value_(v) {}
};

template<class T, class V, size_t SIZE> 
class KeyListValueList {
public:
    T keys_[SIZE];
    V values_[SIZE];

    KeyValue<T,V> at(int pos) { return KeyValue<T,V>(keys_[pos], values_[pos]); }
    void put(int pos, T key, V value) { keys_[pos] = key; values_[pos] = value; }
};

template<class T, class V, size_t SIZE>
class KeyValueList {
public:
    KeyValue<T, V> kvs_[SIZE];

    KeyValue<T, V> at(int pos) { return kvs_[pos]; }
    void put(int pos, T key, V value) { kvs_[pos].key_ = key; kvs_[pos].value_ = value; }
};

}