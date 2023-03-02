#pragma once

#include<cstdint>
#include<cstddef>
#include<cassert>
#include<climits>
#include <immintrin.h> //SIMD


namespace buckindex {

// Key type always use template class T; value type


const unsigned int BUCKET_SIZE = 128;
const unsigned int SBUCKET_SIZE = 8;
const unsigned int BITS_UINT64_T = 64;

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


template<class LISTTYPE, class T, class V, size_t SIZE>
class Bucket { // can be an S-Bucket or a D-Bucket. S-Bucket and D-Bucket and different size
public:

    Bucket() { assert(SIZE % BITS_UINT64_T == 0); }

    // D-Bucket lookup returns a value
    // S-Bucket lookup returns a pointer
    // If no matches -> return nullptr;
    V lookup(T key);

    // The return value indicate whether insert is successful.
    bool insert(KeyValue<T, V> kvptr);

private:
    uint64_t bitmap_[SIZE/BITS_UINT64_T];  //indicate whether the entries in the list_ are valid.
   
    T pivot_;
    LISTTYPE list_;

    inline KeyValue<T, V> at(int pos) { return list_.at(pos); }

    //bitmap operations
    inline int find_first_zero_bit() { // return the offset of the first bit=0
        for (int i = 0; i < SIZE/BITS_UINT64_T; i++) {
            int pos = __builtin_ffs(bitmap_[i]);
            if (pos != 0) return i*BITS_UINT64_T + pos - 1;

        }
        return -1; // No zero bit found
    }

    inline void set_bit(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos >> 5;
        int bit_pos = pos - (bitmap_pos << 5);
        bitmap_[bitmap_pos] |= (1U << bit_pos);
    }

    inline void reset_bit(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos >> 5;
        int bit_pos = pos - (bitmap_pos << 5);
        bitmap_[bitmap_pos] &= ~(1U << bit_pos);
    } 

    inline bool read_bit(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos >> 5;
        int bit_pos = pos - (bitmap_pos << 5);
        return (bitmap_[bitmap_pos]  & (1U << bit_pos)) != 0;
    }
};

template<class LISTTYPE, class T, class V, size_t SIZE>
V Bucket<LISTTYPE, T, V, SIZE>::lookup(T key_) {
    V vptr = nullptr;
    //TODO
    return vptr;
}

template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::insert(KeyValue<T, V> kvptr) {
    int pos = find_first_zero_bit();
    if (pos == -1) return false; // return false if the Bucket is already full
    list_.put(pos, kvptr.key_, kvptr.value_);
    set_bit(pos);

    if (kvptr.key_ < pivot) { pivot = kvptr.key_; }
    return true;
}

} // end namespace buckindex
