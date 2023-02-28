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

const unsigned int INT_BITS = sizeof(unsigned int) * 8;


template<class T, class V>
struct KeyValue
{
    T key_;
    V value_; // 8 bytes; 
                    // In S-bucket, it is a pointer to bucket/segment/segment group(e.g., we can recast it to uint64_t*)
                    // In D-bucket, it is the actual value

    KeyValue(T k, V v): key_(k), value_(v) {}
};


template<class T, class V, size_t SIZE, T MAX_KEY>
class Bucket { // can be an S-Bucket or a D-Bucket. S-Bucket and D-Bucket and different size
public:
    T pivot = MAX_KEY; // smallest element
    // key_type base; // key compression

    Bucket() { }

    // D-Bucket lookup returns a value
    // S-Bucket lookup returns a pointer
    // If no matches -> return nullptr;
    V lookup(T key);

    // The return value indicate whether insert is successful.
    bool insert(KeyValue<T, V> kvptr);


private:
    uint32_t bitmap_[SIZE * 8 / INT_BITS + (SIZE * 8 % INT_BITS != 0)] __attribute__((aligned(32))) = {0}; // indicate whether the entries in keys_ and value_ptrs_ are valid
    
    // KeyValue kv_pairs[SIZE]; //TODO: change to key array + pointer array
    T keys_[SIZE];
    V value_ptrs_[SIZE]; // the pointers are actual

    inline KeyValue<T, V> read_KV(int pos) { return KeyValue<T, V>(keys_[pos], value_ptrs_[pos]); }

    //bitmap operations
    inline int find_first_zero_bit() { // return the offset of the first bit=0
        for (int i = 0; i < SIZE; i++) {
            int pos = __builtin_ffs(bitmap_[i]);
            if (pos != 0) return i*INT_BITS + pos - 1;

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

template<class T, class V, size_t SIZE, T MAX_KEY>
V Bucket<T, V, SIZE, MAX_KEY>::lookup(T key_) {
    V vptr = nullptr;
    //TODO
    return vptr;
}

template<class T, class V, size_t SIZE, T MAX_KEY>
bool Bucket<T, V, SIZE, MAX_KEY>::insert(KeyValue<T, V> kvptr) {
    int pos = find_first_zero_bit();
    if (pos == -1) return false; // return false if the Bucket is already full
    // int pos = find_first_zero_SIMD();
    keys_[pos] = kvptr.key_;
    value_ptrs_[pos] = kvptr.value_;
    // kv_pairs[pos] = kv;
    set_bit(pos);
    if (kvptr.key_ < pivot) { pivot = kvptr.key_; }
    return true;
}

} // end namespace buckindex