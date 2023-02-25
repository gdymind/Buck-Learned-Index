#ifndef _NODE_TYPE_H_
#define _NODE_TYPE_H_
// Minimum requirement:

// node type: segment group, segment, bucket

// segment group: model, metadata, list of {bucket}

// segment: model, metadata, list of {bucket}

// bucket: metadata, list of {key,ptr}

#include<cstdint>
#include<cstddef>
#include<cassert>
#include<climits>
#include <immintrin.h> //SIMD

typedef unsigned long long key_type;
typedef unsigned long long value_type;

const unsigned int BUCKET_SIZE = 128;
const unsigned int SBUCKET_SIZE = 8;

const unsigned int MAX_BITS = 10000;
const unsigned int INT_BITS = sizeof(unsigned int) * 8;

const key_type UNDEFINED_KEY = ULLONG_MAX;


struct KVPTR
{
    key_type key;
    uint64_t* ptr; // 8 bytes; if it points to bucket/segment/segment group, cast it to correct type

    KVPTR(key_type k, uint64_t* p): key(k), ptr(p) {}
};


template<size_t SIZE>
class Bucket {
public:
    key_type pivot = UNDEFINED_KEY; // smallest element
    // int cnt = 0; // the number of valid kvs in the bucket
    // key_type base; // key compression

    Bucket() { }

    value_type lookup(key_type key, value_type &value) {
        value_type v;
        return v;
    }

    bool insert(KVPTR kv);

    inline int find_first_zero_bit() { // return the offset of the first bit=0
        for (int i = 0; i < SIZE; i++) {
            int pos = __builtin_ffs(bitmap_[i]);
            if (pos != 0) return i*INT_BITS + pos - 1;

        }
        return -1; // No zero bit found
    }


private:
    // MAX_BITS is a templete argument
    uint32_t bitmap_[SIZE * 8 / INT_BITS + (SIZE * 8 % INT_BITS != 0)] __attribute__((aligned(32))) = {0}; // size of already occupied slot // can be changed to a bitmap
    
    // KVPTR kv_pairs[SIZE]; //TODO: change to key array + pointer array
    key_type keys_[SIZE];
    uint64_t* value_ptrs_[SIZE];

    inline KVPTR read_KV(int pos) { return KVPTR(keys_[pos], value_ptrs_[pos]); }

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

    inline bool read(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos >> 5;
        int bit_pos = pos - (bitmap_pos << 5);
        return (bitmap_[bitmap_pos]  & (1U << bit_pos)) != 0;
    }
};

template<size_t SIZE>
bool Bucket<SIZE>::insert(KVPTR kv) {
    int pos = find_first_zero_bit();
    if (pos == -1) return false; // return false if the Bucket is already full
    // int pos = find_first_zero_SIMD();
    keys_[pos] = kv.key;
    value_ptrs_[pos] = kv.ptr;
    // kv_pairs[pos] = kv;
    set_bit(pos);
    if (kv.key < pivot) { pivot = kv.key; }
    return true;
}


#endif