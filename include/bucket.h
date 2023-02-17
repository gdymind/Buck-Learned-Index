#ifndef _NODE_TYPE_H_
#define _NODE_TYPE_H_
// Minimum requirement:

// node type: segment group, segment, bucket

// segment group: model, metadata, list of {bucket}

// segment: model, metadata, list of {bucket}

// bucket: metadata, list of {key,ptr}

#include<cstdint>
#include<cstddef>
#include<climits>
#include <immintrin.h> //SIMD

typedef unsigned long long key_type;
typedef unsigned long long value_type;

const unsigned int BUCKET_SIZE = 128;
const unsigned int SBUCKET_SIZE = 8;

const unsigned int MAX_BITS = 10000;
const unsigned int INT_BITS = sizeof(int) * 8;


struct KVPTR
{
    key_type key;
    uint64_t* ptr; // 8 bytes; if it points to bucket/segment/segment group, cast it to correct type
};



class BucketBase {
public:
    key_type pivot = ULLONG_MAX; // smallest element
    int cnt = 0;
    // key_type base; // key compression

    BucketBase() { }

    inline void set_bit(int i) {
        bitmap[i / INT_BITS] |= (1 << (i % INT_BITS));
    }

    inline void reset(int i) {
        bitmap[i / INT_BITS] &= ~(1 << (i % INT_BITS));
    } 

    inline bool read(int i) {
        return (bitmap[i / INT_BITS] & (1 << (i % INT_BITS))) != 0;
    }

    int find_first_zero() {
        for (int i = 0; i <= MAX_BITS / INT_BITS; i++) {
            if (bitmap[i] != -1) {
                for (int j = 0; j < INT_BITS; j++) {
                    if ((bitmap[i] & (1 << j)) == 0) {
                        return i * INT_BITS + j;
                    }
                }
            }
        }
        return -1; // No zero bit found
    }


    int find_first_zero_SIMD() {
        // Load a vector of all ones
        __m256i ones = _mm256_set1_epi32(-1);

        for (int i = 0; i <= MAX_BITS / (8 * sizeof(__m256i)); i++) {
            // Load a vector of bitmap integers
            __m256i mp = _mm256_loadu_si256((__m256i*)(bitmap + 8 * i));

            // Compare the bitmap vector to the vector of ones
            __m256i cmp = _mm256_cmpeq_epi32(mp, ones);

            // Check if any element in the vector is zero
            int mask = _mm256_movemask_ps((__m256)cmp);

            if (mask != 0xFFFFFFFF) {
                // Found a zero bit in the bitmap vector
                int j = __builtin_ctz(~mask);
                return i * 8 * sizeof(__m256i) + j;
            }
        }
        return -1; // No zero bit found
    }


private:
    int bitmap[MAX_BITS/INT_BITS + (MAX_BITS%INT_BITS != 0)] __attribute__((aligned(32))) = {0}; // size of already occupied slot // can be changed to a bitmap
};


class Bucket: BucketBase
{
public:
    inline KVPTR read_KV(int pos) { return kv_pairs[pos]; }

    bool insertKV(KVPTR kv) {
        int pos = find_first_zero();
        // int pos = find_first_zero_SIMD();
        kv_pairs[pos] = kv;
        set_bit(pos);
        if (kv.key < pivot) { pivot = kv.key; }
    }
private:
    KVPTR kv_pairs[BUCKET_SIZE]; //TODO: change to key array + pointer array
};

class SBucket: BucketBase
{
public:
    bool insertKV(KVPTR kv) {
        int pos = find_first_zero();
        // int pos = find_first_zero_SIMD();
        kv_pairs[pos] = kv;
        set_bit(pos);
        if (kv.key < pivot) { pivot = kv.key; }
    }

private:
    KVPTR kv_pairs[SBUCKET_SIZE]; //TODO: change to key array + pointer array
};

#endif