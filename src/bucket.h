#pragma once

#include<cstdint>
#include<cstddef>
#include<cassert>
#include<climits>
#include<vector>
#include<cstring>
#include<iostream>
#include <utility>
#include <immintrin.h> //SIMD

#include "keyvalue.h"

namespace buckindex {

const unsigned int BUCKET_SIZE = 128;
const unsigned int SBUCKET_SIZE = 8;
const unsigned int BITS_UINT64_T = 64;

    /**
     * Bucket is a list of unsorted KeyValue
     * It can be either S-Bucket or D-Bucket, depending on the LISTTYPE
     * Note that the template parameter SIZE must matches the SIZE of the LISTTYPE
     */
template<class LISTTYPE, class T, class V, size_t SIZE>
class Bucket { // can be an S-Bucket or a D-Bucket. S-Bucket and D-Bucket and different size
public:

    Bucket() {
        memset(bitmap_, 0, sizeof(bitmap_));
    }

    void copy(Bucket<LISTTYPE, T, V, SIZE> &other) {
        for (int i = 0; i < SIZE; i++) {
            this->list_.put(i, other.at(i).key_, other.at(i).value_);
        }
    }

    bool lookup(T key, V& value) const;
    // Find the largest key that is <= the lookup key
    bool lb_lookup(T key, V& value) const; // lower-bound lookup
    bool lookup_SIMD(T key, V& value) const;
    bool lb_lookup_SIMD(T key, V& value) const; // lower-bound lookup
    bool insert(KeyValue<T, V> kv, bool update_pivot = true); // Return false if insert() fails

    inline size_t num_keys() const {
        size_t cnt = 0;
        for (int i = 0; i < BITMAP_SIZE; i++) {
            cnt += __builtin_popcountll(bitmap_[i]);
        }
        return cnt;
    }

    inline KeyValue<T, V> at(int pos) { return list_.at(pos); }

    T find_kth_smallest(int k); // find the kth smallest element in 1-based index

    //bitmap operations
    inline int find_empty_slot() { // return the offset of the first bit=0
        for (int i = 0; i < BITMAP_SIZE; i++) {
            int pos = __builtin_clzll(~bitmap_[i]);
            if (pos != BITS_UINT64_T) return i * BITS_UINT64_T + pos;

        }
        return -1; // No zero bit found
    }

    inline void validate(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = BITS_UINT64_T - 1 - pos % BITS_UINT64_T; // pos from the most significant bit
        bitmap_[bitmap_pos] |= (1ULL << bit_pos);
    }

    inline void invalidate(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = BITS_UINT64_T - 1 - pos % BITS_UINT64_T;
        bitmap_[bitmap_pos] &= ~(1ULL << bit_pos);
    } 

    inline bool valid(int pos) const {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = BITS_UINT64_T - 1 - pos % BITS_UINT64_T;
        return (bitmap_[bitmap_pos]  & (1ULL << bit_pos)) != 0;
    }

private:
    uint64_t bitmap_[SIZE/BITS_UINT64_T + (SIZE % BITS_UINT64_T ? 1 : 0)];  //indicate whether the entries in the list_ are valid.
    size_t BITMAP_SIZE = SIZE/BITS_UINT64_T + (SIZE % BITS_UINT64_T ? 1 : 0);
   
    T pivot_;
    LISTTYPE list_;

    // helper function for find_kth_smallest()
    int quickselect_partiton(std::vector<T>& a, int left, int right, int pivot) {
        int pivotValue = a[pivot];
        std::swap(a[pivot], a[right]);  // Move pivot to end
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (a[i] < pivotValue) {
                std::swap(a[i], a[storeIndex]);
                storeIndex++;
            }
        }
        std::swap(a[storeIndex], a[right]);  // Move pivot to its final place
        return storeIndex;
    }

    // helper function for find_kth_smallest()
    int quickselect(std::vector<T>& a, int left, int right, int k) {
        if (left == right) return a[left];
        int pivot = left +  (right - left) / 2;  // We can choose a random pivot
        pivot = quickselect_partiton(a, left, right, pivot);
        if (k == pivot) {
            return a[k];
        } else if (k < pivot) {
            return quickselect(a, left, pivot - 1, k);
        } else {
            return quickselect(a, pivot + 1, right, k);
        }
    }
};

template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lookup(T key, V &value) const {
    for (int i = 0; i < SIZE; i++) {
        if (valid(i) && list_.at(i).key_ == key) {
            value = list_.at(i).value_;
            return true;
        }
    }
    
    return false;
}

template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lb_lookup(T key, V &value) const {
    
    T target_key = 0; // TODO: define zero as a template parameter?
    int pos = -1;
    for (int i = 0; i < SIZE; i++) {
        if (valid(i) && list_.at(i).key_ <= key && list_.at(i).key_ > target_key) {
            target_key = list_.at(i).key_;
            pos = i;
        }
    }

    if (pos == -1) return false;

    value = list_.at(pos).value_;
    return true;
}


template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lookup_SIMD(T key, V &value) const {
    // TODO
    return false;
}

template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lb_lookup_SIMD(T key, V &value) const {
    // TODO
    return false;
}

/*
template<class T, class V, size_t SIZE>
class Bucket<KeyListValueList<T, V, SIZE>, T, V, SIZE> {
public:
    bool lookup_SIMD(T key, V &value) const {
        //TODO
    }
    bool lb_lookup_SIMD(T key, V &value) const {
        //TODO
    }
};

template<class T, class V, size_t SIZE>
class Bucket<KeyValueList<T, V, SIZE>, T, V, SIZE> {
public:
    bool lookup_SIMD(T key, V &value) const {
        //TODO
    }
    bool lb_lookup_SIMD(T key, V &value) const {
        //TODO
    }
};

*/

template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::insert(KeyValue<T, V> kv, bool update_pivot) {
    int pos = find_empty_slot();
    if (pos == -1) return false; // return false if the Bucket is already full
    list_.put(pos, kv.key_, kv.value_);
    validate(pos);

    if (update_pivot && kv.key_ < pivot_) {
        pivot_ = kv.key_;
    }

    return true;
}


template<class LISTTYPE, class T, class V, size_t SIZE>
T Bucket<LISTTYPE, T, V, SIZE>::find_kth_smallest(int k) {
    int n = num_keys();
    k--;
    assert(k >= 0 && k < n);

    std::vector<T> valid_keys(n);
    int id = 0;
    for (int i = 0; i < SIZE; i++) {
        if (valid(i)) valid_keys[id++] = list_.at(i).get_key();
    }
    assert(id == n);
    
    return quickselect(valid_keys, 0, n-1, k);
}

} // end namespace buckindex
