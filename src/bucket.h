#pragma once

#include<cstdint>
#include<cstddef>
#include<cassert>
#include<climits>
#include<vector>
#include<cstring>
#include<iostream>
#include<utility>
#include<limits>
#include <immintrin.h> //SIMD

#include "keyvalue.h"

namespace buckindex {

const unsigned int BITS_UINT64_T = 64;

/**
 * Bucket is a list of unsorted KeyValue
 * It can be either S-Bucket or D-Bucket, depending on the LISTTYPE
 * Note that the template parameter SIZE must matches the SIZE of the LISTTYPE
 */
template<class LISTTYPE, typename T, typename V, size_t SIZE>
class Bucket { // can be an S-Bucket or a D-Bucket. S-Bucket and D-Bucket and different size
public:

    Bucket() {
        pivot_ = std::numeric_limits<T>::max();
        memset(bitmap_, 0, sizeof(bitmap_));
    }

    bool lookup(const T &key, V& value) const;
    // Find the largest key that is <= the lookup key
    bool lb_lookup(const T &key, V& value) const; // lower-bound lookup
    bool lookup_SIMD(const T &key, V& value) const; // TODO: LISTTYPE do the lookup
    bool lb_lookup_SIMD(const T &key, V& value) const; // lower-bound lookup
    bool insert(const KeyValue<T, V> &kv, bool update_pivot); // Return false if insert() fails

    int get_pos(const T &key) const{ // get the index of key in list_; return -1 if not found
        for (int i = 0; i < SIZE; i++) {
            if (valid(i) && list_.at(i).key_ == key) {
                return i;
            }
        }
        return -1;
    }
    
    //TODO: remove all iterators
    // iterator-related
    class UnsortedIterator;
    UnsortedIterator begin_unsort() {return UnsortedIterator(this, 0); }
    UnsortedIterator end_unsort() {return UnsortedIterator(this, SIZE); }

    class SortedIterator;
    SortedIterator begin() {return SortedIterator(this, 0); }
    SortedIterator end() {return SortedIterator(this, num_keys()); }

    void get_valid_kvs(std::vector<KeyValue<T, V>> &v) const {
        // read bitmap
        // get all valid kvs
        // check the bitmap again, read everything again until bitmap matches
        uint64_t bitmap2[BITMAP_SIZE];
        do {
            memcpy(bitmap2, bitmap_, sizeof(bitmap_));
            v.clear();
            for (int i = 0; i < SIZE; i++) {
                if (valid(i)) {
                    v.push_back(list_.at(i));
                }
            }
        } while (memcmp(bitmap_, bitmap2, sizeof(bitmap_)) != 0);
    }  


    inline T get_pivot() const { return pivot_; }
    inline void set_pivot(T pivot) { pivot_ = pivot; }

    inline size_t num_keys() const {
        size_t cnt = 0;
        for (int i = 0; i < BITMAP_SIZE; i++) {
            cnt += __builtin_popcountll(bitmap_[i]);
        }
        return cnt;
    }

    inline KeyValue<T, V> at(int pos) const { return list_.at(pos); }
    inline std::pair<T*, V*> get_kvptr(int pos) { return list_.get_kvptr(pos); }

    KeyValue<T, V> find_kth_smallest(int k) const; // find the kth smallest element in 1-based index

    //bitmap operations
    inline int find_empty_slot() const { // return the offset of the first bit=0
        for (int i = 0; i < BITMAP_SIZE; i++) {
            int pos = __builtin_clzll(~bitmap_[i]);
            if (pos != BITS_UINT64_T) { // found one empty slot
                pos = i * BITS_UINT64_T + pos;
                if (pos < SIZE) return pos;
                else return -1; // there are some redundant bits
                                // when SIZE % BITS_UINT64_T != 0
            }
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
    int quickselect_partiton(std::vector<KeyValue<T, V>>& a, int left, int right, int pivot) const {
        int pivotValue = a[pivot].key_;
        std::swap(a[pivot], a[right]);  // Move pivot to end
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (a[i].key_ < pivotValue) {
                std::swap(a[i], a[storeIndex]);
                storeIndex++;
            }
        }
        std::swap(a[storeIndex], a[right]);  // Move pivot to its final place
        return storeIndex;
    }

    // helper function for find_kth_smallest()
    KeyValue<T, V> quickselect(std::vector<KeyValue<T, V>>& a, int left, int right, int k) const {
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

template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lookup(const T &key, V &value) const {
    for (int i = 0; i < SIZE; i++) {
        if (valid(i) && list_.at(i).key_ == key) {
            value = list_.at(i).value_;
            return true;
        }
    }
 
    return false;
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lb_lookup(const T &key, V &value) const {
    
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


template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lookup_SIMD(const T &key, V &value) const {
    // TODO
    return false;
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::lb_lookup_SIMD(const T &key, V &value) const {
    // TODO
    return false;
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::insert(const KeyValue<T, V> &kv, bool update_pivot) {
    int pos = find_empty_slot();
    if (pos == -1 || pos >= SIZE) return false; // return false if the Bucket is already full
    list_.put(pos, kv.key_, kv.value_);
    validate(pos);

    if (update_pivot && kv.key_ < pivot_) {
        pivot_ = kv.key_;
    }

    return true;
}


//TODO: call get_valid_kvs and std::nth_element
template<class LISTTYPE, typename T, typename V, size_t SIZE>
KeyValue<T, V> Bucket<LISTTYPE, T, V, SIZE>::find_kth_smallest(int k) const {
    int n = num_keys();
    k--;
    assert(k >= 0 && k < n);

    std::vector<KeyValue<T, V>> valid_kvs(n);
    int id = 0;
    for (int i = 0; i < SIZE; i++) {
        if (valid(i)) valid_kvs[id++] = list_.at(i);
    }
    assert(id == n);
    
    return quickselect(valid_kvs, 0, n-1, k);
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
class Bucket<LISTTYPE, T, V, SIZE>::UnsortedIterator {
public:
    using BucketType = Bucket<LISTTYPE, T, V, SIZE>;
    using KeyValueType = KeyValue<T, V>;

    explicit UnsortedIterator(BucketType *bucket) : bucket_(bucket) {
        assert(bucket_ != nullptr);
        cur_pos_ = 0;
        find_next_valid();
    }
        
    UnsortedIterator(BucketType *bucket, int pos) : bucket_(bucket) {
      assert(pos >= 0 && pos <= SIZE);
      cur_pos_ = pos;
      // cur_pos_ should always be at a valid position
      if (pos < SIZE && !bucket_->valid(cur_pos_)) find_next_valid();
    }

    void operator++(int) {
        find_next_valid();
    }

    UnsortedIterator &operator++() {
        find_next_valid();
        return *this;
    }

    KeyValueType operator*() const {
      return bucket_->at(cur_pos_);
    }

    // std::pair<T*, V*> operator*() const {
    //   return bucket_->get_kvptr(cur_pos_);
    // }

    bool operator==(const UnsortedIterator& rhs) const {
      return bucket_ == rhs.bucket_ && cur_pos_ == rhs.cur_pos_;
    }

    bool operator!=(const UnsortedIterator& rhs) const { return !(*this == rhs); };

private:
    BucketType *bucket_;
    int cur_pos_ = 0;  // current position in the bucket list, cur_pos_ == SIZE if at end

    // skip invalid entries, and find the position of the next valid entry
    inline void find_next_valid() {
        if (cur_pos_ == SIZE) return;
        cur_pos_++;
        while (cur_pos_ < SIZE && !bucket_->valid(cur_pos_)) cur_pos_++;
    }
  };

// TODO: store num_keys() as a member variable num_keys_ and ensure concurency?
template<class LISTTYPE, typename T, typename V, size_t SIZE>
class Bucket<LISTTYPE, T, V, SIZE>::SortedIterator {
public:
    using BucketType = Bucket<LISTTYPE, T, V, SIZE>;
    using KeyValueType = KeyValue<T, V>;

    explicit SortedIterator(BucketType *bucket) : bucket_(bucket) {
        assert(bucket_ != nullptr);
        cur_pos_ = 0;
    }
        
    SortedIterator(BucketType *bucket, int pos) : bucket_(bucket) {
      assert(pos >= 0 && pos <= bucket_->num_keys());
      cur_pos_ = pos;
    }

    void operator++(int) {
        ++(*this);
    }

    SortedIterator &operator++() {
        if (cur_pos_ < bucket_->num_keys()) cur_pos_++;
        return *this;
    }

    KeyValueType operator*() const {
        return bucket_->find_kth_smallest(cur_pos_ + 1);
    }

    bool operator==(const SortedIterator& rhs) const {
        return bucket_ == rhs.bucket_ && cur_pos_ == rhs.cur_pos_;
    }

    bool operator!=(const SortedIterator& rhs) const { return !(*this == rhs); };

private:
    BucketType *bucket_;
    int cur_pos_ = 0;  // current position in the bucket list, cur_pos_ == SIZE if at end
  };


} // end namespace buckindex
