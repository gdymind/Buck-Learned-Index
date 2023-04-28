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
#include <algorithm> 
#include <bitset>
#include <immintrin.h> //SIMD

#include "keyvalue.h"


namespace buckindex {

constexpr unsigned int BITS_UINT64_T = sizeof(uint64_t) * 8;;

/**
 * Bucket is a list of unsorted KeyValue
 * It can be either S-Bucket or D-Bucket, depending on the LISTTYPE
 * Note that the template parameter SIZE must matches the SIZE of the LISTTYPE
 */
template<class LISTTYPE, typename T, typename V, size_t SIZE>
class Bucket { // can be an S-Bucket or a D-Bucket. S-Bucket and D-Bucket have different sizes
public:
    using KeyValueType = KeyValue<T, V>;
    using KeyValuePtrType = KeyValue<T, uintptr_t>;
    using BucketType = Bucket<LISTTYPE, T, V, SIZE>;

    Bucket() {
        // assume T and V has the same size, so we can perform masked load
        // assert(sizeof(T) == sizeof(V));

        // support only 32-bit and 64-bit keys for SIMD_lookup
        assert(sizeof(T) == 4 || sizeof(T) == 8);

        pivot_ = std::numeric_limits<T>::max(); // std::numeric_limits<T>::max() means invalid
        memset(bitmap_, 0, sizeof(bitmap_));
    }

    bool lookup(const T &key, V& value) const; // D-Bucket lookup
    bool lb_lookup(const T &key, KeyValueType& kv) const; // S-Bucket lower_bound lookup; find the largest key that is <= the lookup key

    bool SIMD_lookup(const T &key, V& value) const; // D-Bucket lookup
    bool SIMD_lb_lookup(const T &key, KeyValueType& value) const; // S-Bucket lower_bound lookup;

    bool insert(const KeyValueType &kv, bool update_pivot); // Return false if insert() fails
    bool update(const KeyValueType &kv); // find kv.key_ and update its value; return false if not found

    /**
     * Split the bucket into two buckets by the median key, and insert a new key-value pair
     * @param kv: the new key-value pair to be inserted
     * @return two KVptr of the new buckets
     */
    std::pair<KeyValuePtrType, KeyValuePtrType> split_and_insert(const KeyValueType &kv) {
        // find the median key
        T median_key = find_kth_smallest((num_keys()+1) / 2).key_;
        // create a new bucket
        BucketType *new_bucket1 = new BucketType();
        BucketType *new_bucket2 = new BucketType();
        bool success;
        // move all keys that are > median_key to the new bucket
        for (int i = 0; i < SIZE; i++) {
            if (valid(i)) {
                if (list_.at(i).key_ <= median_key)  {
                    success = new_bucket1->insert(list_.at(i), true);
                    assert(success);
                }
                else {
                    success = new_bucket2->insert(list_.at(i), true);
                    assert(success);
                }
            }
        }

        if (kv.key_ <= median_key) {
            success = new_bucket1->insert(kv, true);
            assert(success);
        }
        else {
            success = new_bucket2->insert(kv, true);
            assert(success);
        }

        std::pair<KeyValuePtrType, KeyValuePtrType> ret;
        ret.first = KeyValuePtrType(new_bucket1->get_pivot(), reinterpret_cast<uintptr_t>(new_bucket1));
        ret.second = KeyValuePtrType(new_bucket2->get_pivot(), reinterpret_cast<uintptr_t>(new_bucket2));

        return ret;
    }

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

    void get_valid_kvs(std::vector<KeyValueType> &v) const {
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

    inline KeyValueType at(int pos) const { return list_.at(pos); }
    inline std::pair<T*, V*> get_kvptr(int pos) { return list_.get_kvptr(pos); }

    KeyValueType find_kth_smallest(int k) const; // find the kth smallest element in 1-based index

    //bitmap operations
    inline int find_empty_slot() const { // return the offset of the first bit=0
        for (int i = 0; i < BITMAP_SIZE; i++) {
            if (bitmap_[i] == UINT64_MAX) continue; // all bits are 1 (occupied)
            int pos = __builtin_ctzll(~bitmap_[i]);
            pos = i * BITS_UINT64_T + pos;
            if (pos < SIZE) return pos;
            else return -1; // there are some redundant bits
                            // when SIZE % BITS_UINT64_T != 0
        }
        return -1; // no empty slot
    }

    inline void validate(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = pos % BITS_UINT64_T; // pos from LSB
        bitmap_[bitmap_pos] |= (1ULL << bit_pos);
    }

    inline void invalidate(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = pos % BITS_UINT64_T;
        bitmap_[bitmap_pos] &= ~(1ULL << bit_pos);
    } 

    inline bool valid(int pos) const {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = pos % BITS_UINT64_T;
        return (bitmap_[bitmap_pos]  & (1ULL << bit_pos)) != 0;
    }

private:
    uint64_t bitmap_[SIZE/BITS_UINT64_T + (SIZE % BITS_UINT64_T ? 1 : 0)];  //indicate whether the entries in the list_ are valid.
    size_t BITMAP_SIZE = SIZE/BITS_UINT64_T + (SIZE % BITS_UINT64_T ? 1 : 0);
   
    T pivot_;
    LISTTYPE list_;

    // Helper functions for SIMD
    // assume T and V are the same type, so we can perform masked load
    __m256i SIMD_load_keys(const KeyListValueList<T, V, SIZE>& list, int pos) const; 
    // __m256i SIMD_load_keys(const KeyValueList<T, V, SIZE>& list, int pos) const;
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
bool Bucket<LISTTYPE, T, V, SIZE>::lb_lookup(const T &key, KeyValueType &kv) const {
    T target_key = std::numeric_limits<T>::min();
    int pos = -1;
    for (int i = 0; i < SIZE; i++) {
        if (valid(i) && list_.at(i).key_ <= key && list_.at(i).key_ >= target_key) {
            target_key = list_.at(i).key_;
            pos = i;
        }
    }

    if (pos == -1) return false;

    kv = list_.at(pos);
    return true;
}


template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::insert(const KeyValueType &kv, bool update_pivot) {
    int pos = find_empty_slot();
    if (pos == -1 || pos >= SIZE) return false; // return false if the Bucket is already full
    list_.put(pos, kv.key_, kv.value_);
    validate(pos);

    if (update_pivot && kv.key_ < pivot_) {
        pivot_ = kv.key_;
    }

    return true;
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::update(const KeyValueType &kv) {
    for (int i = 0; i < SIZE; i++) {
        if (valid(i) && list_.at(i).key_ == kv.key_) {
            list_.put(i, kv.key_, kv.value_);
            return true;
        }
    }
    return false;
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
KeyValue<T, V> Bucket<LISTTYPE, T, V, SIZE>::find_kth_smallest(int k) const {
    int n = num_keys();
    k--;
    assert(k >= 0 && k < n);

    std::vector<KeyValueType> valid_kvs;
    get_valid_kvs(valid_kvs);
    assert(valid_kvs.size() == n);
    
    std::nth_element(valid_kvs.begin(), valid_kvs.begin() + k, valid_kvs.end());
    return valid_kvs[k];
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
__m256i Bucket<LISTTYPE, T, V, SIZE>::SIMD_load_keys(const KeyListValueList<T, V, SIZE>& list, int pos) const {
    return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&list.keys_[pos]));
}

// template<class LISTTYPE, typename T, typename V, size_t SIZE>
// __m256i Bucket<LISTTYPE, T, V, SIZE>::SIMD_load_keys(const KeyValueList<T, V, SIZE>& list, int pos) const {
//     __m256i key_mask = _mm256_setr_epi32(-1, 0, -1, 0, -1, 0, -1, 0);
//     const int* ptr = reinterpret_cast<const int*>(&list.kvs_[pos]);
//     return _mm256_maskload_epi32(ptr, key_mask); // only load keys, the values bits are set to 0
// }

// print the bits of a __m256i
inline void print_m256i_bits(const __m256i &key_vector) {
    int element0 = _mm256_extract_epi32(key_vector, 0);
    int element1 = _mm256_extract_epi32(key_vector, 1);
    int element2 = _mm256_extract_epi32(key_vector, 2);
    int element3 = _mm256_extract_epi32(key_vector, 3);
    int element4 = _mm256_extract_epi32(key_vector, 4);
    int element5 = _mm256_extract_epi32(key_vector, 5);
    int element6 = _mm256_extract_epi32(key_vector, 6);
    int element7 = _mm256_extract_epi32(key_vector, 7);

    std::bitset<32> bits0(element0);
    std::bitset<32> bits1(element1);
    std::bitset<32> bits2(element2);
    std::bitset<32> bits3(element3);
    std::bitset<32> bits4(element4);
    std::bitset<32> bits5(element5);
    std::bitset<32> bits6(element6);
    std::bitset<32> bits7(element7);

    std::cout << bits0 << std::endl;
    std::cout << bits1 << std::endl;
    std::cout << bits2 << std::endl;
    std::cout << bits3 << std::endl;
    std::cout << bits4 << std::endl;
    std::cout << bits5 << std::endl;
    std::cout << bits6 << std::endl;
    std::cout << bits7 << std::endl;
}


template<class LISTTYPE, typename T, typename V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::SIMD_lookup(const T &key, V &value) const {
    // We only support D-bucket; S-Bucket always calls SIMD_lb_lookup instead of SIMD_lookup
    // TODO: support S-Bucket, where key and value are in the same array
    assert((std::is_same<LISTTYPE, KeyListValueList<T, V, SIZE>>::value));

    constexpr size_t SIMD_WIDTH = 256 / sizeof(T) / 8; // the number of keys in a 256-bit SIMD register
    __m256i key_vector;
    if constexpr (sizeof(T) == 4) key_vector = _mm256_set1_epi32(key); // 32-bit integer, repeat key 8 times
    else if constexpr(sizeof(T) == 8) key_vector = _mm256_set1_epi64x(key); // 64-bit integer, repeat key 4 times

    for (int i = 0; i < SIZE; i += SIMD_WIDTH) {
        __m256i keys = SIMD_load_keys(list_, i); // load 4 or 8 keys into a SIMD register
        __m256i cmp;
        if constexpr (sizeof(T) == 4) cmp = _mm256_cmpeq_epi32(keys, key_vector); // compare every 32 bits;
                                                                                  // result bits start from LSB
        else if constexpr (sizeof(T) == 8) cmp = _mm256_cmpeq_epi64(keys, key_vector); // compare every 64 bits;
                                                                                       // result bits start from LSB

        unsigned char mask; // there are either 4 or 8 bits in the mask, so unsigned char is enough
        if constexpr(sizeof(T) == 4) mask = _mm256_movemask_ps((__m256)cmp); // 8 bits in the mask, for 32-bit integer
        else if constexpr(sizeof(T) == 8) mask = _mm256_movemask_pd((__m256d)cmp); // 4 bits in the mask, for 64-bit integer
        
        int bitmap_pos = i / BITS_UINT64_T; // use bitmap_[bitmap_pos]
        int bit_pos = i % BITS_UINT64_T; // pos from MSB
        // get 8 bits from (bitmap_[bitmap_pos], bit_pos)
        unsigned char valid_bits = (unsigned char)((bitmap_[bitmap_pos] >> bit_pos) & 0xFF);

        mask &= valid_bits; // only keep the valid bits
        if (mask == 0) continue; // no match in this SIMD register

        int idx = i + __builtin_ctz(mask);
        value = list_.at(idx).value_;
        return true;
    }

    return false;
}

template<class LISTTYPE, typename T, typename V, size_t SIZE>
class Bucket<LISTTYPE, T, V, SIZE>::UnsortedIterator {
public:
    using BucketType = Bucket<LISTTYPE, T, V, SIZE>;

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
