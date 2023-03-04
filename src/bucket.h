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

    V lookup(T key); // D-Bucket lookup returns a value; S-Bucket lookup returns a pointer
    V lookup_SIMD(T key);
    bool insert(KeyValue<T, V> kvptr); // Return false if insert() fails

    size_t num_keys() {
        size_t cnt = 0;
        for (int i = 0; i < SIZE/BITS_UINT64_T; i++) {
            cnt += __builtin_popcountll(bitmap_[i]);
        }
        return cnt;
    }

    T find_kth_smallest(int k);

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
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = pos - (bitmap_pos * BITS_UINT64_T);
        bitmap_[bitmap_pos] |= (1U << bit_pos);
    }

    inline void reset_bit(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = pos - (bitmap_pos * BITS_UINT64_T);
        bitmap_[bitmap_pos] &= ~(1U << bit_pos);
    } 

    inline bool read_bit(int pos) {
        assert(pos >= 0 && pos < SIZE);
        int bitmap_pos = pos / BITS_UINT64_T;
        int bit_pos = pos - (bitmap_pos * BITS_UINT64_T);
        return (bitmap_[bitmap_pos]  & (1U << bit_pos)) != 0;
    }

private:
    uint64_t bitmap_[SIZE/BITS_UINT64_T];  //indicate whether the entries in the list_ are valid.
   
    T pivot_;
    LISTTYPE list_;

    inline KeyValue<T, V> at(int pos) { return list_.at(pos); }

    // helper function for find_kth_smallest()
    int quickselect_partiton(vector<T>& a, int left, int right, int pivot) {
        int pivotValue = a[pivot];
        swap(a[pivot], a[right]);  // Move pivot to end
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (a[i] < pivotValue) {
                swap(a[i], a[storeIndex]);
                storeIndex++;
            }
        }
        swap(a[storeIndex], a[right]);  // Move pivot to its final place
        return storeIndex;
    }

    // helper function for find_kth_smallest()
    int quickselect(vector<T>& a, int left, int right, int k) {
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
V Bucket<LISTTYPE, T, V, SIZE>::lookup(T key_) {
    //TODO: how to indicate the key_ does not exist?
    // For S-Bucket, it's easy as the return value is a pointer, so just return nullptr when key_ does not exist
    // But for D-Bucket, any value are possible
    // We may change to interface from `V lookup(T key_)` to `bool lookup(T key_, V &value)`, and use the return value to indicate if the key_ exists
    
    for (int i = 0; i < SIZE; i++) {
        if (read_bit(i) && list_.at(i).key_ == key_) {
            return list_.at(i).value_;
        }
    }
    
    return nullptr;
}


template<class LISTTYPE, class T, class V, size_t SIZE>
V Bucket<LISTTYPE, T, V, SIZE>::lookup_SIMD(T key_) {
    // TODO
    return nullptr;
}

template<class T, class V, size_t SIZE>
class Bucket<KeyListValueList<T, V, SIZE>, T, V, SIZE> {
public:
    V lookup_SIMD(T key) {
        //TODO
    }
};

template<class T, class V, size_t SIZE>
class Bucket<KeyValueList<T, V, SIZE>, T, V, SIZE> {
public:
    V lookup_SIMD(T key) {
        //TODO
    }
};

template<class LISTTYPE, class T, class V, size_t SIZE>
bool Bucket<LISTTYPE, T, V, SIZE>::insert(KeyValue<T, V> kvptr) {
    int pos = find_first_zero_bit();
    if (pos == -1) return false; // return false if the Bucket is already full
    list_.put(pos, kvptr.key_, kvptr.value_);
    set_bit(pos);

    if (kvptr.key_ < pivot_) { pivot_ = kvptr.key_; } // [Be careful!] in the future devlopment/designs,
                                                      // alway make sure it's safe to update pivot_ after set_bit()

    return true;
}


template<class LISTTYPE, class T, class V, size_t SIZE>
T Bucket<LISTTYPE, T, V, SIZE>::find_kth_smallest(int k) {
    int n = num_keys();
    assert(k >= 0 && k < n);

    vector<T> valid_keys(n);
    int id = 0;
    for (int i = 0; i < SIZE; i++) {
        if (read_bit(i)) valid_keys[id++] = list_.at(i);
    }
    assert(id == n);
    
    return quickselect(valid_keys, 0, n-1, k);
}

} // end namespace buckindex
