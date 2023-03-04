
#pragma once

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"
#include "model.h"

namespace buckindex {

template<class T, class V, size_t SBUCKET_SIZE>
class Segment {
public:
    bool is_leaf_; // true -> segment; false -> segment group
    size_t num_bucket_; // total num of buckets

    Segment* parent_ = nullptr; // the parent Segment node, which enables bottom-up tranversal
    T pivot_; // smallest element
    // T base; // key compression

    Bucket<class KeyValueList<T,V,SBUCKET_SIZE>, T, V, SBUCKET_SIZE>* sbucket_list_; // a list of S-Buckets


    V lookup(T key); //return the child pointer
    bool insert(KeyValue<T, V> kvptr); // insert an entry to the target S-Bucket; If the target S-Bucket is full, reblance the bucket with its right neighbor; If bucket_rebalance does not work, insert() return false

    
private:
    Model<T> model_;

    void train_model();

    inline unsigned int predict_buck(T key) { // get the predicted S-Bucket ID based on the model computing
        unsigned int buckID = (unsigned int)(model_.predict(key) / SBUCKET_SIZE + 0.5); //TODO: if SBUCKET_SIZE is the power of 2, change division to right shift
        buckID = std::min(buckID, (unsigned int)(num_bucket_-1));
        return buckID;
    }

    inline unsigned int locate_buck(T key) { // precition may be incorrect, this function is to find the exact bucket whose range covers the key based on prediction
        // Step1: call predict_buck to get an intial position
        // Step2: search neighbors to find the exact match
    }

    bool bucket_rebalance(unsigned int buckID0);

};


template<class T, class V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::bucket_rebalance(unsigned int buckID0) {
    unsigned int buckID1 = buckID0 +1;
    //TODO
    return true;
}

template<class T, class V, size_t SBUCKET_SIZE>
V Segment<T, V, SBUCKET_SIZE>::lookup(T key) {
    V ret;
    return ret;
}

template<class T, class V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::insert(KeyValue<T, V> kvptr) {
    return true;
}

} // end namespace buckindex
