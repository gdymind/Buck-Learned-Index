
#ifndef __NODE_LAYOUT_H_
#define __NODE_LAYOUT_H_

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"


class Model {
public:
    double a = 0.0;
    double b = 0.0;
    
    Model() = default;
    Model(double a, double b) : a(a), b(b) {}
    Model(const Model& other) : a(other.a), b(other.b) {}

    void expand(double expansion_factor) {
        a *= expansion_factor;
        b *= expansion_factor;
    }
    
    inline int predict(key_type key) const {
        return static_cast<int>(a * static_cast<double>(key) + b);
    }

    inline double predict_double(key_type key) const {
        return a * static_cast<double>(key) + b;
    }
};


template<size_t SBUCKET_SIZE>
class Segment {
public:
    bool is_leaf; // true -> segment; false -> segment group
    size_t num_bucket; // total num of buckets

    // Segment* parent = nullptr; // the parent Segment node, which enables bottom-up tranversal
    key_type pivot; // smallest element
    // key_type base; // key compression

    Bucket<SBUCKET_SIZE>* sbucket_list; // a list of S-Buckets


    uint64_t* lookup(key_type key); //return the child pointer
    bool insert(KVPTR kvptr); // insert an entry to the target S-Bucket; If the target S-Bucket is full, reblance the bucket with its right neighbor; If bucket_rebalance does not work, insert() return false

    
private:
    Model model_;

    void train_model();

    inline unsigned int predict_buck(key_type key) { // get the predicted S-Bucket ID based on the model computing
        unsigned int buckID = (unsigned int)(model_.predict(key) / SBUCKET_SIZE + 0.5); //TODO: if SBUCKET_SIZE is the power of 2, change division to right shift
        buckID = std::min(buckID, (unsigned int)(num_bucket-1));
        return buckID;
    }

    inline unsigned int locate_buck(key_type key) { // precition may be incorrect, this function is to find the exact bucket whose range covers the key based on prediction
        // Step1: call predict_buck to get an intial position
        // Step2: search neighbors to find the exact match
    }

    bool bucket_rebalance(unsigned int buckID0);

};


template<size_t BUCKET_SIZE>
bool Segment<BUCKET_SIZE>::bucket_rebalance(unsigned int buckID0) {
    unsigned int buckID1 = buckID0 +1;
    //TODO
    return true;
}

template<size_t BUCKET_SIZE>
uint64_t* Segment<BUCKET_SIZE>::lookup(key_type key) {
    value_type ret;
    return ret;
}

template<size_t BUCKET_SIZE>
bool Segment<BUCKET_SIZE>::insert(KVPTR kvptr) {
    return true;
}

#endif