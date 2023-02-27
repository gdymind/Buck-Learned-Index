
#ifndef __NODE_LAYOUT_H_
#define __NODE_LAYOUT_H_

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"

template<class T>
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
    
    inline int predict(T key) const {
        return static_cast<int>(a * static_cast<double>(key) + b);
    }

    inline double predict_double(T key) const {
        return a * static_cast<double>(key) + b;
    }
};


template<class T, size_t SBUCKET_SIZE, T MAX_KEY>
class Segment {
public:
    bool is_leaf_; // true -> segment; false -> segment group
    size_t num_bucket_; // total num of buckets

    Segment* parent_ = nullptr; // the parent Segment node, which enables bottom-up tranversal
    T pivot_; // smallest element
    // T base; // key compression

    Bucket<T, SBUCKET_SIZE, MAX_KEY>* sbucket_list_; // a list of S-Buckets


    uint64_t* lookup(T key); //return the child pointer
    bool insert(KVPTR<T> kvptr); // insert an entry to the target S-Bucket; If the target S-Bucket is full, reblance the bucket with its right neighbor; If bucket_rebalance does not work, insert() return false

    
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


template<class T, size_t SBUCKET_SIZE, T MAX_KEY>
bool Segment<T, SBUCKET_SIZE, MAX_KEY>::bucket_rebalance(unsigned int buckID0) {
    unsigned int buckID1 = buckID0 +1;
    //TODO
    return true;
}

template<class T, size_t SBUCKET_SIZE, T MAX_KEY>
uint64_t* Segment<T, SBUCKET_SIZE, MAX_KEY>::lookup(T key) {
    value_type ret;
    return ret;
}

template<class T, size_t SBUCKET_SIZE, T MAX_KEY>
bool Segment<T, SBUCKET_SIZE, MAX_KEY>::insert(KVPTR<T> kvptr) {
    return true;
}

#endif