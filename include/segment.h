
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


template<size_t SEG_SIZE>
class Segment {
public:
    size_t num_bucket; // total num of buckets
    key_type pivot; // smallest element
    // key_type base; // key compression

    Bucket<SEG_SIZE>* bucket_list;
    key_type* child_pivots;
    Segment* parent = nullptr;

    unsigned int predict(key_type key) {
        unsigned int buckID = (unsigned int)(model_.predict(key) / BUCKET_SIZE + 0.5);
        buckID = std::min(buckID, (unsigned int)(BUCKET_SIZE-1));
        return buckID;
    }


    value_type lookup();
    bool insert();
    bool add_bucket();
    bool bucket_rebalance(unsigned int buckID0);

private:
    Model model_;

};

template<size_t SEG_SIZE>
bool Segment<SEG_SIZE>::add_bucket() {
    return true;
}

template<size_t SEG_SIZE>
bool Segment<SEG_SIZE>::bucket_rebalance(unsigned int buckID0) {
    unsigned int buckID1 = buckID0 +1;
    return true;
}

template<size_t SEG_SIZE>
value_type Segment<SEG_SIZE>::lookup() {
    value_type ret;
    return ret;
}

template<size_t SEG_SIZE>
bool Segment<SEG_SIZE>::insert() {
    return true;
}

// template<size_t SEG_GROUP_SIZE>
// struct SegmentGroup
// {
//     Model m;
//     size_t num_bucket; // total num of buckets
//     size_t bucket_size; // size of each bucket
//     key_type pivot; // smallest element
//     // key_type base; // key compression

//     key_type* child_pivots;
//     Bucket<SEG_GROUP_SIZE>* bucket_list;
// };

#endif