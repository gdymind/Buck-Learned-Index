
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

class Segment {
    Model m;
    size_t num_bucket; // total num of buckets
    key_type pivot; // smallest element
    // key_type base; // key compression

    Bucket* bucket_list;
    key_type* child_pivots;

    unsigned int predict(key_type key) {
        unsigned int buckID = (m.predict(key) / BUCKET_SIZE + 0.5);
        buckID = std::min(buckID, (unsigned int)(BUCKET_SIZE-1));
    }

    bool add_bucket();
    bool split();
};


bool Segment::add_bucket() {

}

bool split() {
    
}

struct SegmentGroup
{
    Model m;
    size_t num_bucket; // total num of buckets
    size_t bucket_size; // size of each bucket
    key_type pivot; // smallest element
    // key_type base; // key compression

    SBucket* bucket_list;
    key_type* child_pivots;
};

#endif