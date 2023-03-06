#pragma once

#include "segment.h"
#include "global.h"

namespace buckindex {

template<class T, class V, size_t SBUCKET_SIZE>
class BuckIndex {
public:
    BuckIndex() {
        root_ = new Segment<T, V, SBUCKET_SIZE>();
    }

    ~BuckIndex() {
        delete root_;
    }

    bool lookup(T key, V &value) {
        // TODO
        // Step1: tranverse segments from root to leaf
        // Step2: lookup in the leaf D-Bucket
        return false;
    }

    bool insert(KeyValue<T, V> kv) {
        return true;
    }

    bool bulk_load(KeyValue<T, V> *data, unsigned int num) {
        return true;
    }
private:
    bool adjust_segment(Segment<T, V, SBUCKET_SIZE> *old_seg) { //scale, run segmentation, and retrain the old_seg, and possibly split into multiple new Segment
    
        return true;
    }

    Segment<T, V, SBUCKET_SIZE> *root_;
};

} // end namespace buckindex
