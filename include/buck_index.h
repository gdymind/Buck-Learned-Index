#pragma once

#include "segment.h"
#include "util.h"

namespace buckindex {

template<typename T, typename V, size_t SBUCKET_SIZE>
class BuckIndex {
public:
    BuckIndex(T invalid_pivot) {
        invalid_pivot_ = invalid_pivot;
        // root_ = new Segment<T, V, SBUCKET_SIZE>();
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
    size_t num_levels_;
    T invalid_pivot_;
};

} // end namespace buckindex
