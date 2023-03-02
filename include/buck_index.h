#pragma once

#include "../src/segment.h"
#include "../src/global.h"

namespace buckindex {

template<class T, class V, size_t SBUCKET_SIZE>
class BuckIndex {
public:
    BuckIndex() {}

    V lookup(T key) {
        V v;
        return v;
    }

    bool insert(KeyValue<T, V> kv) {
        return true;
    }

    bool bulk_load(KeyValue<T, V> *data, unsigned int n) {
        return true;
    }
private:
    bool adjust_segment(Segment<T, V, SBUCKET_SIZE> *old_seg) { //scale, run segmentation, and retrain the old_seg, and possibly split into multiple new Segment
    
        return true;
    }
};

} // end namespace buckindex
