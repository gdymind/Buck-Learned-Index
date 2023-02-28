#ifndef __BUCK_INDEX_H_
#define __BUCK_INDEX_H_

#include "../src/segment.h"
#include "../src/global.h"

template<class T, class V, size_t SBUCKET_SIZE, T MAX_KEY>
class BuckIndex {
public:
    BuckIndex() {}

    value_type lookup(T key) {
        value_type v;
        return v;
    }

    bool insert(KeyValue<T, V> kv) {
        return true;
    }

    bool bulk_load(KeyValue<T, V> *data, unsigned int n) {
        return true;
    }
private:
    bool adjust_segment(Segment<T, V, SBUCKET_SIZE, MAX_KEY> *old_seg) { //scale, run segmentation, and retrain the old_seg, and possibly split into multiple new Segment
    
        return true;
    }
};

#endif