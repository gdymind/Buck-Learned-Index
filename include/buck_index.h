#ifndef __BUCK_INDEX_H_
#define __BUCK_INDEX_H_

#include "../src/segment.h"
#include "../src/global.h"

class BuckIndex {
public:
    BuckIndex() {}

    value_type lookup(key_type key) {
        value_type v;
        return v;
    }

    bool insert(KVPTR kv) {
        return true;
    }

    bool bulk_load(KVPTR *data, unsigned int n) {
        return true;
    }
private:
    template<size_t SEG_SIZE>
    bool adjust_segment(Segment<SEG_SIZE> *old_seg) { //scale, run segmentation, and retrain the old_seg, and possibly split into multiple new Segment
    
        return true;
    }
};

#endif