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
    bool adjust_segment(Segment<SEG_SIZE> *old_seg, vector<Segment<SEG_SIZE> *> &new_segs) { //scale, run segmentation, and retrain
                                                                                             //TODO: how to seg new_segs with different SEG_SIZE?
        return true;
    }
};

#endif