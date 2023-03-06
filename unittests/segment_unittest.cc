#include "gtest/gtest.h"

#include "segment.h"

#include<vector>
#include<iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>


namespace buckindex {
    typedef unsigned long long key_t;
    typedef unsigned long long value_t;

    TEST(Segment, insert_and_lookup) {
        Segment<key_t, value_t, 8> seg1;
        Segment<key_t, value_t, 8> seg2(seg1);
    }

}
