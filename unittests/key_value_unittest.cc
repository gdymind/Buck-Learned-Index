#include "gtest/gtest.h"

#include "keyvalue.h"

#include<vector>
#include<iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>

namespace buckindex {
    // TEST(KeyValue, operator_less_than) {
    //     std::vector<KeyValue<int, int>> list;

    //     // insert 12, 24, 28, 67, 98 unsorted
    //     list.push_back(KeyValue<int, int>(98, 12));
    //     list.push_back(KeyValue<int, int>(24, 35));
    //     list.push_back(KeyValue<int, int>(12, 62));
    //     list.push_back(KeyValue<int, int>(28, 18));
    //     list.push_back(KeyValue<int, int>(67, 12345678));

    //     EXPECT_EQ(98, list[0].key_);
    //     EXPECT_EQ(24, list[1].key_);
    //     EXPECT_EQ(12, list[2].key_);
    //     EXPECT_EQ(28, list[3].key_);
    //     EXPECT_EQ(67, list[4].key_);

    //     EXPECT_EQ(12, list[0].value_);
    //     EXPECT_EQ(35, list[1].value_);
    //     EXPECT_EQ(62, list[2].value_);
    //     EXPECT_EQ(18, list[3].value_);
    //     EXPECT_EQ(12345678, list[4].value_);

    //     // sort the vector
    //     sort(list.begin(), list.end());

    //     // check if sorted
    //     EXPECT_EQ(12, list[0].key_);
    //     EXPECT_EQ(24, list[1].key_);
    //     EXPECT_EQ(28, list[2].key_);
    //     EXPECT_EQ(67, list[3].key_);
    //     EXPECT_EQ(98, list[4].key_);

    //     EXPECT_EQ(62, list[0].value_);
    //     EXPECT_EQ(35, list[1].value_);
    //     EXPECT_EQ(18, list[2].value_);
    //     EXPECT_EQ(12345678, list[3].value_);
    //     EXPECT_EQ(12, list[4].value_);
  
    // }
}