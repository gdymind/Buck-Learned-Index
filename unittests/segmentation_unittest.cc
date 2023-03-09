#include "gtest/gtest.h"
#include "keyvalue.h"
#include "segmentation.h"

namespace buckindex {
    TEST(Segmentation, no_segment) {
        uint64_t keys[] = {};
        uint64_t error_bound = 1;
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        vector<Cut<uint64_t>> cuts;

        Segmentation<vector<KeyValue<uint64_t, uint64_t>>, uint64_t>::compute_dynamic_segmentation(
                in_kv_array, cuts, error_bound);
        EXPECT_EQ(0u, cuts.size());
    }

    TEST(Segmentation, one_segment) {
        uint64_t keys[] = {0,1,2,3,4,5,6,7,8,9,10};
        uint64_t error_bound = 1;
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        vector<Cut<uint64_t>> cuts;

        for (uint64_t i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], keys[i]));
        }
        Segmentation<vector<KeyValue<uint64_t, uint64_t>>, uint64_t>::compute_dynamic_segmentation(
            in_kv_array, cuts, error_bound);
        EXPECT_EQ(1u, cuts.size());
        EXPECT_EQ(0u, cuts[0].start_);
        EXPECT_EQ(11u, cuts[0].size_);
        LinearModel<uint64_t> m(cuts[0].get_model());
        EXPECT_EQ(1u, m.get_slope());
        EXPECT_EQ(0u, m.get_offset());
    }

    TEST(Segmentation, multiple_segments) {
        uint64_t keys[] = {0,1,2,2,2,2,6,7,8,9,10};
        uint64_t error_bound = 1;
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        vector<Cut<uint64_t>> cuts;

        for (uint64_t i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], keys[i]));
        }
        Segmentation<vector<KeyValue<uint64_t, uint64_t>>, uint64_t>::compute_dynamic_segmentation(
            in_kv_array, cuts, error_bound);

        EXPECT_EQ(4u, cuts.size());
        /*Expected cuts: 0,1,2|2,2|2,6,7|8,9,10*/
        EXPECT_EQ(0u, cuts[0].start_);
        EXPECT_EQ(3u, cuts[0].size_);
        LinearModel<uint64_t> m(cuts[0].get_model());
        EXPECT_EQ(1u, m.get_slope());
        EXPECT_EQ(0u, m.get_offset());
        EXPECT_EQ(3u, cuts[1].start_);
        EXPECT_EQ(2u, cuts[1].size_);
        LinearModel<uint64_t> m2(cuts[1].get_model());
        EXPECT_EQ(0u, m2.get_slope());
        EXPECT_EQ(0u, m2.get_offset());
        EXPECT_EQ(5u, cuts[2].start_);
        EXPECT_EQ(3u, cuts[2].size_);
        EXPECT_EQ(8u, cuts[3].start_);
        EXPECT_EQ(3u, cuts[3].size_);
    }

    TEST(Segmentation, fixed_segmentation) {
        uint64_t keys[] = {0,1,2,2,2,2,6,7,8,9,10};
        uint64_t error_bound = 3;
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        vector<Cut<uint64_t>> cuts;

        for (uint64_t i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], keys[i]));
        }
        Segmentation<vector<KeyValue<uint64_t, uint64_t>>, uint64_t>::compute_fixed_segmentation(
            in_kv_array, cuts, error_bound);
        EXPECT_EQ(4u, cuts.size());
        /*Expected cuts: 0,1,2,|2,2,2|6,7,8,|9,10*/
        EXPECT_EQ(0u, cuts[0].start_);
        EXPECT_EQ(3u, cuts[0].size_);
        EXPECT_EQ(3u, cuts[1].start_);
        EXPECT_EQ(3u, cuts[1].size_);
        EXPECT_EQ(6u, cuts[2].start_);
        EXPECT_EQ(3u, cuts[2].size_);
        EXPECT_EQ(9u, cuts[3].start_);
        EXPECT_EQ(2u, cuts[3].size_);
    }

}
