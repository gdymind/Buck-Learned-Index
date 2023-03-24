#include "gtest/gtest.h"
#define UNITTEST
#include "buck_index.h"

namespace buckindex {

    TEST(BuckIndex, bulk_load_basic) {
        BuckIndex<uint64_t, uint64_t> bli;
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        uint64_t keys[] = {1,2,3,4,5,6,7,8,9,10};
        uint64_t values[] = {11, 12, 13, 14, 15, 16, 17, 18, 19,20};
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        uint64_t value;
        for (auto i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], values[i]));
        }
        bli.bulk_load(in_kv_array);

        for (auto i = 0; i < length; i++) {
            auto result = bli.lookup(keys[i], value);
            EXPECT_TRUE(result);
            EXPECT_EQ(values[i], value);
        }
    }

    TEST(BuckIndex, bulk_load_multiple_segments) {
        BuckIndex<uint64_t, uint64_t> bli;
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        uint64_t keys[] = {1,2,3,100,110,200,210,300,305,1000,1200,1300,1400};
        uint64_t values[] = {10,20,30,1000,1100,2000,2100,3000,3050,10000,12000,13000,14000};
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        uint64_t value;
        for (auto i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], values[i]));
        }
        bli.bulk_load(in_kv_array);

        for (auto i = 0; i < length; i++) {
            auto result = bli.lookup(keys[i], value);
            EXPECT_TRUE(result);
            EXPECT_EQ(values[i], value);
        }
    }

    TEST(BuckIndex, bulk_load_multiple_model_layers) {
        BuckIndex<uint64_t, uint64_t> bli;
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        uint64_t keys[] = {1,2,3,100,110,200,210,300,305,1000,1200,1300,1400,10000, 10001, 10002, 10003};
        uint64_t values[] = {10,20,30,1000,1100,2000,2100,3000,3050,10000,12000,13000,14000,100000,100010,100020,100030};
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        uint64_t value;
        for (auto i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], values[i]));
        }
        bli.bulk_load(in_kv_array);

        for (auto i = 0; i < length; i++) {
            auto result = bli.lookup(keys[i], value);
            EXPECT_TRUE(result);
            EXPECT_EQ(values[i], value);
        }
    }

    TEST(BuckIndex, insert_from_empty) {
        BuckIndex<uint64_t, uint64_t> bli;

        uint64_t keys[] = {3, 5, 8, 4};
        uint64_t values[] = {32, 52, 82, 42};
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        uint64_t value;
        vector<KeyValue<uint64_t, uint64_t>> list;
        for (auto i = 0; i < length; i++) {
            list.push_back(KeyValue<uint64_t, uint64_t>(keys[i], values[i]));
        }

        for (auto i = 0; i < 2; i++) {
            EXPECT_TRUE(bli.insert(list[i]));
            EXPECT_TRUE(bli.lookup(list[i].key_, value));
            EXPECT_EQ(list[i].value_, value);
        }

        //TODO
    }


}
