#include "gtest/gtest.h"
#define UNITTEST
#include "buck_index.h"

#include <stdlib.h>
#include <time.h>
#include <unordered_set>

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
        EXPECT_EQ(length/2 + (length & 1), bli.get_num_data_buckets());

        for (auto i = 0; i < length; i++) {
            auto result = bli.lookup(keys[i], value);
            EXPECT_TRUE(result);
            EXPECT_EQ(values[i], value);
        }

        EXPECT_EQ(2, bli.get_num_levels());
    }

    TEST(BuckIndex, bulk_load_multiple_segments) {
        BuckIndex<uint64_t, uint64_t> bli;
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        uint64_t keys[] = {1,2,3,100,110,200,210,300,305,1000,1200,1300,1400}; // 13 keys
        uint64_t values[] = {10,20,30,1000,1100,2000,2100,3000,3050,10000,12000,13000,14000};
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        uint64_t value;
        for (auto i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], values[i]));
        }
        bli.bulk_load(in_kv_array);
        EXPECT_EQ(length/2 + (length & 1), bli.get_num_data_buckets());

        for (auto i = 0; i < length; i++) {
            auto result = bli.lookup(keys[i], value);
            EXPECT_TRUE(result);
            EXPECT_EQ(values[i], value);
        }
    }

    TEST(BuckIndex, bulk_load_multiple_model_layers) {
        BuckIndex<uint64_t, uint64_t> bli;
        vector<KeyValue<uint64_t, uint64_t>> in_kv_array;
        uint64_t keys[] = {1,2,3,100,110,200,210,300,305,1000,1200,1300,1400,10000, 10001, 10002, 10003}; // 17 keys
        uint64_t values[] = {10,20,30,1000,1100,2000,2100,3000,3050,10000,12000,13000,14000,100000,100010,100020,100030};
        uint64_t length = sizeof(keys)/sizeof(uint64_t);
        uint64_t value;
        for (auto i = 0; i < length; i++) {
            in_kv_array.push_back(KeyValue<uint64_t, uint64_t>(keys[i], values[i]));
        }
        bli.bulk_load(in_kv_array);
        EXPECT_EQ(length/2 + (length & 1), bli.get_num_data_buckets());

        for (auto i = 0; i < length; i++) {
            auto result = bli.lookup(keys[i], value);
            EXPECT_TRUE(result);
            EXPECT_EQ(values[i], value);
        }
    }

    TEST(BuckIndex, insert_from_empty) {
        BuckIndex<uint64_t, uint64_t> bli;

        uint64_t keys[] = {3, 5};
        uint64_t values[] = {32, 52};
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

        bli.dump();
    }
    TEST(BuckIndex, insert_perfectly_linear_keys) {
        BuckIndex<uint64_t, uint64_t> bli;

        uint64_t key;
        uint64_t value;

        for (int i = 2; i < 1000; i += 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }
    }

    TEST(BuckIndex, insert_multi_segments) {
        BuckIndex<uint64_t, uint64_t> bli;

        uint64_t key;
        uint64_t value;

        for (int i = 2; i < 200; i += 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }


        for (int i = 2000; i < 2000+200; i += 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        for (int i = 8000; i < 8000+200; i += 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        for (int i = 10000; i < 10000+2000; i += 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }
    }


    TEST(BuckIndex, insert_reverse_order) {
        BuckIndex<uint64_t, uint64_t> bli;

        uint64_t key;
        uint64_t value;

        for (int i = 10; i >= 2; i -= 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }
    }


    TEST(BuckIndex, insert_random_order) {
        srand (time(NULL));

        BuckIndex<uint64_t, uint64_t> bli;

        uint64_t key;
        uint64_t value;

        const int N = 10000;

        std::unordered_set<uint64_t> keys_set;
        std::vector<uint64_t> keys(N);
        for (int i = 0; i < N; i++) {
            int key = rand() % 10000000;
            while (keys_set.find(key) != keys_set.end()) {
                key = rand() % 10000000;
            }
            keys[i] = (key);
        }

        for (auto key: keys) {
            // std::cout << "Inserting key = " << key << "\n" << std::flush;
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(key, key * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(key, value));
            EXPECT_EQ(key * 2 + 5, value);
            if (value != key * 2 + 5) break;
            // std::cout << std::endl << std::flush;
        }

        // for (auto key: keys) {
        //     EXPECT_TRUE(bli.lookup(key, value));
        //     EXPECT_EQ(key * 2 + 5, value);
        // }

        EXPECT_FALSE(bli.lookup(1000000000, value));
    }
}
