#include "gtest/gtest.h"
#define UNITTEST
#include "buck_index.h"

#include <stdlib.h>
#include <time.h>
#include <unordered_set>

namespace buckindex {

    TEST(BuckIndex, bulk_load_basic) {
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;
        bli.init(0.5);
        #define BUCKINDEX_USE_LINEAR_REGRESSION
        #define BUCKINDEX_USE_SIMD
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

        // data buckets: [1,2], [3,4], [5,6], [7,8], [9,10]
        // first segment layer: root_ = [[1],[3],[5],[7],[9]]
        EXPECT_EQ(bli.get_num_keys(), length);
        EXPECT_EQ(bli.get_num_levels(), 2);
        EXPECT_EQ(bli.get_num_data_buckets(), 5);
        EXPECT_EQ(bli.get_level_stat(0), 5);
        EXPECT_EQ(bli.get_level_stat(1), 1);

        bli.dump();
    }

    TEST(BuckIndex, bulk_load_multiple_segments) {
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;
        bli.init(0.5);
        #define BUCKINDEX_USE_LINEAR_REGRESSION
        #define BUCKINDEX_USE_SIMD
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

        // data buckets: [1,2], [3,100], [110,200], [210,300], [305,1000], [1200,1300], [1400]
        // pivots: [1, 3, 110, 210, 305, 1200, 1400]
        // first segment layer: [1,3,110,210,305], [1200,1400]
        // second segment layer: root_ = [1,1200]
        vector<KeyValue<uint64_t, uint64_t>> pivot_kv_array;
        for (auto i = 0; i < length; i+=2) {
            pivot_kv_array.push_back(in_kv_array[i]);
        }
        vector<Cut<uint64_t>> cuts;
        vector<LinearModel<uint64_t>> models;
        #define BUCKINDEX_USE_LINEAR_REGRESSION
        Segmentation<vector<KeyValue<uint64_t, uint64_t>>, uint64_t>::compute_dynamic_segmentation(
            pivot_kv_array, cuts, models, 1);

        EXPECT_EQ(bli.get_num_keys(), length);
        EXPECT_EQ(bli.get_num_levels(), 3);
        EXPECT_EQ(bli.get_num_data_buckets(), 7);
        EXPECT_EQ(bli.get_level_stat(0), 7);
        EXPECT_EQ(bli.get_level_stat(1), cuts.size());
        EXPECT_EQ(bli.get_level_stat(2), 1);

        bli.dump();
    }

    TEST(BuckIndex, bulk_load_multiple_model_layers) {
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;
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
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;

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

        EXPECT_EQ(bli.get_num_keys(), length+1);
        EXPECT_EQ(bli.get_num_levels(), 2);
        EXPECT_EQ(bli.get_num_data_buckets(), 1);
        EXPECT_EQ(bli.get_level_stat(0), 1);
        EXPECT_EQ(bli.get_level_stat(1), 1);

        bli.dump();
    }
    TEST(BuckIndex, insert_perfectly_linear_keys) {
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;

        uint64_t key;
        uint64_t value;

        for (int i = 2; i < 1000; i += 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        bli.dump();
    }

    TEST(BuckIndex, insert_multi_segments) {
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;

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

        bli.dump();
    }


    TEST(BuckIndex, insert_reverse_order) {
        BuckIndex<uint64_t, uint64_t, 2, 4> bli;

        uint64_t key;
        uint64_t value;

        for (int i = 10; i >= 2; i -= 2) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        bli.dump();
    }


    TEST(BuckIndex, insert_random_order) {
        srand (time(NULL));

        BuckIndex<uint64_t, uint64_t, 2, 4> bli;

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

        bli.dump();
    }


    TEST(BuckIndex, scan_one_segment) {
        BuckIndex<uint64_t, uint64_t, 8, 64> bli(0.5);
        #define BUCKINDEX_USE_LINEAR_REGRESSION
        #define BUCKINDEX_USE_SIMD

        std::pair<uint64_t, uint64_t> *result;
        result = new std::pair<uint64_t, uint64_t>[1000];
        int n_result = 0;

        uint64_t key;
        uint64_t value;

        for (int i = 3; i < 1000; i += 3) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        uint64_t start_key = 122;
        size_t num_keys = 234;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(234, n_result);
        int idx = 123;
        for (int i = 0; i < n_result; i++) {
            auto &kv = result[i];
            EXPECT_EQ(idx, kv.first);
            EXPECT_EQ(idx * 2 + 5, kv.second);
            idx += 3;
        }

        start_key = 1;
        num_keys = 234;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(234, n_result);
        idx = 3;
        for (int i = 0; i < n_result; i++) {
            auto &kv = result[i];
            EXPECT_EQ(idx, kv.first);
            EXPECT_EQ(idx * 2 + 5, kv.second);
            idx += 3;
        }

        start_key = 10000;
        num_keys = 234;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(0, n_result);

        bli.dump();

        delete[] result;
    }

    TEST(BuckIndex, scan_multi_segment) {
        BuckIndex<uint64_t, uint64_t, 8, 16> bli(0.5);
        #define BUCKINDEX_USE_LINEAR_REGRESSION
        #define BUCKINDEX_USE_SIMD

        std::pair<uint64_t, uint64_t> *result;
        result = new std::pair<uint64_t, uint64_t>[1000];
        int n_result = 0;

        uint64_t key;
        uint64_t value;

        for (int i = 3; i < 1000; i += 3) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        for (int i = 100002; i < 100100; i += 3) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        uint64_t start_key = 122;
        size_t num_keys = 234;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(234, n_result);
        int idx = 123;
       for (int i = 0; i < n_result; i++) {
            auto &kv = result[i];
            EXPECT_EQ(idx, kv.first);
            EXPECT_EQ(idx * 2 + 5, kv.second);
            idx += 3;
        }

        start_key = 1;
        num_keys = 234;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(234, n_result);
        idx = 3;
        for (int i = 0; i < n_result; i++) {
            auto &kv = result[i];
            EXPECT_EQ(idx, kv.first);
            EXPECT_EQ(idx * 2 + 5, kv.second);
            idx += 3;
        }

        start_key = 990;
        num_keys = 10;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(10, n_result);
        EXPECT_EQ(990, result[0].first);
        EXPECT_EQ(990 * 2 + 5, result[0].second);
        EXPECT_EQ(993, result[1].first);
        EXPECT_EQ(993 * 2 + 5, result[1].second);
        EXPECT_EQ(996, result[2].first);
        EXPECT_EQ(996 * 2 + 5, result[2].second);
        EXPECT_EQ(999, result[3].first);
        EXPECT_EQ(999 * 2 + 5, result[3].second);
        EXPECT_EQ(100002, result[4].first);
        EXPECT_EQ(100002 * 2 + 5, result[4].second);
        EXPECT_EQ(100005, result[5].first);
        EXPECT_EQ(100005 * 2 + 5, result[5].second);
        EXPECT_EQ(100008, result[6].first);
        EXPECT_EQ(100008 * 2 + 5, result[6].second);
        EXPECT_EQ(100011, result[7].first);
        EXPECT_EQ(100011 * 2 + 5, result[7].second);
        EXPECT_EQ(100014, result[8].first);
        EXPECT_EQ(100014 * 2 + 5, result[8].second);
        EXPECT_EQ(100017, result[9].first);
        EXPECT_EQ(100017 * 2 + 5, result[9].second);

        // test scanning at the end, and the number of keys to be scan
        // is larger than the number of remaining keys in the index
        start_key = 100080;
        num_keys = 100;
        n_result = bli.scan(start_key, num_keys, result);
        EXPECT_EQ(7, n_result); // 10080, 10083, 10086, 10089, 10092, 10095, 10098
        idx = 100080;
        for (int i = 0; i < n_result; i++) {
            EXPECT_EQ(idx, result[i].first);
            EXPECT_EQ(idx * 2 + 5, result[i].second);
            idx += 3;
        }

        bli.dump();

        delete[] result;
    }

    TEST(BuckIndex, level_stat){
        BuckIndex<uint64_t, uint64_t, 8, 16> bli(0.5);
        #define BUCKINDEX_USE_LINEAR_REGRESSION
        #define BUCKINDEX_USE_SIMD

        std::pair<uint64_t, uint64_t> *result;
        result = new std::pair<uint64_t, uint64_t>[1000];
        int n_result = 0;

        uint64_t key;
        uint64_t value;

        for (int i = 3; i < 1000; i += 3) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        for (int i = 100002; i < 100100; i += 3) {
            KeyValue<uint64_t, uint64_t> kv = KeyValue<uint64_t, uint64_t>(i, i * 2 + 5);
            EXPECT_TRUE(bli.insert(kv));
            EXPECT_TRUE(bli.lookup(i, value));
            EXPECT_EQ(i * 2 + 5, value);
            EXPECT_FALSE(bli.lookup(i+1, value));
        }

        
        EXPECT_EQ(bli.get_num_levels(), 3);
        EXPECT_EQ(bli.get_level_stat(1), 2);
        EXPECT_EQ(bli.get_level_stat(2), 1);

        bli.dump();

        delete[] result;
    }
}
