#include "gtest/gtest.h"

#include "bucket.h"

#include<vector>
#include<iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>


namespace buckindex {
    typedef unsigned long long key_t;
    typedef unsigned long long value_t;

    TEST(Bucket, copy) {
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        for (int i = 0; i < 8; i++) list.put(i, i * 12, i * 2);

        for (int i = 0; i < 8; i++) {
            bucket.insert(list.at(i), true);
        }

        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket2;
        bucket2.copy(bucket);

        for (int i = 0; i < 8; i++) {
            EXPECT_EQ(bucket.at(i).key_, bucket2.at(i).key_);
        }
    }

    TEST(Bucket, lb_lookup) {
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        for (int i = 0; i < 8; i++) list.put(i, (i+1) * 10, (i+1) * 10);
        for (int i = 0; i < 8; i++) {
            bucket.insert(list.at(i), true);
        }

        for (int i = 0; i < 10; i++) {
            EXPECT_EQ(false, bucket.lb_lookup(i, value));
        }

        for (int i = 10; i <= 80; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, value));
            EXPECT_EQ(i/10*10, value);
        }
    }

    TEST(Bucket, insert_and_lookup) {
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        for (int i = 0; i < 8; i++) list.put(i, i, i);

        // initial size == 0
        EXPECT_EQ(0, bucket.num_keys());
        
        // lookup non-existing key
        EXPECT_EQ(false, bucket.lookup(0, value));
        
        // lookup existing/non-existing keys after single insertion
        bucket.insert(list.at(0));
        EXPECT_EQ(true, bucket.lookup(0, value));
        EXPECT_EQ(false, bucket.lookup(1, value));

        for (int i = 1; i < 8; i++) {
            EXPECT_EQ(true, bucket.insert(list.at(i)));
            EXPECT_EQ(true, bucket.lookup(i, value));
            EXPECT_EQ(i+1, bucket.num_keys());
        }

        // EXPECT_EQ(false, bucket.insert(list.at(0)));
    }

    TEST(Bucket, find_kth_smallest) {
        Bucket<KeyValueList<key_t, value_t, 64>, key_t, value_t, 64> bucket;

        std::srand(std::time(nullptr));
        std::vector<key_t> keys;
        for (int i = 0; i < 50; i++) keys.push_back(i);
        std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < 50; i++) bucket.insert(KeyValue<key_t, value_t>(keys[i], 0));
            

        for (int i = 0; i < 50; i++) {
            EXPECT_EQ(i, bucket.find_kth_smallest(i+1));
        }
    }

}