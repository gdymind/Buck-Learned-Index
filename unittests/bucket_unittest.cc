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

    TEST(Bucket, insert_and_lookup) { //TODO: test insert with/without pivot updates
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        for (int i = 0; i < 8; i++) list.put(i, i, i);// TODO: valude different from the key; and check the returned value of lookup

        // initial size == 0
        EXPECT_EQ(0, bucket.num_keys());
        
        // lookup non-existing key
        EXPECT_EQ(false, bucket.lookup(0, value));
        
        // lookup existing/non-existing keys after single insertion
        bucket.insert(list.at(0), true);
        EXPECT_EQ(true, bucket.lookup(0, value));
        EXPECT_EQ(false, bucket.lookup(1, value));

        for (int i = 1; i < 8; i++) {
            EXPECT_EQ(true, bucket.insert(list.at(i), true));
            EXPECT_EQ(true, bucket.lookup(i, value)); // separate for loop
            EXPECT_EQ(i+1, bucket.num_keys());
        }

        // EXPECT_EQ(false, bucket.insert(list.at(0), true));
    }

    TEST(Bucket, find_kth_smallest) {
        Bucket<KeyValueList<key_t, value_t, 64>, key_t, value_t, 64> bucket;

        std::srand(std::time(nullptr));
        std::vector<key_t> keys;
        for (int i = 0; i < 50; i++) keys.push_back(i); // TODO: non-contiguous key
        std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < 50; i++) bucket.insert(KeyValue<key_t, value_t>(keys[i], 0), true);
            

        for (int i = 0; i < 50; i++) {
            EXPECT_EQ(i, bucket.find_kth_smallest(i+1));
        }
    }

    TEST(Bucket, unsorted_iterator) { //TODO: test insert with/without pivot updates
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // insert {12, 24, 28, 67, 98} unsorted
        list.put(0, 98, 12);
        list.put(1, 24, 35);
        list.put(2, 12, 62);
        list.put(3, 28, 18);
        list.put(4, 67, 12345678);

        for (int i = 0; i < 5; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true));
        }

        // test begin(), end(), opreator* and it++
        int i = 0;
        for (auto it = bucket.begin(); it != bucket.end(); it++) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(list.at(i).key_, kv.key_);
            EXPECT_EQ(list.at(i).value_, kv.value_);
            i++;
        }

        // test ++it basic usage
        i = 0;
        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(list.at(i).key_, kv.key_);
            EXPECT_EQ(list.at(i).value_, kv.value_);
            i++;
        }

        // test ++it return value
        i = 1;
        for (auto it = bucket.begin(); it != bucket.end() && i < 4;) {
            KeyValue<key_t, value_t> kv = *(++it);
            EXPECT_EQ(list.at(i).key_, kv.key_);
            EXPECT_EQ(list.at(i).value_, kv.value_);
            i++;
        }

        // test overflow
        auto it = bucket.end();
        it++;
        EXPECT_TRUE(it == bucket.end());
        ++it;
        EXPECT_TRUE(it == bucket.end());
    }
}
