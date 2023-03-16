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
    using KV = KeyValue<key_t, value_t>;
    using KVList8 = KeyValueList<key_t, value_t, 8>;

    TEST(Bucket, lb_lookup) {
        Bucket<KVList8, key_t, value_t, 8> bucket;
        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // test empty buckets
        EXPECT_EQ(false, bucket.lb_lookup(0, value));
        EXPECT_EQ(false, bucket.lb_lookup(10, value));
        EXPECT_EQ(false, bucket.lb_lookup(2898509, value));

        // insert {12, 24, 28, 67, 98} unsorted
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(98, 12), true));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(24, 35), true));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(12, 62), true));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(28, 18), true));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(67, 12345678), true));

        // test keys < pivot
        for (int i = 0; i < 11; i++) EXPECT_EQ(false, bucket.lb_lookup(i, value));

        // test (12, 62)
        for (int i = 12; i < 24; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, value));
            EXPECT_EQ(62, value);
        }

        // test (24, 35)
        for (int i = 24; i < 28; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, value));
            EXPECT_EQ(35, value);
        }

        // test (28, 18)
        for (int i = 28; i < 67; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, value));
            EXPECT_EQ(18, value);
        }       

        // test (67, 12345678)
        for (int i = 67; i < 98; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, value));
            EXPECT_EQ(12345678, value);
        }

        // test (98, 12)
        for (int i = 98; i < 200; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, value));
            EXPECT_EQ(12, value);
        }
    }

    TEST(Bucket, lookup_insert_basic) {
        Bucket<KVList8, key_t, value_t, 8> bucket;
        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // keys =   {0, 1, 2, 3, 4, 5,  6,  7}
        // values = {1, 3, 5, 7, 9, 11, 13, 15}
        for (int i = 0; i < 8; i++) list.put(i, i, i * 2 + 1);

        // initial size == 0
        EXPECT_EQ(0, bucket.num_keys());
        
        // lookup non-existing key
        EXPECT_FALSE(bucket.lookup(0, value));
        
        // lookup existing/non-existing keys after single insertion
        EXPECT_TRUE(bucket.insert(list.at(0), true));
        EXPECT_TRUE(bucket.lookup(0, value));
        EXPECT_FALSE(bucket.lookup(1, value));

        // check "lookup return values" and "num_keys after insertion"
        for (int i = 1; i < 8; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true));
            EXPECT_TRUE(bucket.lookup(i, value));
            EXPECT_EQ(i * 2 + 1, value);
            EXPECT_EQ(i+1, bucket.num_keys());
        }

        // insert overflows
        EXPECT_FALSE(bucket.insert(list.at(0), true));
    }

    TEST(Bucket, insert_pivot_update) {
        Bucket<KVList8, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // test initial pivot
        EXPECT_EQ(ULLONG_MAX, bucket.get_pivot());
        EXPECT_EQ(0, bucket.num_keys());

        // test pivot update
        EXPECT_TRUE(bucket.insert(KV(82, 0), true));
        EXPECT_EQ(1, bucket.num_keys());
        EXPECT_EQ(82, bucket.get_pivot());

        // insert keys > pivot
        EXPECT_TRUE(bucket.insert(KV(98, 0), true));
        EXPECT_EQ(82, bucket.get_pivot());
        EXPECT_EQ(2, bucket.num_keys());

        EXPECT_TRUE(bucket.insert(KV(1000, 0), true));
        EXPECT_EQ(82, bucket.get_pivot());
        EXPECT_EQ(3, bucket.num_keys());

        // insert keys < pivot
        EXPECT_TRUE(bucket.insert(KV(53, 0), true));
        EXPECT_EQ(4, bucket.num_keys());
        EXPECT_EQ(53, bucket.get_pivot());

        EXPECT_TRUE(bucket.insert(KV(46, 0), true));
        EXPECT_EQ(5, bucket.num_keys());
        EXPECT_EQ(46, bucket.get_pivot());

        // test the 'true' arugment
        EXPECT_TRUE(bucket.insert(KV(40, 0), true));
        EXPECT_EQ(6, bucket.num_keys());
        EXPECT_EQ(40, bucket.get_pivot());    

        // test the 'false' argument
        EXPECT_TRUE(bucket.insert(KV(30, 0), false));
        EXPECT_EQ(7, bucket.num_keys());
        EXPECT_EQ(40, bucket.get_pivot());    

        EXPECT_TRUE(bucket.insert(KV(25, 0), true));
        EXPECT_EQ(8, bucket.num_keys());
        EXPECT_EQ(25, bucket.get_pivot());

        //test overflow  
        EXPECT_FALSE(bucket.insert(KV(31, 0), true));
        EXPECT_FALSE(bucket.insert(KV(32, 0), false));
    }

    TEST(Bucket, find_kth_smallest) {
        Bucket<KeyValueList<key_t, value_t, 64>, key_t, value_t, 64> bucket;

        // insert 50 unsorted keys
        std::srand(std::time(nullptr));
        std::vector<key_t> keys;
        for (int i = 0; i < 50; i++) keys.push_back(i * 4 + 12);
        std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < 50; i++) EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(keys[i], keys[i] + 123456), true));

        for (int i = 0; i < 50; i++) {
            EXPECT_EQ(i * 4 + 12, bucket.find_kth_smallest(i+1));
        }
    }

    // TEST(Bucket, unsorted_iterator) {
    //     Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

    //     KeyListValueList<key_t, value_t, 8> list;
    //     key_t key;
    //     value_t value;

    //     // insert {12, 24, 28, 67, 98} unsorted
    //     list.put(0, 98, 12);
    //     list.put(1, 24, 35);
    //     list.put(2, 12, 62);
    //     list.put(3, 28, 18);
    //     list.put(4, 67, 12345678);

    //     // test empty bucket
    //     EXPECT_TRUE(bucket.begin() == bucket.end());

    //     for (int i = 0; i < 5; i++) {
    //         EXPECT_TRUE(bucket.insert(list.at(i), true));
    //     }


    //     // test begin(), end(), opreator* and it++
    //     int i = 0;
    //     for (auto it = bucket.begin(); it != bucket.end(); it++) {
    //         KeyValue<key_t, value_t> kv = *it;
    //         EXPECT_EQ(list.at(i).key_, kv.key_);
    //         EXPECT_EQ(list.at(i).value_, kv.value_);
    //         i++;
    //     }
    //     EXPECT_EQ(5, i); 

    //     // test ++it basic usage
    //     i = 0;
    //     for (auto it = bucket.begin(); it != bucket.end(); ++it) {
    //         KeyValue<key_t, value_t> kv = *it;
    //         EXPECT_EQ(list.at(i).key_, kv.key_);
    //         EXPECT_EQ(list.at(i).value_, kv.value_);
    //         i++;
    //     }
    //     EXPECT_EQ(5, i); 


    //     // test ++it return value
    //     i = 1;
    //     for (auto it = bucket.begin(); it != bucket.end() && i < 4;) {
    //         KeyValue<key_t, value_t> kv = *(++it);
    //         EXPECT_EQ(list.at(i).key_, kv.key_);
    //         EXPECT_EQ(list.at(i).value_, kv.value_);
    //         i++;
    //     }
    //     EXPECT_EQ(4, i); 

    //     // test overflow
    //     auto it = bucket.end();
    //     it++;
    //     EXPECT_TRUE(it == bucket.end());
    //     ++it;
    //     EXPECT_TRUE(it == bucket.end());
    // }
}
