#include "gtest/gtest.h"

#include "bucket.h"

#include<vector>
#include<iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>

#define BUCKINDEX_DEBUG


namespace buckindex {
    typedef unsigned long long key_t;
    typedef unsigned long long value_t;
    using KV = KeyValue<key_t, value_t>;
    using KVList8 = KeyValueList<key_t, value_t, 8>;
    using KListVList8 = KeyListValueList<key_t, value_t, 8>;

    TEST(Bucket, lb_lookup) {
        Bucket<KVList8, key_t, value_t, 8> bucket;
        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;
        KeyValue<key_t, value_t> kv;
        KeyValue<key_t, value_t> kv2;

        // test empty buckets
        EXPECT_EQ(false, bucket.lb_lookup(0, kv, kv2));
        EXPECT_EQ(false, bucket.lb_lookup(10, kv, kv2));
        EXPECT_EQ(false, bucket.lb_lookup(2898509, kv, kv2));

        // insert {12, 24, 28, 67, 98} unsorted
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(98, 12), true, 0));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(24, 35), true, 0));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(12, 62), true, 0));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(28, 18), true, 0));
        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(67, 12345678), true, 0));


        EXPECT_EQ(false, bucket.lb_lookup(0, kv, kv2));

        // test keys < pivot
        for (int i = 0; i < 11; i++) EXPECT_EQ(false, bucket.lb_lookup(i, kv, kv2));

        // test (12, 62)
        for (int i = 12; i < 24; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, kv, kv2));
            EXPECT_EQ(12, kv.key_);
            EXPECT_EQ(62, kv.value_);
        }

        // test (24, 35)
        for (int i = 24; i < 28; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, kv, kv2));
            EXPECT_EQ(24, kv.key_);
            EXPECT_EQ(35, kv.value_);
        }

        // test (28, 18)
        for (int i = 28; i < 67; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, kv, kv2));
            EXPECT_EQ(28, kv.key_);
            EXPECT_EQ(18, kv.value_);
        }       

        // test (67, 12345678)
        for (int i = 67; i < 98; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, kv, kv2));
            EXPECT_EQ(67, kv.key_);
            EXPECT_EQ(12345678, kv.value_);
        }

        // test (98, 12)
        for (int i = 98; i < 200; i++) {
            EXPECT_EQ(true, bucket.lb_lookup(i, kv, kv2));
            EXPECT_EQ(98, kv.key_);
            EXPECT_EQ(12, kv.value_);
        }

        EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(0, 20), true, 0));
        EXPECT_EQ(true, bucket.lb_lookup(0, kv, kv2));
        EXPECT_EQ(0, kv.key_);
        EXPECT_EQ(20, kv.value_);
    }

    TEST(Bucket, lookup_insert_basic) {
        Bucket<KListVList8, key_t, value_t, 8> bucket;
        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;


        // keys =   {0, 1, 2, 3, 4, 5,  6,  7}
        // values = {1, 3, 5, 7, 9, 11, 13, 15}
        for (int i = 0; i < 8; i++) list.put(i, i, i * 2 + 1);

        // initial size == 0
        EXPECT_EQ(0, bucket.num_keys());
        
        // lookup non-existing key
        EXPECT_FALSE(bucket.lookup(0, value, 0));
        
        // lookup existing/non-existing keys after single insertion
        EXPECT_TRUE(bucket.insert(list.at(0), true, 0));
        EXPECT_TRUE(bucket.lookup(0, value, 0));
        EXPECT_FALSE(bucket.lookup(1, value, 0));

        // check "lookup return values" and "num_keys after insertion"
        for (int i = 1; i < 8; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
            EXPECT_TRUE(bucket.lookup(i, value, 0));
            EXPECT_EQ(i * 2 + 1, value);
            EXPECT_EQ(i+1, bucket.num_keys());
        }

        // insert overflows
        EXPECT_FALSE(bucket.insert(list.at(0), true, 0));
    }

    TEST(Bucket, SIMD_lookup_insert_basic) {
        Bucket<KeyListValueList<int, int, 8>, int, int, 8> bucket;
        KeyListValueList<int, int, 8> list;
        int key;
        int value;

        // keys =   {0, 1, 2, 3, 4, 5,  6,  7}
        // values = {1, 3, 5, 7, 9, 11, 13, 15}
        for (int i = 0; i < 8; i++) list.put(i, i, i * 2 + 1);

        // initial size == 0
        EXPECT_EQ(0, bucket.num_keys());
        
        // lookup non-existing key
        EXPECT_FALSE(bucket.lookup(0, value, 0));
        EXPECT_FALSE(bucket.SIMD_lookup(0, value, 0));
        

        // check "lookup return values" and "num_keys after insertion"
        for (int i = 0; i < 8; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
            EXPECT_TRUE(bucket.lookup(i, value, 0));
            EXPECT_TRUE(bucket.SIMD_lookup(i, value, 0));
            EXPECT_EQ(i * 2 + 1, value);
            EXPECT_EQ(i+1, bucket.num_keys());
        }

        // insert overflows
        EXPECT_FALSE(bucket.insert(list.at(0), true, 0));
    }

    // TEST(Bucket, SIMD_lookup_insert_basic_64bit_key) {
    //     Bucket<KeyListValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;
    //     KeyListValueList<key_t, value_t, 8> list;
    //     key_t key;
    //     value_t value;

    //     // keys =   {0, 1, 2, 3, 4, 5,  6,  7}
    //     // values = {1, 3, 5, 7, 9, 11, 13, 15}
    //     for (int i = 0; i < 8; i++) list.put(i, i, i * 2 + 1);

    //     // initial size == 0
    //     EXPECT_EQ(0, bucket.num_keys());
        
    //     // lookup non-existing key
    //     EXPECT_FALSE(bucket.lookup(0, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(0, value, 0));
        

    //     // check "lookup return values" and "num_keys after insertion"
    //     for (int i = 0; i < 8; i++) {
    //         EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
    //         EXPECT_TRUE(bucket.lookup(i, value, 0));
    //         EXPECT_TRUE(bucket.SIMD_lookup(i, value, 0));
    //         EXPECT_EQ(i * 2 + 1, value);
    //         EXPECT_EQ(i+1, bucket.num_keys());
    //     }

    //     // insert overflows
    //     EXPECT_FALSE(bucket.insert(list.at(0), true, 0));
    // }

    TEST(Bucket, lookup_insert_large_bucket) {
        Bucket<KeyListValueList<key_t, value_t, 256>, key_t, value_t, 256> bucket;
        KeyListValueList<key_t, value_t, 256> list;
        key_t key;
        value_t value;

    
        for (int i = 0; i < 200; i++) list.put(i, i, i * 2 + 1);

        // initial size == 0
        EXPECT_EQ(0, bucket.num_keys());
        
        // lookup non-existing key
        EXPECT_FALSE(bucket.lookup(0, value, 0));
        
        // lookup existing/non-existing keys after single insertion
        EXPECT_TRUE(bucket.insert(list.at(0), true, 0));
        EXPECT_TRUE(bucket.lookup(0, value, 0));
        EXPECT_FALSE(bucket.lookup(1, value, 0));

        // check "lookup return values" and "num_keys" after insertion
        for (int i = 1; i < 65; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
            EXPECT_TRUE(bucket.lookup(i, value, 0));
            EXPECT_EQ(i * 2 + 1, value);
            EXPECT_EQ(i+1, bucket.num_keys());
            
        }

        bucket.invalidate(101);
        EXPECT_FALSE(bucket.lookup(101, value, 0));
    }

    // TEST(Bucket, SIMD_lookup_insert_large_bucket) {
    //     Bucket<KeyListValueList<int, int, 256>, int, int, 256> bucket;
    //     KeyListValueList<int, int, 256> list;
    //     int key;
    //     int value;

    //     for (int i = 0; i < 200; i++) list.put(i, i, i * 2 + 1);

    //     // initial size == 0
    //     EXPECT_EQ(0, bucket.num_keys());
        
    //     // lookup non-existing key
    //     EXPECT_FALSE(bucket.lookup(0, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(0, value, 0));
        
    //     // lookup existing/non-existing keys after single insertion
    //     EXPECT_TRUE(bucket.insert(list.at(0), true, 0));
    //     EXPECT_TRUE(bucket.lookup(0, value, 0));
    //     EXPECT_TRUE(bucket.SIMD_lookup(0, value, 0));
    //     EXPECT_FALSE(bucket.lookup(1, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(1, value, 0));

    //     // check "lookup return values" and "num_keys" after insertion
    //     for (int i = 1; i < 65; i++) {
    //         EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
    //         EXPECT_TRUE(bucket.lookup(i, value, 0));
    //         EXPECT_TRUE(bucket.SIMD_lookup(i, value, 0));
    //         EXPECT_EQ(i * 2 + 1, value);
    //         EXPECT_EQ(i+1, bucket.num_keys());
            
    //     }

    //     bucket.invalidate(101);
    //     EXPECT_FALSE(bucket.lookup(101, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(101, value, 0));
    // }

    // TEST(Bucket, SIMD_lookup_insert_large_bucket_64bit_key) {
    //     Bucket<KeyListValueList<key_t, value_t, 256>, key_t, value_t, 256> bucket;
    //     KeyListValueList<key_t, value_t, 256> list;
    //     key_t key;
    //     value_t value;

    //     for (int i = 0; i < 200; i++) list.put(i, i, i * 2 + 1);

    //     // initial size == 0
    //     EXPECT_EQ(0, bucket.num_keys());
        
    //     // lookup non-existing key
    //     EXPECT_FALSE(bucket.lookup(0, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(0, value, 0));
        
    //     // lookup existing/non-existing keys after single insertion
    //     EXPECT_TRUE(bucket.insert(list.at(0), true, 0));
    //     EXPECT_TRUE(bucket.lookup(0, value, 0));
    //     EXPECT_TRUE(bucket.SIMD_lookup(0, value, 0));
    //     EXPECT_FALSE(bucket.lookup(1, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(1, value, 0));

    //     // check "lookup return values" and "num_keys" after insertion
    //     for (int i = 1; i < 2; i++) {
    //         EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
    //         EXPECT_TRUE(bucket.lookup(i, value, 0));
    //         EXPECT_TRUE(bucket.SIMD_lookup(i, value, 0));
    //         EXPECT_EQ(i * 2 + 1, value);
    //         EXPECT_EQ(i+1, bucket.num_keys());
            
    //     }

    //     bucket.invalidate(101);
    //     EXPECT_FALSE(bucket.lookup(101, value, 0));
    //     EXPECT_FALSE(bucket.SIMD_lookup(101, value, 0));
    // }

    TEST(Bucket, insert_pivot_update) {
        Bucket<KVList8, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // test initial pivot
        EXPECT_EQ(ULLONG_MAX, bucket.get_pivot());
        EXPECT_EQ(0, bucket.num_keys());

        // test pivot update
        EXPECT_TRUE(bucket.insert(KV(82, 0), true, 0));
        EXPECT_EQ(1, bucket.num_keys());
        EXPECT_EQ(82, bucket.get_pivot());

        // insert keys > pivot
        EXPECT_TRUE(bucket.insert(KV(98, 0), true, 0));
        EXPECT_EQ(82, bucket.get_pivot());
        EXPECT_EQ(2, bucket.num_keys());

        EXPECT_TRUE(bucket.insert(KV(1000, 0), true, 0));
        EXPECT_EQ(82, bucket.get_pivot());
        EXPECT_EQ(3, bucket.num_keys());

        // insert keys < pivot
        EXPECT_TRUE(bucket.insert(KV(53, 0), true, 0));
        EXPECT_EQ(4, bucket.num_keys());
        EXPECT_EQ(53, bucket.get_pivot());

        EXPECT_TRUE(bucket.insert(KV(46, 0), true, 0));
        EXPECT_EQ(5, bucket.num_keys());
        EXPECT_EQ(46, bucket.get_pivot());

        // test the 'true' arugment
        EXPECT_TRUE(bucket.insert(KV(40, 0), true, 0));
        EXPECT_EQ(6, bucket.num_keys());
        EXPECT_EQ(40, bucket.get_pivot());    

        // test the 'false' argument
        EXPECT_TRUE(bucket.insert(KV(30, 0), false, 0));
        EXPECT_EQ(7, bucket.num_keys());
        EXPECT_EQ(40, bucket.get_pivot());    

        EXPECT_TRUE(bucket.insert(KV(25, 0), true, 0));
        EXPECT_EQ(8, bucket.num_keys());
        EXPECT_EQ(25, bucket.get_pivot());

        //test overflow  
        EXPECT_FALSE(bucket.insert(KV(31, 0), true, 0));
        EXPECT_FALSE(bucket.insert(KV(32, 0), false, 0));
    }

    TEST(Bucket, find_kth_smallest) {
        Bucket<KeyValueList<key_t, value_t, 64>, key_t, value_t, 64> bucket;

        // insert 50 unsorted keys
        std::srand(std::time(nullptr));
        std::vector<key_t> keys;
        for (int i = 0; i < 50; i++) keys.push_back(i * 4 + 12);
        std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
        for (int i = 0; i < 50; i++) EXPECT_TRUE(bucket.insert(KeyValue<key_t, value_t>(keys[i], keys[i] + 123456), true, 0));

        for (int i = 0; i < 50; i++) {
            EXPECT_EQ(i * 4 + 12, bucket.find_kth_smallest(i+1).key_);
            EXPECT_EQ(i * 4 + 12 + 123456, bucket.find_kth_smallest(i+1).value_);
        }
    }

    TEST(Bucket, unsorted_iterator) {
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

        // test empty bucket
        EXPECT_TRUE(bucket.begin_unsort() == bucket.end_unsort());

        for (int i = 0; i < 5; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
        }


        // test begin(), end(), opreator* and it++
        int i = 0;
        for (auto it = bucket.begin_unsort(); it != bucket.end_unsort(); it++) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(list.at(i).key_, kv.key_);
            EXPECT_EQ(list.at(i).value_, kv.value_);
            i++;
        }
        EXPECT_EQ(5, i); 

        // test ++it basic usage
        i = 0;
        for (auto it = bucket.begin_unsort(); it != bucket.end_unsort(); ++it) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(list.at(i).key_, kv.key_);
            EXPECT_EQ(list.at(i).value_, kv.value_);
            i++;
        }
        EXPECT_EQ(5, i); 


        // test ++it return value
        i = 1;
        for (auto it = bucket.begin_unsort(); it != bucket.end_unsort() && i < 4;) {
            KeyValue<key_t, value_t> kv = *(++it);
            EXPECT_EQ(list.at(i).key_, kv.key_);
            EXPECT_EQ(list.at(i).value_, kv.value_);
            i++;
        }
        EXPECT_EQ(4, i); 

        // test overflow
        auto it = bucket.end_unsort();
        it++;
        EXPECT_TRUE(it == bucket.end_unsort());
        ++it;
        EXPECT_TRUE(it == bucket.end_unsort());
    }

    TEST(Bucket, sorted_iterator) {
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

        // test empty bucket
        EXPECT_TRUE(bucket.begin() == bucket.end());

        for (int i = 0; i < 5; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
        }


        // insert {12, 24, 28, 67, 98} sorted
        KeyListValueList<key_t, value_t, 8> list_sorted;
        list_sorted.put(0, 12, 62);
        list_sorted.put(1, 24, 35);
        list_sorted.put(2, 28, 18);
        list_sorted.put(3, 67, 12345678);
        list_sorted.put(4, 98, 12);  

        // test begin(), end(), opreator* and it++
        int i = 0;
        for (auto it = bucket.begin(); it != bucket.end(); it++) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(list_sorted.at(i).key_, kv.key_);
            // EXPECT_EQ(list_sorted.at(i).value_, kv.value_);
            i++;
        }
        EXPECT_EQ(5, i); 

        // test ++it basic usage
        i = 0;
        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(list_sorted.at(i).key_, kv.key_);
            EXPECT_EQ(list_sorted.at(i).value_, kv.value_);
            i++;
        }
        EXPECT_EQ(5, i); 


        // test ++it return value
        i = 1;
        for (auto it = bucket.begin(); it != bucket.end() && i < 4;) {
            KeyValue<key_t, value_t> kv = *(++it);
            EXPECT_EQ(list_sorted.at(i).key_, kv.key_);
            EXPECT_EQ(list_sorted.at(i).value_, kv.value_);
            i++;
        }
        EXPECT_EQ(4, i); 

        // test overflow
        auto it = bucket.end();
        it++;
        EXPECT_TRUE(it == bucket.end());
        ++it;
        EXPECT_TRUE(it == bucket.end());
    }

    TEST(Bucket, get_valid_kvs) {
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
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
        }

        bucket.invalidate(2);
        bucket.invalidate(4);

        std::vector<KeyValue<key_t, value_t>> valid_kv;
        bucket.get_valid_kvs(valid_kv);
        
        EXPECT_EQ(3, valid_kv.size());
        EXPECT_EQ(98, valid_kv[0].key_);
        EXPECT_EQ(12, valid_kv[0].value_);
        EXPECT_EQ(24, valid_kv[1].key_);
        EXPECT_EQ(35, valid_kv[1].value_);
        EXPECT_EQ(28, valid_kv[2].key_);
        EXPECT_EQ(18, valid_kv[2].value_);
    }

    TEST(Bucket, split_and_insert_middle_key) {
        using KeyValuePtrType = KeyValue<key_t, uintptr_t>;
        using BucketType = Bucket<KeyListValueList<key_t, value_t, 8>, key_t, value_t, 8>;


        Bucket<KeyListValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // insert {12, 24, 28, 67, 98, 100} unsorted
        list.put(0, 98, 12);
        list.put(1, 24, 35);
        list.put(2, 12, 62);
        list.put(3, 28, 18);
        list.put(4, 67, 12345678);
        list.put(5, 100, 5552);

        for (int i = 0; i < 6; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
        }

        // insert 88, which in the middle of the bucket key range
        std::pair<KeyValuePtrType, KeyValuePtrType> new_buckets = bucket.split_and_insert(KeyValue<key_t, value_t>(88, 1234));

        BucketType *bucket1;
        BucketType *bucket2;
        bucket1 = (BucketType *)(void *)(new_buckets.first.value_);
        bucket2 = (BucketType *)(void *)(new_buckets.second.value_);

        EXPECT_EQ(6, bucket.num_keys()); // the old bucket does not change
        EXPECT_EQ(3, bucket1->num_keys()); // keys = 12, 24, 28
        EXPECT_EQ(4, bucket2->num_keys()); // keys = 67, 88, 98, 100

        // look up all key values in bucket
        for (int i = 0; i < 6; i++) {
            EXPECT_TRUE(bucket.lookup(list.at(i).key_, value, 0));
            EXPECT_EQ(list.at(i).value_, value);
        }

        // look up keys in bucket1
        EXPECT_TRUE(bucket1->lookup(12, value, 0));
        EXPECT_EQ(62, value);
        EXPECT_TRUE(bucket1->lookup(24, value, 0));
        EXPECT_EQ(35, value);
        EXPECT_TRUE(bucket1->lookup(28, value, 0));
        EXPECT_EQ(18, value);

        // look up keys not in bucket1
        EXPECT_FALSE(bucket1->lookup(67, value, 0));
        EXPECT_FALSE(bucket1->lookup(88, value, 0));
        EXPECT_FALSE(bucket1->lookup(98, value, 0));
        EXPECT_FALSE(bucket1->lookup(100, value, 0));

        // look up keys in bucket2
        EXPECT_TRUE(bucket2->lookup(67, value, 0));
        EXPECT_EQ(12345678, value);
        EXPECT_TRUE(bucket2->lookup(88, value, 0));
        EXPECT_EQ(1234, value);
        EXPECT_TRUE(bucket2->lookup(98, value, 0));
        EXPECT_EQ(12, value);
        EXPECT_TRUE(bucket2->lookup(100, value, 0));
        EXPECT_EQ(5552, value);

        // look up keys not in bucket2
        EXPECT_FALSE(bucket2->lookup(12, value, 0));
        EXPECT_FALSE(bucket2->lookup(24, value, 0));
        EXPECT_FALSE(bucket2->lookup(28, value, 0));
    }


    TEST(Bucket, split_and_insert_smaller_key) {
        using KeyValuePtrType = KeyValue<key_t, uintptr_t>;
        using BucketType = Bucket<KeyListValueList<key_t, value_t, 8>, key_t, value_t, 8>;

        Bucket<KeyListValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        // insert {12, 24, 28, 67, 98, 100} unsorted
        list.put(0, 98, 12);
        list.put(1, 24, 35);
        list.put(2, 12, 62);
        list.put(3, 28, 18);
        list.put(4, 67, 12345678);
        list.put(5, 100, 5552);

        for (int i = 0; i < 6; i++) {
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
        }

        // insert 20, which is smaller than keys in the bucket
        auto new_buckets = bucket.split_and_insert(KeyValue<key_t, value_t>(20, 1234));

        BucketType *bucket1;
        BucketType *bucket2;
        bucket1 = (BucketType *)(void *)(new_buckets.first.value_);
        bucket2 = (BucketType *)(void *)(new_buckets.second.value_);

        EXPECT_EQ(6, bucket.num_keys()); // the old bucket does not change
        EXPECT_EQ(4, bucket1->num_keys()); // keys = 12, 20, 24, 28
        EXPECT_EQ(3, bucket2->num_keys()); // keys = 67, 98, 100

        // look up all key values in bucket
        for (int i = 0; i < 6; i++) {
            EXPECT_TRUE(bucket.lookup(list.at(i).key_, value, 0));
            EXPECT_EQ(list.at(i).value_, value);
        }

        // look up keys in bucket1
        EXPECT_TRUE(bucket1->lookup(12, value, 0));
        EXPECT_EQ(62, value);
        EXPECT_TRUE(bucket1->lookup(20, value, 0));
        EXPECT_EQ(1234, value);
        EXPECT_TRUE(bucket1->lookup(24, value, 0));
        EXPECT_EQ(35, value);
        EXPECT_TRUE(bucket1->lookup(28, value, 0));
        EXPECT_EQ(18, value);

        // look up keys not in bucket1
        EXPECT_FALSE(bucket1->lookup(67, value, 0));
        EXPECT_FALSE(bucket1->lookup(98, value, 0));
        EXPECT_FALSE(bucket1->lookup(100, value, 0));

        // look up keys in bucket2
        EXPECT_TRUE(bucket2->lookup(67, value, 0));
        EXPECT_EQ(12345678, value);
        EXPECT_TRUE(bucket2->lookup(98, value, 0));
        EXPECT_EQ(12, value);
        EXPECT_TRUE(bucket2->lookup(100, value, 0));
        EXPECT_EQ(5552, value);

        // look up keys not in bucket2
        EXPECT_FALSE(bucket2->lookup(12, value, 0));
        EXPECT_FALSE(bucket2->lookup(20, value, 0));
        EXPECT_FALSE(bucket2->lookup(24, value, 0));
        EXPECT_FALSE(bucket2->lookup(28, value, 0));
    }


    TEST(Bucket, update) {
        Bucket<KeyListValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

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
            EXPECT_TRUE(bucket.insert(list.at(i), true, 0));
        }

        // update key 12
        KeyValue<key_t, value_t> kv(12, 112);
        EXPECT_TRUE(bucket.update(kv));
        EXPECT_EQ(5, bucket.num_keys());
        EXPECT_TRUE(bucket.lookup(12, value, 0));
        EXPECT_EQ(112, value);

        // update key 24
        kv = KeyValue<key_t, value_t>(24, 124);
        EXPECT_TRUE(bucket.update(kv));
        EXPECT_EQ(5, bucket.num_keys());
        EXPECT_TRUE(bucket.lookup(24, value, 0));
        EXPECT_EQ(124, value);

        // update key 28
        kv = KeyValue<key_t, value_t>(28, 128);
        EXPECT_TRUE(bucket.update(kv));
        EXPECT_EQ(5, bucket.num_keys());
        EXPECT_TRUE(bucket.lookup(28, value, 0));

        // update non-existing key
        kv = KeyValue<key_t, value_t>(128, 233);
        EXPECT_FALSE(bucket.update(kv));
        EXPECT_EQ(5, bucket.num_keys());
        EXPECT_FALSE(bucket.lookup(128, value, 0));
    }

    TEST(Bucket, lower_bound){
        // write unit test for segment::lower_bound and segment::upper_bound
        // construct a segment
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;
        // insert keys to the bucket
        for (size_t i = 0; i < length; i++) {
            EXPECT_TRUE(bucket.insert(in_array[i], true, 0));
        }


        // test lower_bound
        auto it = bucket.lower_bound(0);
        EXPECT_EQ(0, (*it).key_);
        it = bucket.lower_bound(1);
        EXPECT_EQ(20, (*it).key_);
        it = bucket.lower_bound(20);
        EXPECT_EQ(20, (*it).key_);
        it = bucket.lower_bound(21);
        EXPECT_EQ(40, (*it).key_);
        it = bucket.lower_bound(140);
        EXPECT_EQ(140, (*it).key_);
        it = bucket.lower_bound(141);
        EXPECT_TRUE(it == bucket.end());

        
    }

    TEST(Bucket, insert_with_hint) {
        key_t keys[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 
                        12, 13, 14, 15, 16, 17, 18, 19, 20};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        
        Bucket<KeyValueList<key_t, value_t, 256>, key_t, value_t, 256> bucket;


        for (int i = 0; i < 4; i++) { // insert 4 keys, all hint = 0
            std::cout << "i = " << i << std::endl;
            EXPECT_TRUE(bucket.insert(in_array[i], true, 0));
            EXPECT_EQ(i, bucket.get_pos(in_array[i].key_));
        }

        for (int i = 4; i < 8; i++) {
            EXPECT_TRUE(bucket.insert(in_array[i], true, i * 2 + 3)); // hint  = 11, 13, 15, 17
            EXPECT_EQ(i * 2 + 3, bucket.get_pos(in_array[i].key_));
        }

        for (int i = 8; i < 12; i++) {
            EXPECT_TRUE(bucket.insert(in_array[i], true, 132)); // hint  = 132, 132, 132, 132
            EXPECT_EQ(132 + i - 8, bucket.get_pos(in_array[i].key_));
        }

        int positions[] = {255, 4, 5, 6}; // [0, 1, 2, 3] are occupied by keys 0, 1, 2, 3
        for (int i = 12; i < 16; i++) { // test wrap around
            EXPECT_TRUE(bucket.insert(in_array[i], true, 255)); // hint  = 255, 255, 255, 255
            EXPECT_EQ(positions[i-12], bucket.get_pos(in_array[i].key_));
        }
    }

    TEST(Bucket, mem_size){
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;
        size_t meta_size = sizeof(key_t) + sizeof(int) + sizeof(uint64_t) + sizeof(size_t);
        // pivot_, num_keys_, bitmap_ and BITMAP_SIZE are all in the meta data
        // assume BITMAP_SIZE = 1, when bucket_size <=64;

        size_t kv_size = sizeof(key_t) + sizeof(value_t);
        EXPECT_GE(bucket.mem_size(), meta_size + 8 * kv_size);
        EXPECT_LT(bucket.mem_size(), meta_size + 8 * kv_size + 10);
        // expect the mem_size should be a little bit larger than the expected value

        Bucket<KeyValueList<key_t, value_t, 16>, key_t, value_t, 16> bucket1;
        EXPECT_GE(bucket1.mem_size(), meta_size + 16 * kv_size);
        EXPECT_LT(bucket1.mem_size(), meta_size + 16 * kv_size + 10);
        
        Bucket<KeyValueList<key_t, value_t, 32>, key_t, value_t, 32> bucket2;
        EXPECT_GE(bucket2.mem_size(), meta_size + 32 * kv_size);
        EXPECT_LT(bucket2.mem_size(), meta_size + 32 * kv_size + 10);

        meta_size = sizeof(key_t) + sizeof(int) + 2*sizeof(uint64_t) + sizeof(size_t);
        // pivot_, num_keys_, bitmap_ and BITMAP_SIZE are all in the meta data
        // assume BITMAP_SIZE = 2, when 64<bucket_size <=128;

        Bucket<KeyValueList<key_t, value_t, 128>, key_t, value_t, 128> bucket4;
        EXPECT_GE(bucket4.mem_size(), meta_size + 128 * kv_size);
        EXPECT_LT(bucket4.mem_size(), meta_size + 128 * kv_size + 10);
    }

}