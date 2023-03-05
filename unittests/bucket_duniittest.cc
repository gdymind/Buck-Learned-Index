#include "gtest/gtest.h"

#include "bucket.h"

#include<vector>


namespace buckindex {
    typedef unsigned long long key_t;
    typedef unsigned long long value_t;

    TEST(Bucket, insert_and_lookup) {
        Bucket<KeyValueList<key_t, value_t, 8>, key_t, value_t, 8> bucket;

        KeyListValueList<key_t, value_t, 8> list;
        key_t key;
        value_t value;

        for (int i = 0; i < 8; i++) list.put(i, i, i);

        EXPECT_EQ(0, bucket.num_keys());

        
        EXPECT_EQ(false, bucket.lookup(0, value));
        
        bucket.insert(KeyValue<key_t, value_t>(1, 1));
        EXPECT_EQ(1, bucket.num_keys());
        // EXPECT_EQ(false, bucket.lookup(0, value));
        // EXPECT_EQ(true, bucket.lookup(1, value));

    }

    TEST(Bucket, find_kth_smallest) {
        Bucket<KeyValueList<key_t, value_t, 64>, key_t, value_t, 64> bucket;

        // bucket.insert(KeyValue<key_t, value_t>(8, 0));

        // EXPECT_EQ(0, (int)b.num_keys());
    }

}
