#include "gtest/gtest.h"

#include "segment.h"

#include<vector>
#include<iostream>
#include <cstdlib>
#include <ctime>
#include <random>
#include <algorithm>


namespace buckindex {
    typedef unsigned long long key_t;
    typedef unsigned long long value_t;

    TEST(Segment, default_constructor) {
        Segment<key_t, value_t, 8> seg;
        EXPECT_EQ(0, seg.num_bucket_);
        EXPECT_EQ(nullptr, seg.sbucket_list_);
    }

    TEST(Segment, constructor_model_based_insert) {
        key_t keys[] = {0,1,2,3,4,5,6,7,8,9,10};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=x
        LinearModel<key_t> model(1,0);
        double fill_ratio = 0.5;
        Segment<key_t, value_t, 8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // // put 11 elements into 3*8 = 24 empty slots 
        // // expanded model is y = 2x
        EXPECT_EQ(3, seg.num_bucket_);
        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[2].num_keys());
    }

    TEST(Segment, constructor_bucket_overflow) {
        key_t keys[] = {0,1,2,3,4,5,6,7,8,9,10,12,14,16,18,20,22,24,26,28};
        // with increasing num of keys following the step of 2, 
        // the model would be y = 1/2 * x + 5

        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }

        LinearModel<key_t> model(0.5,5);
        double fill_ratio = 0.8;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        

        // put 20 elements into 7*4 = 28 empty slots 
        // expanded model is y = 0.625x + 6.25;
        EXPECT_EQ(7, seg.num_bucket_);
        EXPECT_EQ(0, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[1].num_keys());

        // there suppose to be 7 keys mapped to the third bucket
        // but now it overflowed to the next one
        EXPECT_EQ(4, seg.sbucket_list_[2].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[3].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[4].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[5].num_keys());
        EXPECT_EQ(1, seg.sbucket_list_[6].num_keys());
    }

    TEST(Segment, constructor_remaining_slot) {
        key_t keys[] = {0,1,2,3,4,5,6,7,8,9,10,12,14,16,18,20,22,24,26,28};
        // with increasing num of keys following the step of 2, 
        // the model would be y = 1/2 * x + 5

        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }

        LinearModel<key_t> model(0.5,5);
        double fill_ratio = 1;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        

        // put 20 elements into 5*4 = 20 empty slots 
        // expanded model is y = 1/2 * x + 5;
        EXPECT_EQ(5, seg.num_bucket_);

        // there suppose to be 0 keys mapped to the first bucket
        // but as the fill_ratio=1,
        // keys are still inserted
        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[2].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[3].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[4].num_keys());
    }

    TEST(Segment, lookup) {
        key_t keys[] = {1,2,4,6,8,10,12,14,16,18,20};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=0.5x
        LinearModel<key_t> model(0.5,0);
        double fill_ratio = 0.5;
        Segment<key_t, value_t, 8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
    
        value_t value = 0;
        bool success = false;
        success = seg.lookup(1,value);
        EXPECT_EQ(true, success);
        EXPECT_EQ(1, value);

        success = seg.lookup(0,value);
        EXPECT_EQ(false, success);

        success = seg.lookup(5,value); // find the lower bound
        EXPECT_EQ(true, success);
        EXPECT_EQ(4, value);
    }
}
