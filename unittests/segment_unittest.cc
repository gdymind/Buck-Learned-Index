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

    TEST(Segment, insert_normal_case) {
        key_t keys[] = {0,2,4,6,8,10,12,14,16,18,20};
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
        KeyValue<key_t, value_t> key1(3,4);
        success = seg.insert(key1);
        EXPECT_EQ(true, success);
        success = seg.lookup(3,value);
        EXPECT_EQ(true, success);
        EXPECT_EQ(4, value);

        KeyValue<key_t, value_t> key2(11,8);
        success = seg.insert(key2);
        EXPECT_EQ(true, success);

        size_t count = 0;
        for(size_t i=0;i<seg.num_bucket_;i++){
            count += seg.sbucket_list_[i].num_keys();
        }
        EXPECT_EQ(length+2, count);
        EXPECT_EQ(5, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(5, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[2].num_keys());
    }

    TEST(Segment, insert_rebalance){
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.5;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(2, seg.sbucket_list_[2].num_keys());
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys());

        bool success = false;
        KeyValue<key_t, value_t> key1(50,4);
        success = seg.insert(key1);
        KeyValue<key_t, value_t> key2(55,6);
        success = seg.insert(key2);

        // Now there are 4 elements in the second segment (40,50,55,60)
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(40,seg.sbucket_list_[1].get_pivot());
        
        // bucket rebalance triggered by one more insertion
        // Before migration: (40,50,55,60) and (80,100)
        // After migration: (40,50,55) and (60,80,100)
        KeyValue<key_t, value_t> key3(56,8);
        success = seg.insert(key3);
        EXPECT_EQ(true, success);
        // After insertion: (40,50,55,56) and (60,80,100)

        // migrate forward, and update pivot
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[2].num_keys());
        EXPECT_EQ(60,seg.sbucket_list_[2].get_pivot());

        KeyValue<key_t, value_t> key4(45,8);
        success = seg.insert(key4);
        EXPECT_EQ(true, success);
        // Before migration: (0,20) and (40,50,55,56) and (60,80,100)
        // After migration: (0,20,40) and (50,55,56)
        // After insertion: (0,20,40,45) and (50,55,56)

        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(50,seg.sbucket_list_[1].get_pivot());
    }

    TEST(Segment, insert_fail){
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.5;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(4, seg.num_bucket_);

        bool success = false;
        KeyValue<key_t, value_t> key1(50,4);
        success = seg.insert(key1);
        KeyValue<key_t, value_t> key2(55,6);
        success = seg.insert(key2);

        KeyValue<key_t, value_t> key3(85,4);
        success = seg.insert(key3);

        KeyValue<key_t, value_t> key4(5,4);
        success = seg.insert(key4);
        KeyValue<key_t, value_t> key5(15,6);
        success = seg.insert(key5);

        // Now the first three segments are (0,5,15,20)(40,50,55,60)(80,85,100)
        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[2].num_keys());

        KeyValue<key_t, value_t> key6(65,6);
        success = seg.insert(key6);
        // Before migration: (40,50,55,60) and (80,85,100)
        // After migration: (40,50,55) and (60,80,85,100)
        // Now the new key should insert in the third segment
        EXPECT_FALSE(success);

        KeyValue<key_t, value_t> key7(52,6);
        success = seg.insert(key7);
        
        // Now the first three segments are (0,5,15,20)(40,50,52,55)(60,80,85,100)
        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[2].num_keys());
        // No key can be inserted in the second segment
        KeyValue<key_t, value_t> key8(46,6);
        success = seg.insert(key8);
        EXPECT_FALSE(success);
    }

    TEST(Segment, size) {
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.5;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        EXPECT_EQ(8, seg.size());
        
        bool success = false;
        KeyValue<key_t, value_t> key1(50,4);
        success = seg.insert(key1);
        EXPECT_TRUE(success);
        EXPECT_EQ(9, seg.size());

        success = false;
        KeyValue<key_t, value_t> key2(70,4);
        success = seg.insert(key2);
        EXPECT_TRUE(success);
        EXPECT_EQ(10, seg.size());

        success = false;
        KeyValue<key_t, value_t> key3(75,4);
        success = seg.insert(key3);
        EXPECT_TRUE(success);
        EXPECT_EQ(11, seg.size());
    }

    TEST(Segment, scale){
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(2, seg.num_bucket_);

        bool success = true;
        KeyValue<key_t, value_t> key1(50,4);
        success = seg.insert(key1);
        // expect insert fails because of no empty slot
        EXPECT_FALSE(success);


        // call scale_and_segmentation 
        std::vector<KeyValue<key_t,uintptr_t>> new_segs;
        new_segs.clear();
        double new_fill_ratio = 0.5;
        success = false;
        success = seg.scale_and_segmentation(new_fill_ratio, new_segs);
        EXPECT_TRUE(success);
        
        EXPECT_EQ(1, new_segs.size());

        Segment<key_t, value_t, 4> *seg1 = reinterpret_cast<Segment<key_t, value_t, 4> *>(new_segs[0].value_);
        
        // pivot key
        EXPECT_EQ(0, new_segs[0].key_);

        // num_bucket_ is scaled up
        // and each bucket's fill ratio is 0.5
        EXPECT_EQ(4, seg1->num_bucket_);
        EXPECT_EQ(2, seg1->sbucket_list_[1].num_keys());
        success = false;
        success = seg1->insert(key1);
        EXPECT_TRUE(success);
        EXPECT_EQ(3, seg1->sbucket_list_[1].num_keys());

        delete seg1;
    }

    TEST(Segment, scale_and_segmentation){
        key_t keys[] = {0,1,2,2,2,2,6,7,8,9,10};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, value_t, 1> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(11, seg.num_bucket_);

        // insert key
        bool success = true;
        KeyValue<key_t, value_t> key1(5,4);
        success = seg.insert(key1);
        // expect insert fails because of no empty slot
        EXPECT_FALSE(success);

        // call scale_and_segmentation
        std::vector<KeyValue<key_t,uintptr_t>> new_segs;
        new_segs.clear();
        double new_fill_ratio = 1;
        success = false;
        success = seg.scale_and_segmentation(new_fill_ratio, new_segs);
        EXPECT_TRUE(success);

        /*Expected cuts: 0,1,2|2,2|2,6,7|8,9,10*/
        EXPECT_EQ(4, new_segs.size());
        EXPECT_EQ(0, new_segs[0].key_);
        EXPECT_EQ(2, new_segs[1].key_);
        EXPECT_EQ(2, new_segs[2].key_);
        EXPECT_EQ(8, new_segs[3].key_);

        Segment<key_t, value_t, 1> *seg1 = reinterpret_cast<Segment<key_t, value_t, 1> *>(new_segs[0].value_);
        Segment<key_t, value_t, 1> *seg2 = reinterpret_cast<Segment<key_t, value_t, 1> *>(new_segs[1].value_);
        Segment<key_t, value_t, 1> *seg3 = reinterpret_cast<Segment<key_t, value_t, 1> *>(new_segs[2].value_);
        Segment<key_t, value_t, 1> *seg4 = reinterpret_cast<Segment<key_t, value_t, 1> *>(new_segs[3].value_);

        EXPECT_EQ(3, seg1->num_bucket_);
        EXPECT_EQ(2, seg2->num_bucket_);
        EXPECT_EQ(3, seg3->num_bucket_);
        EXPECT_EQ(3, seg4->num_bucket_);
        
        delete seg1;
        delete seg2;
        delete seg3;
        delete seg4;
    }

    TEST(Segment, const_iterator){
        // write unit test for segment::const_iterator including testing begin() and end() and ++ operator and * operator and == operator and != operator
        // construct a segment
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, value_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, value_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, value_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // test begin(), end(), opreator* and it++
        int i = 0;
        for (auto it = seg.cbegin(); it != seg.cend(); it++) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(keys[i], kv.key_);
            i++;
        }
        EXPECT_EQ(8, i);

        // test ++it basic usage
        i = 0;
        for (auto it = seg.cbegin(); it != seg.cend(); ++it) {
            KeyValue<key_t, value_t> kv = *it;
            EXPECT_EQ(keys[i], kv.key_);
            i++;
        }

        // test ++it return value
        i = 1;
        for (auto it = seg.cbegin(); it != seg.cend() && i < 7;) {
            KeyValue<key_t, value_t> kv = *(++it);
            EXPECT_EQ(keys[i], kv.key_);
            i++;
        }
        EXPECT_EQ(7, i);

        // test overflow and == operator
        auto it = seg.cend();
        it++;
        EXPECT_TRUE(it == seg.cend());
        ++it;
        EXPECT_TRUE(it == seg.cend());

    }

}
