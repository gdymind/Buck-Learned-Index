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
        Segment<key_t, 8> seg;
        EXPECT_EQ(0, seg.num_bucket_);
        EXPECT_EQ(nullptr, seg.sbucket_list_);
    }

    TEST(Segment, constructor_model_based_insert) {
        key_t keys[] = {0,1,2,3,4,5,6,7,8,9,10};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=x
        LinearModel<key_t> model(1,0);
        double fill_ratio = 0.5;
        Segment<key_t, 8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
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

        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }

        LinearModel<key_t> model(0.5,5);
        double fill_ratio = 0.8;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        

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

        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }

        LinearModel<key_t> model(0.5,5);
        double fill_ratio = 1;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        

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
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.5x
        LinearModel<key_t> model(0.5,0);
        double fill_ratio = 0.5;
        Segment<key_t, 8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
    
        KeyValue<key_t, uintptr_t> kv;
        uintptr_t value = 0;
        bool success = false;
        success = seg.lookup(1,kv);
        EXPECT_EQ(true, success);
        EXPECT_EQ(1, kv.key_);
        EXPECT_EQ(1, kv.value_);

        success = seg.lookup(0,kv);
        EXPECT_EQ(true, success); // seg.lookup always return true

        success = seg.lookup(5,kv); // find the lower bound
        EXPECT_EQ(true, success);
        EXPECT_EQ(4, kv.key_);
        EXPECT_EQ(4, kv.value_);
    }

    TEST(Segment, insert_normal_case) {
        key_t keys[] = {0,2,4,6,8,10,12,14,16,18,20};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.5x
        LinearModel<key_t> model(0.5,0);
        double fill_ratio = 0.5;
        Segment<key_t, 8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
    
        KeyValue<key_t, uintptr_t> kv;
        uintptr_t value = 0;
        bool success = false;
        KeyValue<key_t, uintptr_t> key1(3,4);
        success = seg.insert(key1);
        EXPECT_EQ(true, success);
        success = seg.lookup(3,kv);
        EXPECT_EQ(true, success);
        EXPECT_EQ(4, kv.value_);

        KeyValue<key_t, uintptr_t> key2(11,8);
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
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.5;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(2, seg.sbucket_list_[2].num_keys());
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys());

        bool success = false;
        KeyValue<key_t, uintptr_t> key1(50,4);
        success = seg.insert(key1);
        KeyValue<key_t, uintptr_t> key2(55,6);
        success = seg.insert(key2);

        // Now there are 4 elements in the second segment (40,50,55,60)
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(40,seg.sbucket_list_[1].get_pivot());
        
        // bucket rebalance triggered by one more insertion
        // Before migration: (40,50,55,60) and (80,100)
        // After migration: (40,50,55) and (60,80,100)
        KeyValue<key_t, uintptr_t> key3(56,8);
        success = seg.insert(key3);
        EXPECT_EQ(true, success);
        // After insertion: (40,50,55,56) and (60,80,100)

        // migrate forward, and update pivot
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[2].num_keys());
        EXPECT_EQ(60,seg.sbucket_list_[2].get_pivot());

        KeyValue<key_t, uintptr_t> key4(45,8);
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
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.5;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(4, seg.num_bucket_);

        bool success = false;
        KeyValue<key_t, uintptr_t> key1(50,4);
        success = seg.insert(key1);
        KeyValue<key_t, uintptr_t> key2(55,6);
        success = seg.insert(key2);

        KeyValue<key_t, uintptr_t> key3(85,4);
        success = seg.insert(key3);

        KeyValue<key_t, uintptr_t> key4(5,4);
        success = seg.insert(key4);
        KeyValue<key_t, uintptr_t> key5(15,6);
        success = seg.insert(key5);

        // Now the first three segments are (0,5,15,20)(40,50,55,60)(80,85,100)
        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(3, seg.sbucket_list_[2].num_keys());

        KeyValue<key_t, uintptr_t> key6(65,6);
        success = seg.insert(key6);
        // Before migration: (40,50,55,60) and (80,85,100)
        // After migration: (40,50,55) and (60,80,85,100)
        // Now the new key should insert in the third segment
        EXPECT_FALSE(success);

        KeyValue<key_t, uintptr_t> key7(52,6);
        success = seg.insert(key7);
        
        // Now the first three segments are (0,5,15,20)(40,50,52,55)(60,80,85,100)
        EXPECT_EQ(4, seg.sbucket_list_[0].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[1].num_keys());
        EXPECT_EQ(4, seg.sbucket_list_[2].num_keys());
        // No key can be inserted in the second segment
        KeyValue<key_t, uintptr_t> key8(46,6);
        success = seg.insert(key8);
        EXPECT_FALSE(success);
    }

    TEST(Segment, size) {
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.5;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        EXPECT_EQ(8, seg.size());
        
        bool success = false;
        KeyValue<key_t, uintptr_t> key1(50,4);
        success = seg.insert(key1);
        EXPECT_TRUE(success);
        EXPECT_EQ(9, seg.size());

        success = false;
        KeyValue<key_t, uintptr_t> key2(70,4);
        success = seg.insert(key2);
        EXPECT_TRUE(success);
        EXPECT_EQ(10, seg.size());

        success = false;
        KeyValue<key_t, uintptr_t> key3(75,4);
        success = seg.insert(key3);
        EXPECT_TRUE(success);
        EXPECT_EQ(11, seg.size());
    }
/*
    TEST(Segment, scale){
        key_t keys[] = {1,21,41,61,81,101,121,141};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,-0.05);
        double fill_ratio = 1;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(2, seg.num_bucket_);

        bool success = true;
        KeyValue<key_t, uintptr_t> key1(50,4);
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

        Segment<key_t, 4> *seg1 = reinterpret_cast<Segment<key_t, 4> *>(new_segs[0].value_);
        
        // pivot key
        EXPECT_EQ(1, new_segs[0].key_);

        // num_bucket_ is scaled up
        // and each bucket's fill ratio is 0.5
        EXPECT_EQ(4, seg1->num_bucket_);
        EXPECT_EQ(2, seg1->sbucket_list_[1].num_keys());
        success = false;
        success = seg1->insert(key1);
        EXPECT_TRUE(success);

        //check all keys are in the new segment
        uintptr_t value = 0;
        for (size_t i = 0; i < length; i++) {
            success = false;
            success = seg1->lookup(keys[i],value);
            EXPECT_TRUE(success);
            EXPECT_EQ(keys[i], value);
        }
        success = false;
        success = seg1->lookup(key1.key_,value);
        EXPECT_TRUE(success);
        EXPECT_EQ(key1.value_, value);

        delete seg1;
    }

    TEST(Segment, scale_and_segmentation){
        key_t keys[] = {0,1,2,2,2,2,6,7,8,9,10};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, 2> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        EXPECT_EQ(6, seg.num_bucket_);

        // insert key
        bool success = true;
        KeyValue<key_t, uintptr_t> key1(5,4);
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

        //Expected cuts: 0,1,2|2,2|2,6,7|8,9,10
        EXPECT_EQ(4, new_segs.size());
        EXPECT_EQ(0, new_segs[0].key_);
        EXPECT_EQ(2, new_segs[1].key_);
        EXPECT_EQ(2, new_segs[2].key_);
        EXPECT_EQ(8, new_segs[3].key_);

        Segment<key_t, 2> *seg1 = reinterpret_cast<Segment<key_t, 2> *>(new_segs[0].value_);
        Segment<key_t, 2> *seg2 = reinterpret_cast<Segment<key_t, 2> *>(new_segs[1].value_);
        Segment<key_t, 2> *seg3 = reinterpret_cast<Segment<key_t, 2> *>(new_segs[2].value_);
        Segment<key_t, 2> *seg4 = reinterpret_cast<Segment<key_t, 2> *>(new_segs[3].value_);

        EXPECT_EQ(2, seg1->num_bucket_);
        EXPECT_EQ(1, seg2->num_bucket_);
        EXPECT_EQ(2, seg3->num_bucket_);
        EXPECT_EQ(2, seg4->num_bucket_);

        //check all other keys are in the new segments
        uintptr_t value = 0;
        size_t count = 0;
        vector<Segment<key_t, 2>*> segs={seg1,seg2,seg3,seg4};
        for(size_t i=0;i<segs.size();i++){
            for (size_t j=0;j<segs[i]->size();j++,count++){
                success = false;
                success = segs[i]->lookup(keys[count],value);
                EXPECT_TRUE(success);
                EXPECT_EQ(keys[count], value);
            }
        }

        delete seg1;
        delete seg2;
        delete seg3;
        delete seg4;
    }
*/
    TEST(Segment, const_iterator){
        // write unit test for segment::const_iterator including testing begin() and end() and ++ operator and * operator and == operator and != operator
        // construct a segment
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // test begin(), end(), opreator* and it++
        int i = 0;
        for (auto it = seg.cbegin(); it != seg.cend(); it++) {
            KeyValue<key_t, uintptr_t> kv = *it;
            EXPECT_EQ(keys[i], kv.key_);
            i++;
        }
        EXPECT_EQ(8, i);

        // test ++it basic usage
        i = 0;
        for (auto it = seg.cbegin(); it != seg.cend(); ++it) {
            KeyValue<key_t, uintptr_t> kv = *it;
            EXPECT_EQ(keys[i], kv.key_);
            i++;
        }

        // test ++it return value
        i = 1;
        for (auto it = seg.cbegin(); it != seg.cend() && i < 7;) {
            KeyValue<key_t, uintptr_t> kv = *(++it);
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

    TEST(Segment, batch_update_same_bucket) {
        using SegmentType = Segment<key_t, 8>;

        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        SegmentType *segs[length];
        for (size_t i = 0; i < length; i++) {
            std::vector<KeyValue<key_t, uintptr_t>> pivot_list(1, KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
            segs[i] = new SegmentType(1, 1, LinearModel<key_t>(0, 0), pivot_list.begin(), pivot_list.end());
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], reinterpret_cast<uintptr_t>(segs[i])));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.25;
        Segment<key_t,  8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // 4 buckets, each bucket has 2 keys and 2 empty slots
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(2, seg.sbucket_list_[2].num_keys()); // 80, 100
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        KeyValue<key_t, uintptr_t> kv(100, 100);
        // look up all inserted keys
        uintptr_t value;
        for (size_t i = 0; i < length; i++) {
            EXPECT_TRUE(seg.lookup(keys[i], kv));
            EXPECT_EQ(keys[i], kv.key_);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(segs[i]), kv.value_);
        }

        // update (100, 100+0xdeadbeefaaaa0000) to a bunch of new entries
        const int len2 = 6;
        key_t update_keys[len2] = {100, 102, 104, 106, 109, 115};
        std::vector<KeyValue<key_t, uintptr_t>>update_list;
        for (size_t i = 0; i < len2; i++) {
            update_list.push_back(KeyValue<key_t, uintptr_t>(update_keys[i], update_keys[i] + 0xdeadbeefbbbb0000));
        }
        EXPECT_TRUE(seg.lookup(100, kv));
        EXPECT_EQ(100, kv.key_);
        EXPECT_EQ((uintptr_t)segs[5], kv.value_);
        EXPECT_TRUE(seg.batch_update(in_array[5], update_list, true));

        // 4 buckets, each bucket has 2 keys
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(7, seg.sbucket_list_[2].num_keys()); // 80, 100, 102, 104, 106, 109, 115
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        // look up all updated entries: 100, 102, 104, 106, 109, 115
        for (size_t i = 0; i < len2; i++) {
            seg.lookup(update_keys[i], kv);
            EXPECT_EQ(update_keys[i], kv.key_);
            EXPECT_EQ(update_keys[i] + 0xdeadbeefbbbb0000, kv.value_);
        }
        // look up all old keys except 100
        for (size_t i = 0; i < length; i++) {
            if (keys[i] == 100) continue;
            seg.lookup(keys[i], kv);
            EXPECT_EQ(keys[i], kv.key_);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(segs[i]), kv.value_);
        }

        // delete all segments
        for (size_t i = 0; i < length; i++) {
            delete segs[i];
        }
    }

    TEST(Segment, batch_update_multi_bucket) {
        using SegmentType = Segment<key_t,  8>;

        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        SegmentType *segs[length];
        for (size_t i = 0; i < length; i++) {
            std::vector<KeyValue<key_t, uintptr_t>> pivot_list(1, KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
            segs[i] = new SegmentType(1, 1, LinearModel<key_t>(0, 0), pivot_list.begin(), pivot_list.end());
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], reinterpret_cast<uintptr_t>(segs[i])));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.25;
        Segment<key_t,  8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // 4 buckets, each bucket has 2 keys and 2 empty slots
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(2, seg.sbucket_list_[2].num_keys()); // 80, 100
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        KeyValue<key_t, uintptr_t> kv;
        // look up all inserted keys
        uintptr_t value;
        for (size_t i = 0; i < length; i++) {
            EXPECT_TRUE(seg.lookup(keys[i], kv));
            EXPECT_EQ(keys[i], kv.key_);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(segs[i]), kv.value_);
        }

        // update (100, 100+0xdeadbeefaaaa0000) to a bunch of new entries
        const int len2 = 6;
        key_t update_keys[len2] = {100, 102, 104, 106, 121, 123};
        std::vector<KeyValue<key_t, uintptr_t>>update_list;
        for (size_t i = 0; i < len2; i++) {
            update_list.push_back(KeyValue<key_t, uintptr_t>(update_keys[i], update_keys[i] + 0xdeadbeefbbbb0000));
        }
        EXPECT_TRUE(seg.lookup(100, kv));
        EXPECT_EQ((uintptr_t)segs[5], kv.value_);
        EXPECT_TRUE(seg.batch_update(in_array[5], update_list, true));

        // 4 buckets, each bucket has 2 keys
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(5, seg.sbucket_list_[2].num_keys()); // 80, 100, 102, 104, 106
        EXPECT_EQ(4, seg.sbucket_list_[3].num_keys()); // 120, 121, 123, 140

        // look up all updated entries: 100, 102, 104, 106, 109, 115
        for (size_t i = 0; i < len2; i++) {
            seg.lookup(update_keys[i], kv);
            EXPECT_EQ(update_keys[i], kv.key_);
            EXPECT_EQ(update_keys[i] + 0xdeadbeefbbbb0000, kv.value_);
        }
        // look up all old keys except 100
        for (size_t i = 0; i < length; i++) {
            if (keys[i] == 100) continue;
            seg.lookup(keys[i], kv);
            EXPECT_EQ(keys[i], kv.key_);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(segs[i]), kv.value_);
        }

        // delete all segments
        for (size_t i = 0; i < length; i++) {
            delete segs[i];
        }
    }

/*
    TEST(Segment, batch_update_old_seg_not_in_new_segs) {
        using SegmentType = Segment<key_t,  8>;

        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        SegmentType *segs[length];
        for (size_t i = 0; i < length; i++) {
            std::vector<KeyValue<key_t, uintptr_t>> pivot_list(1, KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
            segs[i] = new SegmentType(1, 1, LinearModel<key_t>(0, 0), pivot_list.begin(), pivot_list.end());
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], reinterpret_cast<uintptr_t>(segs[i])));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.25;
        Segment<key_t,  8> seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // 4 buckets, each bucket has 2 keys and 2 empty slots
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(2, seg.sbucket_list_[2].num_keys()); // 80, 100
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        // look up all inserted keys
        uintptr_t value;
        for (size_t i = 0; i < length; i++) {
            EXPECT_TRUE(seg.lookup(keys[i], value));
            EXPECT_EQ(reinterpret_cast<uintptr_t>(segs[i]), value);
        }

        // update (100, 100+0xdeadbeefaaaa0000) to a bunch of new entries
        const int len2 = 6;
        key_t update_keys[len2] = {82, 102, 104, 106, 109, 115}; // 82 is not the old key 100
        std::vector<KeyValue<key_t, uintptr_t>>update_list;
        for (size_t i = 0; i < len2; i++) {
            update_list.push_back(KeyValue<key_t, uintptr_t>(update_keys[i], update_keys[i] + 0xdeadbeefbbbb0000));
        }
        EXPECT_TRUE(seg.lookup(100, value));
        EXPECT_TRUE(seg.batch_update(value, update_list, true));

        // 4 buckets, each bucket has 2 keys
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(7, seg.sbucket_list_[2].num_keys()); // 80, 82, 102, 104, 106, 109, 115
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        // look up all updated entries: 82, 102, 104, 106, 109, 115
        for (size_t i = 0; i < len2; i++) {
            seg.lookup(update_keys[i], value);
            EXPECT_EQ(update_keys[i] + 0xdeadbeefbbbb0000, value);
        }
        // look up all old keys except 100
        for (size_t i = 0; i < length; i++) {
            if (keys[i] == 100) continue;
            seg.lookup(keys[i], value);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(segs[i]), value);
        }

        seg.lookup(100, value); // should find 82
        EXPECT_EQ(82 + 0xdeadbeefbbbb0000, value);

        // delete all segments
        for (size_t i = 0; i < length; i++) {
            delete segs[i];
        }
    }
*/
        TEST(Segment, batch_update_d_bucket_entry) {
        using DataBucketType = Bucket<KeyListValueList<key_t, value_t, MAX_DATA_BUCKET_SIZE>,
                                  key_t, value_t, MAX_DATA_BUCKET_SIZE>;
        using SegmentType = Segment<key_t,  8>;

        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        DataBucketType *d_buckets[length];
        for (size_t i = 0; i < length; i++) {
            d_buckets[i] = new DataBucketType();
            d_buckets[i]->insert(KeyValue<key_t, value_t>(keys[i], keys[i]), true);
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], reinterpret_cast<uintptr_t>(d_buckets[i])));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 0.25;
        SegmentType seg(length, fill_ratio, model, in_array.begin(), in_array.end());
        
        // 4 buckets, each bucket has 2 keys and 2 empty slots
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(2, seg.sbucket_list_[2].num_keys()); // 80, 100
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        KeyValue<key_t, uintptr_t> kv;
        // look up all inserted keys
        uintptr_t value;
        for (size_t i = 0; i < length; i++) {
            EXPECT_TRUE(seg.lookup(keys[i], kv));
            EXPECT_EQ(keys[i], kv.key_);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(d_buckets[i]), kv.value_);
        }

        // update (100, 100+0xdeadbeefaaaa0000) to a bunch of new entries
        const int len2 = 6;
        key_t update_keys[len2] = {100, 102, 104, 106, 109, 115};
        std::vector<KeyValue<key_t, uintptr_t>>update_list;
        for (size_t i = 0; i < len2; i++) {
            update_list.push_back(KeyValue<key_t, uintptr_t>(update_keys[i], update_keys[i] + 0xdeadbeefbbbb0000));
        }
        EXPECT_TRUE(seg.lookup(100, kv));
        EXPECT_EQ(100, kv.key_);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(d_buckets[5]), kv.value_);
        EXPECT_TRUE(seg.batch_update(in_array[5], update_list, false));

        // 4 buckets, each bucket has 2 keys
        // Each bucket has 6 empty slots
        EXPECT_EQ(4, seg.num_bucket_);
        EXPECT_EQ(2, seg.sbucket_list_[0].num_keys()); // 0, 20
        EXPECT_EQ(2, seg.sbucket_list_[1].num_keys()); // 40, 60
        EXPECT_EQ(7, seg.sbucket_list_[2].num_keys()); // 80, 100, 102, 104, 106, 109, 115
        EXPECT_EQ(2, seg.sbucket_list_[3].num_keys()); // 120, 140

        // look up all updated entries: 100, 102, 104, 106, 109, 115
        for (size_t i = 0; i < len2; i++) {
            seg.lookup(update_keys[i], kv);
            EXPECT_EQ(update_keys[i], kv.key_);
            EXPECT_EQ(update_keys[i] + 0xdeadbeefbbbb0000, kv.value_);
        }
        // look up all old keys except 100
        for (size_t i = 0; i < length; i++) {
            if (keys[i] == 100) continue;
            seg.lookup(keys[i], kv);
            EXPECT_EQ(keys[i], kv.key_);
            EXPECT_EQ(reinterpret_cast<uintptr_t>(d_buckets[i]), kv.value_);
        }

        // delete all segments
        for (size_t i = 0; i < length; i++) {
            delete d_buckets[i];
        }
    }


    TEST(Segment, lower_and_upper_bound){
        // write unit test for segment::lower_bound and segment::upper_bound
        // construct a segment
        key_t keys[] = {0,20,40,60,80,100,120,140};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());

        // test lower_bound
        auto it = seg.lower_bound(0);
        EXPECT_EQ(0, it->key_);
        it = seg.lower_bound(1);
        EXPECT_EQ(20, it->key_);
        it = seg.lower_bound(20);
        EXPECT_EQ(20, it->key_);
        it = seg.lower_bound(21);
        EXPECT_EQ(40, it->key_);
        it = seg.lower_bound(140);
        EXPECT_EQ(140, it->key_);
        it = seg.lower_bound(141);

        EXPECT_TRUE(it == seg.cend());

        // // test upper_bound
        // it = seg.upper_bound(0);
        // EXPECT_EQ(20, it->key_);
        // it = seg.upper_bound(1);
        // EXPECT_EQ(20, it->key_);
        // it = seg.upper_bound(20);
        // EXPECT_EQ(40, it->key_);
        // it = seg.upper_bound(21);
        // EXPECT_EQ(40, it->key_);
        // it = seg.upper_bound(140);
        // EXPECT_TRUE(it == seg.cend());
        // it = seg.upper_bound(141);
        // EXPECT_TRUE(it == seg.cend());
        

        int idx = 0;
        auto lower = seg.lower_bound(0); // shallow copy is enough
        auto upper = seg.upper_bound(120);
        // query key range [0,120] using lower_bound and upper_bound
        for (it = lower; it != upper; ++it) {
            EXPECT_TRUE(it->key_ >= 0 && it->key_ <= 120);
            EXPECT_EQ(it->key_, keys[idx]);
            //cout<<"key: "<<it->key_<<endl;
            idx++;
        }
        EXPECT_EQ(7, idx);

    
        // alternative:
        // store the upper_bound(99) iterator to avoid calling upper_bound(99) multiple times
        // may have an unexpected behavior if the segment is modified
        // auto upper = seg.upper_bound(99);
        
        idx = 1;
        lower = seg.lower_bound(1);
        upper = seg.upper_bound(99);
        // query key range [1,99] using lower_bound and upper_bound
        for (it = lower; it != upper; ++it) {
            EXPECT_TRUE(it->key_ >= 1 && it->key_ <= 99);
            EXPECT_EQ(it->key_, keys[idx]);
            //cout<<"key: "<<it->key_<<endl;
            idx++;
        }
        EXPECT_EQ(5, idx);

        idx = 1;
        lower = seg.lower_bound(1);
        upper = seg.upper_bound(150);
        // query key range [1,150] using lower_bound and upper_bound
        for (it = lower; it != upper; ++it) {
            EXPECT_TRUE(it->key_ >= 1 && it->key_ <= 150);
            EXPECT_EQ(it->key_, keys[idx]);
            //cout<<"key: "<<it->key_<<endl;
            idx++;
        }
        EXPECT_EQ(8, idx);
    }

    TEST(Segment, segment_and_batch_update){
        // construct a segment
        key_t keys[] = {0,10,20,30};
        std::vector<KeyValue<key_t, uintptr_t>> in_array;
        size_t length = sizeof(keys)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            in_array.push_back(KeyValue<key_t, uintptr_t>(keys[i], keys[i]));
        }
        // model is y=0.05x
        LinearModel<key_t> model(0.05,0);
        double fill_ratio = 1;
        Segment<key_t, 4> seg(length, fill_ratio, model, in_array.begin(), in_array.end());

        std::vector<KeyValue<key_t,uintptr_t>> insert_anchors;
        key_t keys2[] = {30,40,50,60,70,190,191,192,193,194,195,196,197,400,410,420,430};
        // result should be {0,10,20,30,40,50,60,70,|190,191,192,193,194,195,196,197,|400,410,420,430}
        // total 20 keys
        length = sizeof(keys2)/sizeof(key_t);
        for (size_t i = 0; i < length; i++) {
            insert_anchors.push_back(KeyValue<key_t, uintptr_t>(keys2[i], keys2[i]));
        }
        std::vector<KeyValue<key_t,uintptr_t>> new_segs;
        bool success = seg.segment_and_batch_update(fill_ratio, insert_anchors, new_segs);
        
        EXPECT_TRUE(success);
        EXPECT_EQ(3, new_segs.size());
        EXPECT_EQ(0, new_segs[0].key_);
        EXPECT_EQ(190, new_segs[1].key_);
        EXPECT_EQ(400, new_segs[2].key_);

        Segment<key_t, 4> *seg1 = reinterpret_cast<Segment<key_t, 4> *>(new_segs[0].value_);
        Segment<key_t, 4> *seg2 = reinterpret_cast<Segment<key_t, 4> *>(new_segs[1].value_);
        Segment<key_t, 4> *seg3 = reinterpret_cast<Segment<key_t, 4> *>(new_segs[2].value_);


        EXPECT_EQ(2, seg1->num_bucket_);
        EXPECT_EQ(2, seg2->num_bucket_);
        EXPECT_EQ(1, seg3->num_bucket_);

        KeyValue<key_t, uintptr_t> kv;
        //check all other keys are in the new segments
        uintptr_t value = 0;
        size_t count = 0;
        vector<Segment<key_t, 4>*> segs={seg1,seg2,seg3};
        vector<key_t> key_list = {0,10,20,30,40,50,60,70,190,191,192,193,194,195,196,197,400,410,420,430};
        for(size_t i=0;i<segs.size();i++){
            for (size_t j=0;j<segs[i]->size();j++,count++){
                success = false;
                success = segs[i]->lookup(key_list[count], kv);
                
                EXPECT_TRUE(success);
                EXPECT_EQ(key_list[count], kv.key_);
                EXPECT_EQ(key_list[count], kv.value_);
            }
        }

        delete seg1;
        delete seg2;
        delete seg3;
    }


}
