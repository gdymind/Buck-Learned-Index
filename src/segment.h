
#pragma once

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"
#include "linear_model.h"
#include "segmentation.h"

namespace buckindex {


// T is the key type, SBUCKET_SIZE is the size of S-Bucket; value type is uintptr_t
template<typename T, size_t SBUCKET_SIZE>
class Segment {
public:
    using SegmentType = Segment<T, SBUCKET_SIZE>;
    using KeyValuePtrType = KeyValue<T, uintptr_t>;
    using BucketType = Bucket<KeyValueList<T, uintptr_t,  SBUCKET_SIZE>, T, uintptr_t, SBUCKET_SIZE>;
    // T base; // key compression
    // TBD: flag to determine whether it has rebalanced

    // stats
#ifdef BUCKINDEX_DEBUG
    inline static int fail_predict = 0;
    inline static int success_predict = 0;
    inline static int num_locate = 0;
    // inline static int num_insert = 0;
    // inline static int num_insert_fail = 0;
    inline static int fail_distance = 0;

    inline static int fail_predict_bulk = 0;
    inline static int success_predict_bulk = 0;
    inline static int fail_distance_bulk = 0;

    inline static int num_bucket_rebalance = 0;
    inline static int num_bucket_rebalance_fail = 0;

    inline static int num_segment_and_update = 0;
    inline static int num_segment_and_update_fail = 0;

#endif

    int num_bucket_; // total num of buckets
    BucketType* sbucket_list_; // a list of S-Buckets

    // default constructors
    Segment(){
        num_bucket_ = 0; // indicating it is empty now
        sbucket_list_ = nullptr;
        is_bottom_seg_ = true;
    }

    /**
     * @brief Parameterized Constructor
     * Pre-requisite: list of entries must be sorted
     * @param num_kv number of key-value pairs to be inserted
     * @param fill_ratio the average fill ratio of the all S-Buckets
     * @param model the linear model(before scaling) used to predict the bucket ID
     * @param it the start iterator of the list of entries
     * @param end the end iterator of the list of entries
    */
    template<typename IterType>
    Segment(size_t num_kv, double fill_ratio, const LinearModel<T> &model, 
            IterType it, IterType end, bool is_bottom_seg)
    :model_(model), is_bottom_seg_(is_bottom_seg){
        //assert(it+num_kv == end); // + operator may not be supported 
        assert(num_kv>0);
        assert(fill_ratio > 0.01 && fill_ratio <= 1);
        size_t num_slot = ceil(num_kv / fill_ratio);
        num_bucket_ = ceil((double)num_slot / SBUCKET_SIZE);
        assert((int)num_bucket_ > 0);
        sbucket_list_ = new BucketType[num_bucket_];
        model_.expand(1/fill_ratio);

        // model_based insertion
        // normal case: insert in the bucket of prdiction
        // two corner cases:
        //      1. if predicted bucket is full, find the next avilable bucket to insert
        //      2. if the remaining slots are not enough for the future insertion,
        //          insert at the nearest bucket in which future insertions have enough slots

    
        size_t remaining_slots = num_bucket_ * SBUCKET_SIZE;
        size_t remaining_keys = num_kv;
        size_t buckID = 0;
        size_t current_max_bukID = 0;
        for(;it!=end;it++){
            assert(remaining_keys <= remaining_slots);
            buckID = model_.predict(it->get_key()) / SBUCKET_SIZE;

            while(buckID + 1 < num_bucket_ && sbucket_list_[buckID].num_keys()==SBUCKET_SIZE){
                buckID++; // search forwards until find a bucket with empty slot
            }

            // the new remaining_slots if the key is inserted here
            remaining_slots = SBUCKET_SIZE * (num_bucket_ - buckID) - sbucket_list_[buckID].num_keys();
            if(remaining_keys > remaining_slots){ // refuse to insert in this place 
                                                  // and find the nearest bucket backwards so that it can be put in
                buckID = num_bucket_ - 1 - (remaining_keys-1)/SBUCKET_SIZE;
                remaining_slots = SBUCKET_SIZE * (num_bucket_ - buckID) - sbucket_list_[buckID].num_keys();
                assert(buckID >= current_max_bukID); // ensure the order between buckets
            }
            assert(remaining_slots >= remaining_keys);
            current_max_bukID = std::max(current_max_bukID, buckID);
            // else: accept to insert in this bucket

            bool succuess = sbucket_list_[buckID].insert(*it, true, 0 /*hint*/);
            assert(succuess);

#ifdef BUCKINDEX_DEBUG
            // int act = buckID;
            // int pred = model_.predict(it->get_key()) / SBUCKET_SIZE;
            // if(act != pred){
            //     fail_predict_bulk++;
            //     fail_distance_bulk += abs(act-pred);
            // }
            // else{
            //     success_predict_bulk++;
            // }

#endif
            remaining_keys--;
            remaining_slots--;
        }
    }

    ~Segment(){
        if (sbucket_list_ != nullptr) {
            for(size_t i = 0; i<num_bucket_;i++){
                sbucket_list_[i].~BucketType();
            }
            delete[] sbucket_list_; // delete the array of pointers
        }
    }

    bool is_bottom_seg() const{
        return is_bottom_seg_;
    }

    // iterator-related
    // class UnsortedIterator;
    // UnsortedIterator unsorted_begin() {return UnsortedIterator(this, 0); }
    // UnsortedIterator unsorted_end() {return UnsortedIterator(this, SBUCKET_SIZE * num_bucket_); }
    class const_iterator;
    const_iterator cbegin() {return const_iterator(this, 0); }
    const_iterator cend() {return const_iterator(this, this->size()); }

    // return the iterator of the smallest element >= key
    const_iterator lower_bound(T key);

    // return the iterator of the smallest element > key
    const_iterator upper_bound(T key);

    T get_pivot() const{
        assert(num_bucket_>0);
        return sbucket_list_[0].get_pivot();
    }

    // TODO: change to member variable?
    /**
     * @brief return the number of key-value pairs in the segment
    */
    inline size_t size(){
        size_t ret=0;
        for (size_t i=0;i<num_bucket_;i++){
            ret += sbucket_list_[i].num_keys();
        }
        return ret;
    }

    size_t mem_size() const{
        size_t ret = 0;
        ret += sizeof(SegmentType); // model_, num_bucket_, sbucket_list_
        ret += num_bucket_ * sizeof(BucketType); // sbucket_list_

        // bucket has no pointer type member variable, 
        // so no need to count the danymic memory from buckets
        return ret;
    }

    // TODO: a non-pivoting version (deferred)

    /**
     * @brief lookup the largest element <= key in the segment
     * @param key the key to be looked up
     * @param kvptr the largest element <= key
     * @param next_kvptr the smallest element > key; if not found, reuturn <max_key, 0>
     * @return true if found, false otherwise
    */
    bool lb_lookup(T key, KeyValuePtrType &kvptr, KeyValuePtrType &next_kvptr) const;

    /**
     * @brief return the S-Bucket at the given position
     * @param pos the position of the S-Bucket
     * @return the pointer to the S-Bucket at the given position
    */
    BucketType *get_bucket(int pos) {
        assert(pos >= 0 && pos < num_bucket_);
        return &sbucket_list_[pos];
    }


    /**
     * @brief insert a key-value pair into the segment
     *  If the target S-Bucket is full, reblance the bucket with its right neighbor
     * @param kvptr the key-value pair to be inserted
     * @return true if success, false otherwise
    */
    bool insert(KeyValue<T, uintptr_t> &kvptr);

    // scale_and_segmentation() is called by BucketIndex when insert/bucket_rebalance failed
    // return a list of segments after scale and segmentation; put the segments into the tree index, then destroy the old seg
    // new_segs = old_seg->scale_and_seg()
    // bucket_index.update_seg(old_seg, new_segs)
    // ~old_seg()
    // assumption: error bound is the sbucket_size
    // NOTE: the SBUCKET_SIZE of new segments is the same as the old one
    // bool scale_and_segmentation(double fill_ratio, std::vector<KeyValue<T,uintptr_t>> &new_segs);

    bool update(KeyValuePtrType &old_entry, KeyValuePtrType &new_entry) {
        T old_key = old_entry.key_;
        T new_key = new_entry.key_;
        assert(old_key == new_key);
        unsigned int buckID = locate_buck(old_key);
        bool success = sbucket_list_[buckID].update(new_entry);
        assert(success);
        return success;
    }

    /**
     * @brief replace the old segment with new_pivots
     * Asuume that the new_pivots can be inserted into different buckets
     * But actually, the new_pivots are always inserted into the same bucket at this point
     * We can still use this multi-bucket version to support the future multi-bucket insertion
     * And the multi-bucket insertion overhead is small in the same bucket case
     * @param old_pivot: the old segment or d-bucket pointer to be replaced
     * @param new_pivots: the new pivots to be inserted
     * @param is_segment: true if old_pivot is a segment, false if old_pivot is a d-bucket
     * @return true if success, false if fail
    */
    bool batch_update(KeyValuePtrType old_pivot, std::vector<KeyValuePtrType> &new_pivots, bool is_segment) {
        T old_pivot_key = old_pivot.key_;

        // check if have enough space to insert new_pivots
        int cnt_current_bucket = 0;
        int first_buckID = locate_buck(new_pivots[0].key_);
        int current_buckID = first_buckID;
        for (int i = 0; i < new_pivots.size(); i++) {
            int buckID = current_buckID;
            while(buckID + 1 < num_bucket_ && sbucket_list_[buckID+1].get_pivot() <= new_pivots[i].key_){
                buckID++;
            }

            if (buckID == current_buckID) cnt_current_bucket++;
            else {
                int left = SBUCKET_SIZE - sbucket_list_[current_buckID].num_keys();
                // if (current_buckID == first_buckID) left++; // the first bucket has one more space (old_seg)
                if (left < cnt_current_bucket) { return false; }
                cnt_current_bucket = 1;
                current_buckID = buckID;
            }
        }
        if (cnt_current_bucket > 0) {
            int left = SBUCKET_SIZE - sbucket_list_[current_buckID].num_keys();
            // if (current_buckID == first_buckID) left++; // the first bucket has one more space (old_seg)
            if (left < cnt_current_bucket) { return false; }
        }

        // insert new_pivots except the first one
        if (new_pivots.size() > 1) current_buckID = locate_buck(new_pivots[1].key_);
        for (int i = 1; i < new_pivots.size(); i++) {
            while(current_buckID + 1 < num_bucket_ && sbucket_list_[current_buckID+1].get_pivot() <= new_pivots[i].key_){
                current_buckID++;
            }
            bool success = sbucket_list_[current_buckID].insert(new_pivots[i], true, 0 /*hint*/);
            assert(success);
        }

        // insert the first pivot
        bool success;
        assert(old_pivot_key == new_pivots[0].key_);
        if (old_pivot_key == new_pivots[0].key_) {  // update the first pivot
            success = sbucket_list_[first_buckID].update(new_pivots[0]);
            assert(success);
        } else { // insert the first pivot, and invalidate the old pivot
            success = sbucket_list_[first_buckID].insert(new_pivots[0], true, 0 /*hint*/);
            assert(success);

            int buckID = locate_buck(old_pivot_key);
            int pos = sbucket_list_[buckID].get_pos(old_pivot_key);
            assert(pos >= 0);
            sbucket_list_[buckID].invalidate(pos);
        }  
        
        return true;
    }

    // /**
    // * scale the segment and batch insert the new keys, and remove the entries within the new keys range
    // * @param fill_ratio: the fill ratio of the new segment
    // * @param insert_anchors: the new keys to be inserted; keys are sorted
    // * @param new_segs: the new segments after scale and batch insert
    // * @return true if scale and batch insert success, false otherwise
    // * NOTE: the SBUCKET_SIZE of new segments is the same as the old one
    // * NOTE: the new segments are not inserted into the tree index and old segment is not destroyed
    // */
    // bool segment_and_batch_update(double fill_ratio, const std::vector<KeyValue<T,uintptr_t>> &insert_anchors,std::vector<KeyValue<T,uintptr_t>> &new_segs);

private:
    LinearModel<T> model_;
    bool is_bottom_seg_;

    // TODO: TBD-do we explicitly store x_sum, y_sum, xx_sum and xy_sum

    inline unsigned int predict_buck(T key) const { // get the predicted S-Bucket ID based on the model computing
        unsigned int buckID = (unsigned int)(model_.predict(key) / SBUCKET_SIZE);
        
        buckID = std::max(buckID, 0U);
        buckID = std::min(buckID, (unsigned int)(num_bucket_-1)); // ensure num_bucket>0
        assert(buckID < num_bucket_);
        return buckID;
    }

    inline unsigned int locate_buck(T key) const {
        // prediction may be incorrect, this function is to find the exact bucket whose range covers the key based on prediction
        // Step1: call predict_buck to get an intial position
        // Step2: search neighbors to find the exact match (linear search)
        unsigned int buckID = predict_buck(key); // ensure buckID is valid s

        // search forward
        while(buckID+1<num_bucket_ && sbucket_list_[buckID+1].get_pivot() <= key){
            buckID++;
        }
        // search backward
        while(buckID>0 && sbucket_list_[buckID].get_pivot() > key){
            buckID--;
        }

        // //std::cout << "buckID: " << buckID << std::endl;
        // if(sbucket_list_[buckID].get_pivot() <= key){ // search forwards
        //     while(buckID+1<num_bucket_){
        //         if(sbucket_list_[buckID+1].get_pivot() > key){
        //             break;
        //         }
        //         buckID++;
        //     }
        // }
        // else{ // search backwards
        //     while(buckID>0){
        //         if(sbucket_list_[buckID-1].get_pivot() <= key){
        //             buckID--;
        //             break;
        //         }
        //         buckID--;
        //     }
        // }
#ifdef BUCKINDEX_DEBUG
        num_locate++;
        auto pred_buckID = predict_buck(key);
        if (buckID != pred_buckID){
            fail_predict++;
            fail_distance += abs(((int)buckID - (int)pred_buckID));
            
            
        }
        else{
            success_predict++;
        }
#endif
        return buckID;
    }

    bool bucket_rebalance(unsigned int buckID);
};


/*
template<typename T, size_t SBUCKET_SIZE>
bool Segment<T, SBUCKET_SIZE>::scale_and_segmentation(double fill_ratio, std::vector<KeyValue<T,uintptr_t>> &new_segs){

    // the error_bound should be less than 1/2 of the bucket size.
    uint64_t error_bound = 0.5 * SBUCKET_SIZE;

    // collect all the valid keys (sorted)
    // done by segment::SortedIterator 

    // run the segmentation algorithm
    std::vector<Cut<T>> out_cuts;
    out_cuts.clear();

    Segmentation<SegmentType, T>::compute_dynamic_segmentation(*this, out_cuts, error_bound);

    // put result of segmentation into multiple segments
    size_t start_pos = 0;
    for(size_t i = 0;i<out_cuts.size();i++){
        // using dynamic allocation in case the segment is destroyed after the loop
        SegmentType* seg = new SegmentType(out_cuts[i].size_, fill_ratio, out_cuts[i].get_model(), const_iterator(this, start_pos), const_iterator(this, start_pos+out_cuts[i].size_));
        T key = out_cuts[i].start_key_;
        KeyValue<T,uintptr_t> kv(key, (uintptr_t)seg);
        new_segs.push_back(kv);
        start_pos += out_cuts[i].size_;
    }
    assert(start_pos == this->size());
    return true;
}
*/


// template<typename T, size_t SBUCKET_SIZE>
// bool Segment<T, SBUCKET_SIZE>::segment_and_batch_update(
//     double fill_ratio, 
//     const std::vector<KeyValue<T,uintptr_t>> &input_pivots,
//     std::vector<KeyValue<T,uintptr_t>> &new_segs){

//     // the error_bound should be less than 1/2 of the bucket size.
//     uint64_t error_bound = 0.5 * SBUCKET_SIZE;

//     // collect all the valid keys (sorted)
//     std::vector<KeyValue<T,uintptr_t>> list;
//     T input_min = input_pivots.front().key_;
//     T input_max = input_pivots.back().key_;
//     auto it = this->cbegin();
//     // current segment: insert entries before the input_pivots range
//     for(;it!=this->cend();it++){ 
//         KeyValue<T,uintptr_t> kv = *it;
//         if(kv.key_ >= input_min) break;
//         list.push_back(kv);
//     }
//     assert(it == this->cend() || input_min == (*it).key_);
//     // current segment: skip entries in the input_pivots range
//     for(;it!=this->cend();it++){
//         KeyValue<T,uintptr_t> kv = *it;
//         if((*it).key_ > input_max) break;
//     }
//     // input_pivots: insert the input_pivots range
//     for(auto it2 = input_pivots.begin(); it2 != input_pivots.end(); it2++){
//         list.push_back(*it2);
//     }
//     // current segment: insert entries after the input_pivots range
//     for(;it!=this->cend();it++){ 
//         KeyValue<T,uintptr_t> kv = *it;
//         list.push_back(*it);
//     }

//     // sort(list.begin(), list.end());

//     // run the segmentation algorithm
//     std::vector<Cut<T>> out_cuts;
//     out_cuts.clear();

//     vector<LinearModel<T>> out_models;
//     Segmentation<std::vector<KeyValue<T,uintptr_t>>, T>::compute_dynamic_segmentation(list, out_cuts, out_models, error_bound);

//     // put result of segmentation into multiple segments
//     size_t start_pos = 0;
//     for(size_t i = 0;i<out_cuts.size();i++){
//         // using dynamic allocation in case the segment is destroyed after the loop
//         // SegmentType* seg = new SegmentType(out_cuts[i].size_, fill_ratio, out_cuts[i].get_model(), list.begin() + start_pos, list.begin() + start_pos+out_cuts[i].size_);
//         SegmentType* seg = new SegmentType(out_cuts[i].size_, fill_ratio, out_models[i], list.begin() + start_pos, list.begin() + start_pos+out_cuts[i].size_);
//         T key = out_cuts[i].start_key_;
//         KeyValue<T,uintptr_t> kv(key, (uintptr_t)seg);
//         new_segs.push_back(kv);
//         start_pos += out_cuts[i].size_;
//     }
//     assert(start_pos == list.size());
//     return true;
// }

template<typename T, size_t SBUCKET_SIZE>
bool Segment<T, SBUCKET_SIZE>::bucket_rebalance(unsigned int buckID) { // re-balance between adjcent bucket
    // Case 1: migrate forwards

    // Case 2: migrate backwards

    // if two directions are possible, migrate to the bucket with fewer element
    //std::cout<<buckID<<std::endl;
    if(num_bucket_ == 0 || num_bucket_ == 1){return false;}

    size_t src_buck_num = sbucket_list_[buckID].num_keys();
    size_t des_buck_num = 0;

    bool migrate_forwards = true;
    if(buckID == num_bucket_-1 || (buckID != 0 && sbucket_list_[buckID+1].num_keys() > sbucket_list_[buckID-1].num_keys())){
        migrate_forwards = false;
    }

    if(migrate_forwards){
        assert(buckID + 1 < num_bucket_);
        des_buck_num = sbucket_list_[buckID+1].num_keys();
        if(des_buck_num == SBUCKET_SIZE){return false;}
        size_t median = (src_buck_num + des_buck_num)/2; // Be careful, current bucket can have one less element than the next one
        T new_pivot = sbucket_list_[buckID].find_kth_smallest(median+1).key_;

        // for concurrency, first insert new entries, then update pivot_, then remove old entries
        for(size_t i = 0;i<src_buck_num;i++){
            if(sbucket_list_[buckID].at(i).get_key()>=new_pivot){
                sbucket_list_[buckID+1].insert(sbucket_list_[buckID].at(i), false, 0 /*hint*/);
            }
        }
        sbucket_list_[buckID+1].set_pivot(new_pivot);


        for(size_t i = 0; i<SBUCKET_SIZE;i++){
            if(!sbucket_list_[buckID].valid(i)){
                continue;
            }
            if(sbucket_list_[buckID].at(i).get_key()>=new_pivot){
                sbucket_list_[buckID].invalidate(i);
            }
        }

    }
    else{
        assert(buckID - 1 >= 0);
        des_buck_num = sbucket_list_[buckID-1].num_keys();
        if(des_buck_num == SBUCKET_SIZE){return false;}

        size_t median = (src_buck_num + des_buck_num)/2; // Be careful, current bucket can have one less element than the next one
        size_t num_migration = src_buck_num - median;

        
        T new_pivot = sbucket_list_[buckID].find_kth_smallest(num_migration+1).key_;

        // for concurrency, first insert new entries, then update pivot_, then remove old entries
        for(size_t i = 0;i<sbucket_list_[buckID].num_keys();i++){
            if(sbucket_list_[buckID].at(i).get_key()<new_pivot){
                sbucket_list_[buckID-1].insert(sbucket_list_[buckID].at(i),false, 0 /*hint*/);
            }
        }
        sbucket_list_[buckID].set_pivot(new_pivot);
        for(size_t i = 0; i<SBUCKET_SIZE;i++){
            if(!sbucket_list_[buckID].valid(i)){
                continue;
            }
            if(sbucket_list_[buckID].at(i).get_key()<new_pivot){
                sbucket_list_[buckID].invalidate(i);
            }
        }

    }
    assert(sbucket_list_[buckID].num_keys() < SBUCKET_SIZE);
    return true;
}


template<typename T, size_t SBUCKET_SIZE>
bool Segment<T, SBUCKET_SIZE>::lb_lookup(T key, KeyValuePtrType &kvptr, KeyValuePtrType &next_kvptr) const {
    // cout << "In segment lb_lookup, key = " << key << ", num_bucket_ = " << num_bucket_ << ", pivot = " << sbucket_list_[0].get_pivot() << endl << flush;
    assert(num_bucket_>0);
    unsigned int buckID = locate_buck(key);
    // cout << "In segment lb_lookup, locate_buck(key): " << buckID << endl << flush;
    bool success = sbucket_list_[buckID].lb_lookup(key, kvptr, next_kvptr);
    
    // predict -> search within bucket ---if fail----> locate -> search (put a flag) (deferred)
    return success;
}

template<typename T, size_t SBUCKET_SIZE>
bool Segment<T, SBUCKET_SIZE>::insert(KeyValue<T, uintptr_t> &kv) {

    assert(num_bucket_>0);

    unsigned int buckID = locate_buck(kv.key_);

    if(sbucket_list_[buckID].num_keys() == SBUCKET_SIZE){
        if(!bucket_rebalance(buckID)){
            //std::cout<<"case 2"<<std::endl;
            return false;
        }
        buckID = locate_buck(kv.key_);

        // special case when the des bucket is full due to migration
        // and new_pivot is updated to be smaller than target key
        // Three adjcent buckets have totally one empty slot, 
        // so we think it's time to adjust segment
        if(sbucket_list_[buckID].num_keys() == SBUCKET_SIZE){
            //std::cout<<"case 1"<<std::endl;
            return false;
        }
    }

    // TODO: only the first bucket of the layer should do the assert
    // assert(kv.key_>=sbucket_list_[buckID].get_pivot());
    // bool ret = sbucket_list_[buckID].insert(kv, false, hint);
    // // target key should be within the pivots

    // if buckID == 0, key may be smaller than pivot
    // then we need to update the pivot
    
    bool ret = sbucket_list_[buckID].insert(kv, true, 0 /*hint*/);

    return ret;
}

// return the first element that is not less than key
template<typename T, size_t SBUCKET_SIZE>
typename Segment<T, SBUCKET_SIZE>::const_iterator Segment<T, SBUCKET_SIZE>::lower_bound(T key){
    assert(num_bucket_>0);
    unsigned int buckID = locate_buck(key);

    return const_iterator(this, buckID, key, true);
}

// return the first element that is greater than key
template<typename T, size_t SBUCKET_SIZE>
typename Segment<T, SBUCKET_SIZE>::const_iterator Segment<T, SBUCKET_SIZE>::upper_bound(T key){
    assert(num_bucket_>0);
    //unsigned int buckID = locate_buck(key);

    return const_iterator(this, 0, key, false);
}


/*
//TODO: unit test
template<typename T, size_t SBUCKET_SIZE>
class Segment<T, SBUCKET_SIZE>::UnsortedIterator {
public:
    using SegmentType = Segment<T, SBUCKET_SIZE>;

    explicit UnsortedIterator(SegmentType *segment) : segment_(segment) {
        assert(segment_ != nullptr);
        cur_pos_ = 0;
        size = segment->num_bucket_ * SBUCKET_SIZE;
        find_next_valid();
    }

    UnsortedIterator(SegmentType *segment, int pos) : segment_(segment) {
      assert(pos >= 0 && pos <= segment->num_bucket_ * SBUCKET_SIZE);
      cur_pos_ = pos;
      size = segment->num_bucket_ * SBUCKET_SIZE;
      // cur_pos_ is always at a valid position, except end()
      if (pos < size && !sbucket_list_[cur_pos_/SBUCKET_SIZE].valid(cur_pos_%SBUCKET_SIZE)) find_next_valid();
    }

    void operator++(int) {
        find_next_valid();
    }

    UnsortedIterator &operator++() {
        find_next_valid();
        return *this;
    }

    KeyValue<T, uintptr_t> operator*() const {
        return sbucket_list_[cur_pos_/SBUCKET_SIZE].at(cur_pos_%SBUCKET_SIZE);
    }

    bool operator==(const UnsortedIterator& rhs) const {
      return segment_ == rhs.segment_ && cur_pos_ == rhs.cur_pos_;
    }

    bool operator!=(const UnsortedIterator& rhs) const { return !(*this == rhs); };

private:
    SegmentType *segment_;
    int cur_pos_ = 0;  // current position in the sbucket list, 

    // skip invalid entries, and find the position of the next valid entry
    inline void find_next_valid() {
        if (cur_pos_ == size) return;
        cur_pos_++;
        while (cur_pos_ < size && !sbucket_list_[cur_pos_/SBUCKET_SIZE].valid(cur_pos_%SBUCKET_SIZE)) cur_pos_++;
    }
    size_t size;
};
*/

template<typename T, size_t SBUCKET_SIZE>
class Segment<T, SBUCKET_SIZE>::const_iterator {
public:
    using SegmentType = Segment<T, SBUCKET_SIZE>;

    const_iterator() {
        segment_ = nullptr;
        cur_buckID_ = 0;
        cur_index_ = 0;
    }

    /*
    // explicit const_iterator(SegmentType *segment) : segment_(segment) {
    //     assert(segment_ != nullptr);
    //     cur_buckID_ = 0;
    //     cur_index_ = 0;
    //     sorted_list_.clear();

    //     // find the first valid bucket
    //     while(!reach_to_end()){
    //         if(segment_->sbucket_list_[cur_buckID_].num_keys() == 0){
    //             cur_buckID_++;
    //         }
    //         else{
    //             segment_->sbucket_list_[cur_buckID_].get_valid_kvs(sorted_list_); 
    //             sort(sorted_list_.begin(), sorted_list_.end());
    //             break;
    //         }
    //     }       
    // }
    */

    // num of bucket to indicate the end
    const_iterator(SegmentType *segment, int pos) : segment_(segment) {
        assert(pos >= 0 && pos <= segment_->size());
        cur_buckID_ = 0;
        cur_index_ = 0;
        if(pos == segment_->size()){
            cur_buckID_ = segment_->num_bucket_;
            return;
        }

        // locate the bucket
        while(pos >= segment_->sbucket_list_[cur_buckID_].num_keys()){
            //if(pos<0) std::cout<<"pos: "<<pos<<", num_keys: "<<segment_->sbucket_list_[cur_buckID_].num_keys()<<std::endl;
            
            pos -= segment_->sbucket_list_[cur_buckID_].num_keys();
            cur_buckID_++;
        }

        // inside the bucket, locate the index
        segment_->sbucket_list_[cur_buckID_].get_valid_kvs(sorted_list_); 
        sort(sorted_list_.begin(), sorted_list_.end());
        cur_index_ = pos;
    }

    // find the first key >= key or the first key > key
    // allow_qual -> upper_bound
    const_iterator(SegmentType *segment, int buckID, T key, bool allow_equal) : segment_(segment) {
        assert(buckID >= 0 && buckID <= segment_->num_bucket_);
        cur_buckID_ = buckID;
        cur_index_ = 0;

        if(!allow_equal){ // indicating this is an upper bound iterator
            upper_bound = key;
            cur_buckID_ = segment_->num_bucket_;
            return;
        }

        // locate the bucket
        while(segment_->sbucket_list_[cur_buckID_].num_keys() == 0){
            cur_buckID_++;
            if(cur_buckID_ == segment_->num_bucket_) return;
        }
        
        segment_->sbucket_list_[cur_buckID_].get_valid_kvs(sorted_list_);
        sort(sorted_list_.begin(), sorted_list_.end());
        KeyValue<T, uintptr_t> kv;
        kv.key_ = key;
        if(allow_equal){ // make sure no matter what the value is, it will be the first key >= key
            kv.value_ = 0;
            cur_index_ = std::lower_bound(sorted_list_.begin(), sorted_list_.end(), kv) - sorted_list_.begin();
        }
        else{
            kv.value_ = std::numeric_limits<uintptr_t>::max(); // make sure no matter what the value is, it will be the first key > key
            cur_index_ = std::upper_bound(sorted_list_.begin(), sorted_list_.end(), kv) - sorted_list_.begin();
        }
        if(cur_index_ == sorted_list_.size()) {
            assert(cur_index_ > 0);
            cur_index_--;
            find_next();
        }
    }

    // disable upper bound iterator to be increased/decresed or deferenced 

    void operator++(int) {
        assert(upper_bound == std::numeric_limits<T>::max());
        find_next();
    }

    void operator--(int) {
        assert(upper_bound == std::numeric_limits<T>::max());
        find_previous();
    }

    // prefix ++it
    const_iterator &operator++() {
        assert(upper_bound == std::numeric_limits<T>::max());
        find_next();
        return *this;
    }

    bool has_next() const {
        if (reach_to_end()) return false;
        return cur_buckID_ < segment_->num_bucket_ || (cur_buckID_ == segment_->num_bucket_ && cur_index_ < sorted_list_.size());
    }

    bool has_prev() const {
        return !reach_to_begin();
    }

    // *it
    const KeyValue<T, uintptr_t> operator*() const {
        assert(upper_bound == std::numeric_limits<T>::max());
        return sorted_list_[cur_index_];
    }

    // it->
    const KeyValue<T, uintptr_t>* operator->() const {
        assert(upper_bound == std::numeric_limits<T>::max());
        return &(sorted_list_[cur_index_]);
    }

    // if rhs is an upper bound iterator, then it will return true, if cur_key > upper bound
    bool operator==(const const_iterator& rhs) const {
        if (sorted_list_.size() != 0 && cur_index_ < sorted_list_.size() && sorted_list_[cur_index_].key_ > rhs.upper_bound) {
            return true;
        }
        return segment_ == rhs.segment_ && cur_buckID_ == rhs.cur_buckID_ && cur_index_ == rhs.cur_index_;
    }

    bool operator!=(const const_iterator& rhs) const { 
        return !(*this == rhs);
    }

private:
    SegmentType *segment_;
    //int cur_pos_ = 0;  // current position in the sbucket list, 
    size_t cur_buckID_ = 0;
    size_t cur_index_ = 0;

    T upper_bound = std::numeric_limits<T>::max();

    std::vector<KeyValue<T, uintptr_t>> sorted_list_; // initialized when cbegin() is called or move to another bucket

    // find the next entry in the sorted list (Can cross boundary of bucket)
    inline void find_next() {
        if (reach_to_end()) return;
        cur_index_++;
        if(cur_index_ == sorted_list_.size()){
            cur_buckID_++;
            sorted_list_.clear();
            cur_index_ = 0;
            
            // find the next valid bucket
            while(!reach_to_end()){
                if(segment_->sbucket_list_[cur_buckID_].num_keys() == 0){
                    cur_buckID_++;
                }
                else{
                    segment_->sbucket_list_[cur_buckID_].get_valid_kvs(sorted_list_); 
                    sort(sorted_list_.begin(), sorted_list_.end());
                    break;
                }
            }
        }
    }

    // find the previous entry in the sorted list (Can cross boundary of bucket)
    inline void find_previous() {
        // cout << "seg.find_previous() for :" << cur_buckID_ << ", " << cur_index_ << endl;
        if (reach_to_begin()) return;
        if (cur_index_ == 0) {
            cur_buckID_--;
            cur_index_ = segment_->sbucket_list_[cur_buckID_].num_keys() - 1;
            while(!reach_to_begin()){
                if(segment_->sbucket_list_[cur_buckID_].num_keys() == 0){
                    cur_buckID_--;
                }
                else{
                    segment_->sbucket_list_[cur_buckID_].get_valid_kvs(sorted_list_); 
                    sort(sorted_list_.begin(), sorted_list_.end());
                    cur_index_ = sorted_list_.size() - 1;
                    break;
                }
            }
        }
        else {
            cur_index_--;
        }
    }

    bool reach_to_begin() const {
        return (cur_buckID_ == 0 && cur_index_ == 0);
    }

    bool reach_to_end() const{
        return (cur_buckID_ == segment_->num_bucket_);
    }
};

} // end namespace buckindex
