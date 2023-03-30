
#pragma once

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"
#include "linear_model.h"
#include "segmentation.h"

namespace buckindex {

template<typename T, typename V, size_t SBUCKET_SIZE>
class Segment {
public:
    //bool is_leaf_; // true -> segment; false -> segment group
    //Segment* parent_; // the parent Segment node, which enables bottom-up tranversal
    // T base; // key compression
    // TBD: flag to determine whether it has rebalanced

    using SegmentType = Segment<T, V, SBUCKET_SIZE>;


    size_t num_bucket_; // total num of buckets
    Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>* sbucket_list_; // a list of S-Buckets

    // default constructors
    Segment(){
        num_bucket_ = 0; // indicating it is empty now
        sbucket_list_ = nullptr;
    }


    // Parameterized Constructor
    // Pre-requisite: list of entries must be sorted before insertion

    // a constructor that recevices the number of entries, fill ratio, and the model(before expansion)
    // also pass a start iterator and an end iterator; iterate over the list and insert into the sbucket_list_
    template<typename IterType>
    Segment(size_t num_kv, double fill_ratio, const LinearModel<T> &model, IterType it, IterType end)
    :model_(model){
        //assert(it+num_kv == end); // + operator may not be supported 

        //create_bucket_and_load(num_kv,fill_ratio,model,it,end);

        assert(num_kv>0);
        assert(fill_ratio>0 && fill_ratio<=1);
        size_t num_slot = ceil(num_kv / fill_ratio);
        num_bucket_ = ceil((double)num_slot / SBUCKET_SIZE);
        sbucket_list_ = new Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>[num_bucket_];
        //model_.dump();
        model_.expand(1/fill_ratio);
        //model_.dump();
        //std::cout<<num_bucket_<<" "<<num_slot<<std::endl;

        // model_based insertion
        // normal case: insert in the bucket of prdiction
        // two corner cases:
        //      1. if predicted bucket is full, find the next avilable bucket to insert
        //      2. if the remaining slots are not enough for the future insertion,
        //          insert at the nearest bucket, so that future insertion has enough slots


        size_t remaining_slots = num_bucket_ * SBUCKET_SIZE;
        size_t remaining_keys = num_kv;
        size_t buckID = 0;
        for(;it!=end;it++){
            assert(remaining_keys <= remaining_slots);
            buckID = model_.predict(it->get_key()) / SBUCKET_SIZE; // TBD: suppose iterator iterate through KeyValue element
            // model predicts the offset, we translate it to buckID

            //std::cout<<"key: "<<it->get_key()<<" buckID: "<<buckID;
            while(buckID<num_bucket_ && sbucket_list_[buckID].num_keys()==SBUCKET_SIZE){
                buckID++; // search forwards until find a bucket with empty slot
            }
            if(buckID>=num_bucket_){
                buckID = num_bucket_-1;
            }
            // the new remaining_slots if the key is inserted here
            remaining_slots = SBUCKET_SIZE * (num_bucket_ - buckID) - sbucket_list_[buckID].num_keys();
            if(remaining_keys > remaining_slots){ // refuse to insert in this place // and find the nearest bucket backwards so that it can be put in
                buckID = num_bucket_ - 1 - (remaining_keys-1)/SBUCKET_SIZE;
                remaining_slots = SBUCKET_SIZE * (num_bucket_ - buckID) - sbucket_list_[buckID].num_keys();
                // update the remaining_slot if insert in the new buckID
            }
            // else: accept to insert in this bucket
            //std::cout<<" inserted buckID: "<<buckID<<std::endl;
            sbucket_list_[buckID].insert(*it, true); // TBD: suppose iterator iterate through KeyValue element
            remaining_keys--;
            remaining_slots--;
           
        }
    }

    ~Segment(){
        if (sbucket_list_ != nullptr) {
            for(size_t i = 0; i<num_bucket_;i++){
                sbucket_list_[i].~Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>();
            }
            delete[] sbucket_list_; // delete the array of pointers
        }
    }


    // iterator-related
    // class UnsortedIterator;
    // UnsortedIterator unsorted_begin() {return UnsortedIterator(this, 0); }
    // UnsortedIterator unsorted_end() {return UnsortedIterator(this, SBUCKET_SIZE * num_bucket_); }

    class const_iterator;
    const_iterator cbegin() {return const_iterator(this, 0); }
    const_iterator cend() {return const_iterator(this, this->size()); }
    
    //TODO:lower_bound, upper_bound 
    // const_iterator func(KEY_TYPE key){

    // }
    //for(it = being();it!= ned())

    // return the first element that is not less than key
    const_iterator lower_bound(T key);

    // return the first element that is greater than key
    const_iterator upper_bound(T key);


    // TBD: build a function / store a variable 
    // count the valid keys in segment
    inline size_t size(){
        size_t ret=0;
        for (size_t i=0;i<num_bucket_;i++){
            ret += sbucket_list_[i].num_keys();
        }
        return ret;
    }

    // TODO: a non-pivoting version (deferred)

    bool lookup(T key, V &value) const; //return the child pointer; return nullptr if not exist
    Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE> *get_bucket(int pos) {
        assert(pos >= 0 && pos < num_bucket_);
        return &sbucket_list_[pos];
    }

    // TODO: delete an entry that matches the value
    bool del_value(V value) {
        return true;
    } 

    // insert an entry to the target S-Bucket;
    // If the target S-Bucket is full, reblance the bucket with its right neighbor;
    // If bucket_rebalance does not work, insert() return false
    bool insert(KeyValue<T, V> &kvptr);

    // is called by BucketIndex when insert/bucket_rebalance failed
    // return a list of segments after scale and segmentation; put the segments into the tree index, then destroy the old seg
    // new_segs = old_seg->scale_and_seg()
    // bucket_index.update_seg(old_seg, new_segs)
    // ~old_seg()
    // assumption: error bound is the sbucket_size
    // NOTE: the SBUCKET_SIZE of new segments is the same as the old one
    //bool Segment<T, V, SBUCKET_SIZE>::scale_and_segmentation(double fill_ratio, std::vector<KeyValue<T,uintptr_t>> &new_segs);
    bool scale_and_segmentation(double fill_ratio, std::vector<KeyValue<T,uintptr_t>> &new_segs);

private:
    LinearModel<T> model_;

    // void train_model(); // based on the pivot of each bucket

    // TBD: do we explicitly store x_sum, y_sum, xx_sum and xy_sum

    inline unsigned int predict_buck(T key) const { // get the predicted S-Bucket ID based on the model computing
        unsigned int buckID = (unsigned int)(model_.predict(key) / SBUCKET_SIZE);
        buckID = std::min(buckID, (unsigned int)std::max(0,(int)(num_bucket_-1))); // ensure num_bucket>0
        return buckID;
    }

    inline unsigned int locate_buck(T key) const {
        // prediction may be incorrect, this function is to find the exact bucket whose range covers the key based on prediction
        // Step1: call predict_buck to get an intial position
        // Step2: search neighbors to find the exact match (linear search)
        unsigned int buckID = predict_buck(key); // ensure buckID is valid s


        if(sbucket_list_[buckID].get_pivot() <= key){ // search forwards
            while(buckID+1<num_bucket_){
                if(sbucket_list_[buckID+1].get_pivot() > key){
                    break;
                }
                buckID++;
            }
        }
        else{ // search backwards
            while(buckID>0){
                if(sbucket_list_[buckID-1].get_pivot() <= key){
                    buckID--;
                    break;
                }
                buckID--;
            }
        }

        return buckID;
    }

    bool bucket_rebalance(unsigned int buckID);
};



template<typename T, typename V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::scale_and_segmentation(double fill_ratio, std::vector<KeyValue<T,uintptr_t>> &new_segs){
    //std::vector<KeyValue<T,uintptr_t>> ret;
    //std::vector<Segment*> ret;
    //ret.clear();

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

template<typename T, typename V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::bucket_rebalance(unsigned int buckID) { // re-balance between adjcent bucket
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
                sbucket_list_[buckID+1].insert(sbucket_list_[buckID].at(i), false);
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
                sbucket_list_[buckID-1].insert(sbucket_list_[buckID].at(i),false);
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


template<typename T, typename V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::lookup(T key, V &value) const { // pass return value by argument; return a boolean to decide success or not
    assert(num_bucket_>0);
    unsigned int buckID = locate_buck(key);

    bool success = sbucket_list_[buckID].lb_lookup(key, value);

    // TODO: predict -> search within bucket -> locate -> search (put a flag) (deferred)
    return success;
}

template<typename T, typename V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::insert(KeyValue<T, V> &kv) {

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
    // bool ret = sbucket_list_[buckID].insert(kv, false);
    // // target key should be within the pivots

    // if buckID == 0, key may be smaller than pivot
    // then we need to update the pivot
     
    bool ret = sbucket_list_[buckID].insert(kv, true);

    return ret;
}

// return the first element that is not less than key
template<typename T, typename V, size_t SBUCKET_SIZE>
typename Segment<T, V, SBUCKET_SIZE>::const_iterator Segment<T, V, SBUCKET_SIZE>::lower_bound(T key){
    assert(num_bucket_>0);
    unsigned int buckID = locate_buck(key);

    return const_iterator(this, buckID, key, true);
}

// return the first element that is greater than key
template<typename T, typename V, size_t SBUCKET_SIZE>
typename Segment<T, V, SBUCKET_SIZE>::const_iterator Segment<T, V, SBUCKET_SIZE>::upper_bound(T key){
    assert(num_bucket_>0);
    unsigned int buckID = locate_buck(key);

    return const_iterator(this, buckID, key, false);
}

/*
// template<class T, class V, size_t SBUCKET_SIZE>
// void Segment<T, V, SBUCKET_SIZE>::train_model() {
//     // input: pivot key of each bucket
//     // output: model's slope and intercept

//     long double x_sum_ = 0;
//     long double y_sum_ = 0;
//     long double xx_sum_ = 0;
//     long double xy_sum_ = 0;
//     for(size_t i = 0;i<num_bucket_;i++){
//         T key = sbucket_list_[i].pivot_;
//         x_sum_ += static_cast<long double>(key);
//         y_sum_ += static_cast<long double>(i);
//         xx_sum_ += static_cast<long double>(key) * key;
//         xy_sum_ += static_cast<long double>(key) * i;
//     }


//     if (num_bucket_ <= 1) {
//         model_->a_ = 0;
//         model_->b_ = static_cast<double>(y_sum_);
//         return;
//     }

//     if (static_cast<long double>(num_bucket_) * xx_sum_ - x_sum_ * x_sum_ == 0) {
//         // all values in a bucket have the same key.
//         model_->a_ = 0;
//         model_->b_ = static_cast<double>(y_sum_) / num_bucket_;
//         return;
//     }

//     auto slope = static_cast<double>(
//         (static_cast<long double>(num_bucket_) * xy_sum_ - x_sum_ * y_sum_) /
//         (static_cast<long double>(num_bucket_) * xx_sum_ - x_sum_ * x_sum_));
//     auto intercept = static_cast<double>(
//         (y_sum_ - static_cast<long double>(slope) * x_sum_) / num_bucket_);
//     model_->a_ = slope;
//     model_->b_ = intercept;

//     // If floating point precision errors, fit spline
//     if (model_->a_ <= 0) {
//         model_->a_ = (num_bucket_ - 1) / (sbucket_list_[num_bucket_-1].pivot_ - sbucket_list_[0].pivot_);
//         model_->b_ = -static_cast<double>(sbucket_list_[0].pivot_) * model_->a_;
//     }

// }
*/

/*
//TODO: unit test
template<typename T, typename V, size_t SBUCKET_SIZE>
class Segment<T, V, SBUCKET_SIZE>::UnsortedIterator {
public:
    using SegmentType = Segment<T, V, SBUCKET_SIZE>;

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

    KeyValue<T, V> operator*() const {
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

template<typename T, typename V, size_t SBUCKET_SIZE>
class Segment<T, V, SBUCKET_SIZE>::const_iterator {
public:
    using SegmentType = Segment<T, V, SBUCKET_SIZE>;

    /*
    // explicit const_iterator(SegmentType *segment) : segment_(segment) {
    //     assert(segment_ != nullptr);
    //     cur_buckID = 0;
    //     cur_index = 0;
    //     sorted_list.clear();

    //     // find the first valid bucket
    //     while(!reach_to_end()){
    //         if(segment_->sbucket_list_[cur_buckID].num_keys() == 0){
    //             cur_buckID++;
    //         }
    //         else{
    //             segment_->sbucket_list_[cur_buckID].get_valid_kvs(sorted_list); 
    //             sort(sorted_list.begin(), sorted_list.end());
    //             break;
    //         }
    //     }       
    // }
    */

    // num of bucket to indicate the end
    const_iterator(SegmentType *segment, int pos) : segment_(segment) {
        assert(pos >= 0 && pos <= segment_->size());
        cur_buckID = 0;
        cur_index = 0;
        if(pos == segment_->size()){
            cur_buckID = segment_->num_bucket_;
            return;
        }

        // locate the bucket
        while(pos >= segment_->sbucket_list_[cur_buckID].num_keys()){
            cur_buckID++;
            pos -= segment_->sbucket_list_[cur_buckID].num_keys();
        }

        // inside the bucket, locate the index
        segment_->sbucket_list_[cur_buckID].get_valid_kvs(sorted_list); 
        sort(sorted_list.begin(), sorted_list.end());
        cur_index = pos;
    }

    // find the first key >= key or the first key > key
    const_iterator(SegmentType *segment, int buckID, T key, bool allow_equal) : segment_(segment) {
        assert(buckID >= 0 && buckID <= segment_->num_bucket_);
        cur_buckID = buckID;
        cur_index = 0;

        // locate the bucket
        while(segment_->sbucket_list_[cur_buckID].num_keys() == 0){
            cur_buckID++;
            if(cur_buckID == segment_->num_bucket_) return;
        }
        
        segment_->sbucket_list_[cur_buckID].get_valid_kvs(sorted_list);
        sort(sorted_list.begin(), sorted_list.end());
        KeyValue<T, V> kv;
        kv.key_ = key;
        if(allow_equal){ // make sure no matter what the value is, it will be the first key >= key
            kv.value_ = 0;
            cur_index = std::lower_bound(sorted_list.begin(), sorted_list.end(), kv) - sorted_list.begin();
        }
        else{
            kv.value_ = std::numeric_limits<V>::max(); // make sure no matter what the value is, it will be the first key > key
            cur_index = std::upper_bound(sorted_list.begin(), sorted_list.end(), kv) - sorted_list.begin();
        }
        if(cur_index == sorted_list.size()) {
            assert(cur_index > 0);
            cur_index--;
            find_next();
        }
    }

    void operator++(int) {
        find_next();
    }

    // prefix ++it
    const_iterator &operator++() {
        find_next();
        return *this;
    }

    // *it
    const KeyValue<T, V> operator*() const {
        return sorted_list[cur_index];
    }

    // it->
    const KeyValue<T, V>* operator->() const {
        return &(sorted_list[cur_index]);
    }

    bool operator==(const const_iterator& rhs) const {
        return segment_ == rhs.segment_ && cur_buckID == rhs.cur_buckID && cur_index == rhs.cur_index;
    }

    bool operator!=(const const_iterator& rhs) const { return !(*this == rhs); };

private:
    SegmentType *segment_;
    //int cur_pos_ = 0;  // current position in the sbucket list, 
    size_t cur_buckID = 0;
    size_t cur_index = 0;

    std::vector<KeyValue<T, V>> sorted_list; // initialized when cbegin() is called or move to another bucket

    // find the next entry in the sorted list (Can cross boundary of bucket)
    inline void find_next() {
        if (reach_to_end()) return;
        cur_index++;
        if(cur_index == sorted_list.size()){
            cur_buckID++;
            sorted_list.clear();
            cur_index = 0;
            
            // find the next valid bucket
            while(!reach_to_end()){
                if(segment_->sbucket_list_[cur_buckID].num_keys() == 0){
                    cur_buckID++;
                }
                else{
                    segment_->sbucket_list_[cur_buckID].get_valid_kvs(sorted_list); 
                    sort(sorted_list.begin(), sorted_list.end());
                    break;
                }
            }
        }
    }

    bool reach_to_end(){
        return (cur_buckID == segment_->num_bucket_);
    }
};


} // end namespace buckindex
