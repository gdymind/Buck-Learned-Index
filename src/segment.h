
#pragma once

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"
#include "linear_model.h"

namespace buckindex {

template<typename T, typename V, size_t SBUCKET_SIZE>
class Segment {
public:
    //bool is_leaf_; // true -> segment; false -> segment group 
    //Segment* parent_; // the parent Segment node, which enables bottom-up tranversal
    // T base; // key compression
    // TBD: flag to determine whether it has rebalanced 


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
    Segment(size_t num_kv, double fill_ratio, LinearModel<T> &model, 
    typename std::vector<KeyValue<T, V>>::iterator it, 
    typename std::vector<KeyValue<T, V>>::iterator end)
    :model_(model){
        assert(fill_ratio>=0 && fill_ratio<=1);
        size_t num_slot = ceil(num_kv / fill_ratio);
        num_bucket_ = ceil((double)num_slot / SBUCKET_SIZE);
        sbucket_list_ = new Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>[num_bucket_];
        model_.expand(1/fill_ratio);
        //model_.dump();

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
            
            while(buckID<num_bucket_ && sbucket_list_[buckID].num_keys()==SBUCKET_SIZE){
                buckID++; // search forwards until find a bucket with empty slot
            }
            
            // the new remaining_slots if the key is inserted here
            remaining_slots = SBUCKET_SIZE * (num_bucket_ - buckID) - sbucket_list_[buckID].num_keys();
            if(remaining_keys > remaining_slots){ // refuse to insert in this place // and find the nearest bucket backwards so that it can be put in
                buckID = num_bucket_ - 1 - (remaining_keys-1)/SBUCKET_SIZE;
                remaining_slots = SBUCKET_SIZE * (num_bucket_ - buckID) - sbucket_list_[buckID].num_keys();
                // update the remaining_slot if insert in the new buckID
            }
            // else: accept to insert in this bucket
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

    // TODO: iterator?

    // TODO: a non-pivoting version (deferred)

    bool lookup(T key, V &value) const; //return the child pointer; return nullptr if not exist
    
    // insert an entry to the target S-Bucket; 
    // If the target S-Bucket is full, reblance the bucket with its right neighbor; 
    // If bucket_rebalance does not work, insert() return false
    bool insert(KeyValue<T, V> &kvptr);
    
private:
    LinearModel<T> model_;

    // void train_model(); // based on the pivot of each bucket

    // TBD: do we explicitly store x_sum, y_sum, xx_sum and xy_sum

    inline unsigned int predict_buck(T key) const { // get the predicted S-Bucket ID based on the model computing 
        unsigned int buckID = (unsigned int)(model_.predict(key) / SBUCKET_SIZE + 0.5);
        buckID = std::min(buckID, (unsigned int)std::max(0,(int)(num_bucket_-1))); // ensure num_bucket>0
        return buckID;
    }

    inline unsigned int locate_buck(T key) const { 
        // prediction may be incorrect, this function is to find the exact bucket whose range covers the key based on prediction
        // Step1: call predict_buck to get an intial position
        // Step2: search neighbors to find the exact match (linear search)
        unsigned int buckID = predict_buck(key); // ensure buckID is valid s


        if(sbucket_list_[buckID].pivot_ <= key){ // search forwards
            while(buckID+1<num_bucket_){
                if(sbucket_list_[buckID+1].pivot_ > key){
                    break;
                }
                buckID++;
            }
        }
        else{ // search backwards
            while(buckID>0){ 
                if(sbucket_list_[buckID-1].pivot_ <= key){
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
bool Segment<T, V, SBUCKET_SIZE>::bucket_rebalance(unsigned int buckID) { // re-balance between adjcent bucket
    // Case 1: migrate forwards

    // Case 2: migrate backwards

    // if two directions are possible, migrate to the bucket with fewer element

    size_t src_buck_num = sbucket_list_[buckID].num_keys();
    size_t des_buck_num = 0;

    bool migrate_forwards = true;
    if(buckID == num_bucket_-1 || (buckID != 0 && sbucket_list_[buckID+1].num_keys() > sbucket_list_[buckID-1].num_keys())){
        migrate_forwards = false;
    }

    if(migrate_forwards){
        assert(buckID + 1 < num_bucket_);
        des_buck_num = sbucket_list_[buckID+1].num_keys();
        if(sbucket_list_[buckID+1].num_keys() == SBUCKET_SIZE){return false;}
        size_t median = (src_buck_num + des_buck_num)/2; // Be careful, current bucket can have one less element than the next one
        T new_pivot = sbucket_list_[buckID].find_kth_smallest(median);

        // for concurrency, first insert new entries, then update pivot_, then remove old entries
        for(size_t i = 0;i<src_buck_num;i++){
            if(sbucket_list_[buckID].at(i).get_key()>=new_pivot){
                sbucket_list_[buckID+1].insert(sbucket_list_[buckID].at(i), false);
            }
        }
        sbucket_list_[buckID+1].pivot_ = new_pivot;

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
        if(sbucket_list_[buckID-1].num_keys() == SBUCKET_SIZE){return false;}
        
        size_t median = (src_buck_num + des_buck_num)/2; // Be careful, current bucket can have one less element than the next one
        size_t num_migration = src_buck_num - median;

        T new_pivot = sbucket_list_[buckID].find_kth_smallest(num_migration);

        // for concurrency, first insert new entries, then update pivot_, then remove old entries
        for(size_t i = 0;i<sbucket_list_[buckID].getnum();i++){
            if(sbucket_list_[buckID].at(i).get_key()<new_pivot){
                sbucket_list_[buckID-1].insert(sbucket_list_[buckID].at(i));
            }
        }
        sbucket_list_[buckID].pivot_ = new_pivot;
        for(size_t i = 0; i<SBUCKET_SIZE;i++){
            if(!sbucket_list_[buckID].valid(i)){
                continue;
            }
            if(sbucket_list_[buckID].at(i).get_key()<new_pivot){
                sbucket_list_[buckID].invalidate(i);
            }
        }

    }
    assert(!sbucket_list_[buckID].num_keys() == SBUCKET_SIZE);
    return true;
}


template<typename T, typename V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::lookup(T key, V &value) const { // pass return value by argument; return a boolean to decide success or not

    unsigned int buckID = locate_buck(key); 

    bool sucess = sbucket_list_[buckID].lb_lookup(key, value);

    // TODO: predict -> search within bucket -> locate -> search (put a flag) (deferred)
    return sucess;
}

template<typename T, typename V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::insert(KeyValue<T, V> &kv) {
    unsigned int buckID = locate_buck(kv.key_);

    if(sbucket_list_[buckID].num_keys() == SBUCKET_SIZE){
        if(!bucket_rebalance(buckID)){
            return false;
        }
        buckID = locate_buck(kv.key_);
    }

    
    bool ret = sbucket_list_[buckID].insert(kv, true);

    return ret;
}


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

} // end namespace buckindex
