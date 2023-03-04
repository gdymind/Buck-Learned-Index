
#pragma once

#include<cmath>
#include<cstdlib>
#include<algorithm>

#include "bucket.h"
#include "linear_model.h"

namespace buckindex {

template<class T, class V, size_t SBUCKET_SIZE>
class Segment {
public:
    bool is_leaf_; // true -> segment; false -> segment group
    size_t num_bucket_; // total num of buckets

    // TBD: flag to determine whether it has rebalanced 

    Segment* parent_; // the parent Segment node, which enables bottom-up tranversal
    // T pivot_; // smallest element // TODO: No need for now
    // T base; // key compression

    Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>* sbucket_list_; // a list of S-Buckets 
    
    // constructors
    Segment(){
        is_leaf_ = false;
        num_bucket_ = 0; // indicating it is empty now
        parent_ = nullptr;
        sbucket_list_ = nullptr;
    }

    Segment(Segment &seg){ // do deep copy
        is_leaf_ = seg.is_leaf_;
        num_bucket_ = seg.num_bucket_;
        parent_ = seg.parent_;
        sbucket_list_ = new Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>[num_bucket_];
        for(size_t i = 0; i<num_bucket_;i++){
            sbucket_list_[i].copy(seg.sbucket_list_[i]); // TODO: add function in bucket.h, do deep copy
        }
    }

    Segment(size_t num, Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>* list, bool leaf = false, Segment* parent = nullptr){
        num_bucket_ = num;
        sbucket_list_ = new Bucket<KeyValueList<T, V,  SBUCKET_SIZE>, T, V, SBUCKET_SIZE>[num];
        is_leaf_ = leaf;
        parent = nullptr;
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

    V lookup(T key); //return the child 
    
    // insert an entry to the target S-Bucket; 
    // If the target S-Bucket is full, reblance the bucket with its right neighbor; 
    // If bucket_rebalance does not work, insert() return false
    bool insert(KeyValue<T, V> &kvptr); 


    
private:
    LinearModel<T> model_;

    void train_model(); // based on the pivot of each bucket

    // TBD: do we explicitly store x_sum, y_sum, xx_sum and xy_sum

    inline unsigned int predict_buck(T key) { // get the predicted S-Bucket ID based on the model computing 
        unsigned int buckID = (unsigned int)(model_.predict(key) / SBUCKET_SIZE + 0.5); //TODO: if SBUCKET_SIZE is the power of 2, change division to right shift
        buckID = std::min(buckID, (unsigned int)std::max(0,(int)(num_bucket_-1))); // ensure num_bucket>0
        return buckID;
    }

    inline unsigned int locate_buck(T key) { // prediction may be incorrect, this function is to find the exact bucket whose range covers the key based on prediction
        // Step1: call predict_buck to get an intial position
        // Step2: search neighbors to find the exact match (linear search)
        unsigned int buckID = predict_buck(key); // ensure buckID is valid s


        if(sbucket_list_[buckID].pivot <= key){ // search forwards
            while(buckID+1<num_bucket_){
                if(sbucket_list_[buckID+1].pivot > key){
                    break;
                }
                buckID++;
            }
        }
        else{ // search backwards
            while(buckID>0){ 
                if(sbucket_list_[buckID-1].pivot <= key){
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


template<class T, class V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::bucket_rebalance(unsigned int buckID) { // re-balance between adjcent bucket
    // Case 1: migrate forwards

    // Case 2: migrate backwards

    // if two directions are possible, migrate to the bucket with fewer element

    size_t src_buck_num = sbucket_list_[buckID].get_num();
    size_t des_buck_num = 0;

    bool migrate_forwards = true;
    if(buckID == num_bucket_-1 || (buckID != 0 && sbucket_list_[buckID+1].get_num() > sbucket_list_[buckID-1].get_num())){ // TODO: add function in bucket // get the valid num of entries
        migrate_forwards = false;
    }

    if(migrate_forwards){
        assert(buckID + 1 < num_bucket_);
        des_buck_num = sbucket_list_[buckID+1].get_num();
        if(sbucket_list_[buckID+1].is_full()){return false;}
        size_t median = (src_buck_num + des_buck_num)/2; // Be careful, current bucket can have one less element than the next one
        T new_pivot = sbucket_list_[buckID].find_kth_key(median); // TODO: add function in bucket // find the key pos index if it is sorted inside the bucket // input ranges from 0 ~ n-1

        // for concurrency, first insert new entries, then update pivot, then remove old entries
        for(size_t i = 0;i<src_buck_num;i++){
            if(sbucket_list_[buckID].keys_[i]>=new_pivot){
                sbucket_list_[buckID+1].insert(sbucket_list_[buckID].read_KV(i));
            }
        }
        sbucket_list_[buckID+1].pivot = new_pivot; // TODO: delete line 107 in bucket.h // pivot is not changed in insert()

        for(size_t i = 0; i<SBUCKET_SIZE;i++){
            if(!sbucket_list_[buckID].is_valid(i)){ // TODO: add function // check if it is invalid
                continue;
            }
            if(sbucket_list_[buckID].keys_[i]>=new_pivot){
                sbucket_list_[buckID].set_invalid(i);// TODO: add function in bucket // need to mark it as invalid // delete()
            }
        }
    }
    else{
        assert(buckID - 1 >= 0);
        des_buck_num = sbucket_list_[buckID-1].get_num();
        if(sbucket_list_[buckID-1].is_full()){return false;}
        
        size_t median = (src_buck_num + des_buck_num)/2; // Be careful, current bucket can have one less element than the next one
        size_t num_migration = src_buck_num - median;

        T new_pivot = sbucket_list_[buckID].find_kth_key(num_migration); // TODO: add function in bucket

        // for concurrency, first insert new entries, then update pivot, then remove old entries
        for(size_t i = 0;i<sbucket_list_[buckID].getnum();i++){
            if(sbucket_list_[buckID].keys_[i]<new_pivot){
                sbucket_list_[buckID-1].insert(sbucket_list_[buckID].read_KV(i));
            }
        }
        sbucket_list_[buckID].pivot = new_pivot;
        for(size_t i = 0; i<SBUCKET_SIZE;i++){
            if(!sbucket_list_[buckID].is_valid(i)){ // TODO: add function // check if it is invalid
                continue;
            }
            if(sbucket_list_[buckID].keys_[i]<new_pivot){
                sbucket_list_[buckID].set_invalid(i);// TODO: add function in bucket // need to mark it as invalid
            }
        }

    }
    assert(!sbucket_list_[buckID].is_full());
    return true;
}

template<class T, class V, size_t SBUCKET_SIZE>
V Segment<T, V, SBUCKET_SIZE>::lookup(T key) { // pass return value by argument; return a boolean to decide success or not
    unsigned int buckID = locate_buck(key); 
    V ret = sbucket_list_[buckID].lower_bound_lookup(key); // TODO: lower bound look up

    // TODO: predict -> search within bucket -> locate -> search (put a flag) (deferred)
    return ret;
}

template<class T, class V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::insert(KeyValue<T, V> &kv) {
    unsigned int buckID = locate_buck(kv.key_);

    if(sbucket_list_[buckID].is_full()){ // TODO: add function in bucket
        if(!bucket_rebalance(buckID)){
            return false;
        }
        buckID = locate_buck(kv.key_);
    }

    
    bool ret = sbucket_list_[buckID].insert(kv);

    return ret;
}

template<class T, class V, size_t SBUCKET_SIZE>
void Segment<T, V, SBUCKET_SIZE>::train_model() {
    // input: pivot key of each bucket
    // output: model's slope and intercept    

    long double x_sum_ = 0;
    long double y_sum_ = 0;
    long double xx_sum_ = 0;
    long double xy_sum_ = 0;
    for(size_t i = 0;i<num_bucket_;i++){
        T key = sbucket_list_[i].pivot;
        x_sum_ += static_cast<long double>(key);
        y_sum_ += static_cast<long double>(i);
        xx_sum_ += static_cast<long double>(key) * key;
        xy_sum_ += static_cast<long double>(key) * i;
    }
    
    if (num_bucket_ <= 1) {
        model_->a_ = 0;
        model_->b_ = static_cast<double>(y_sum_);
        return;
    }

    if (static_cast<long double>(num_bucket_) * xx_sum_ - x_sum_ * x_sum_ == 0) {
        // all values in a bucket have the same key.
        model_->a_ = 0;
        model_->b_ = static_cast<double>(y_sum_) / num_bucket_;
        return;
    }

    auto slope = static_cast<double>(
        (static_cast<long double>(num_bucket_) * xy_sum_ - x_sum_ * y_sum_) /
        (static_cast<long double>(num_bucket_) * xx_sum_ - x_sum_ * x_sum_));
    auto intercept = static_cast<double>(
        (y_sum_ - static_cast<long double>(slope) * x_sum_) / num_bucket_);
    model_->a_ = slope;
    model_->b_ = intercept;

    // If floating point precision errors, fit spline
    if (model_->a_ <= 0) {
        model_->a_ = (num_bucket_ - 1) / (sbucket_list_[num_bucket_-1].pivot - sbucket_list_[0].pivot);
        model_->b_ = -static_cast<double>(sbucket_list_[0].pivot) * model_->a_;
    }

}

} // end namespace buckindex
