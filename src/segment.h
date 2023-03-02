
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

    Segment* parent_ = nullptr; // the parent Segment node, which enables bottom-up tranversal
    T pivot_; // smallest element
    // T base; // key compression

    Bucket<class KeyValueList<T,V,SBUCKET_SIZE>, T, V, SBUCKET_SIZE>* sbucket_list_; // a list of S-Buckets


    V lookup(T key); //return the child pointer
    bool insert(KeyValue<T, V> kvptr); // insert an entry to the target S-Bucket; If the target S-Bucket is full, reblance the bucket with its right neighbor; If bucket_rebalance does not work, insert() return false

    
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
        unsigned int buckID = predict_buck(key); // ensure buckID is valid

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

    bool bucket_rebalance(unsigned int buckID0);

};


template<class T, class V, size_t SBUCKET_SIZE>
bool Segment<T, V, SBUCKET_SIZE>::bucket_rebalance(unsigned int buckID0) {
    unsigned int buckID1 = buckID0 +1;
    
    //TODO
    return true;
}

template<class T, class V, size_t SBUCKET_SIZE, T MAX_KEY>
V Segment<T, V, SBUCKET_SIZE, MAX_KEY>::lookup(T key) {
    unsigned int buckID = locate_buck(key);
    V ret = sbucket_list_[buckID].lookup(key);
    return ret;
}

template<class T, class V, size_t SBUCKET_SIZE, T MAX_KEY>
bool Segment<T, V, SBUCKET_SIZE, MAX_KEY>::insert(KeyValue<T, V> kvptr) {
    unsigned int buckID = locate_buck(kvptr.key_);

    if(sbucket_list_[buckID].is_full()){ // TODO: add function in bucket
        if(!bucket_rebalance(buckID)){
            return false;
        }
    }

    buckID = locate_buck(kvptr.key_);
    bool ret = sbucket_list_[buckID].insert(kvptr);

    return ret;
}

template<class T, class V, size_t SBUCKET_SIZE, T MAX_KEY>
void Segment<T, V, SBUCKET_SIZE, MAX_KEY>::train_model() {
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
