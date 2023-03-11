#pragma once

#include "bucket.h"
#include "segment.h"
#include "segmentation.h"
#include "util.h"

namespace buckindex {
/**
 * Index configurations
 */
#ifdef UNITTEST
// Parameters used by the unit test
#define MAX_DATA_BUCKET_SIZE 2
#define MAX_SEGMENT_BUCKET_SIZE 2
#define FILLED_RATIO 0.5
#else
#define MAX_DATA_BUCKET_SIZE 128
#define MAX_SEGMENT_BUCKET_SIZE 8
#define FILLED_RATIO 0.5
#endif


template<typename KeyType, typename ValueType>
class BuckIndex {
public:
    //List of template aliasing
    using DataBucketType = Bucket<KeyListValueList<KeyType, ValueType, MAX_DATA_BUCKET_SIZE>,
                                  KeyType, ValueType, MAX_DATA_BUCKET_SIZE>;
    using SegBucketType = Bucket<KeyValueList<KeyType, ValueType, MAX_SEGMENT_BUCKET_SIZE>,
                                  KeyType, ValueType, MAX_SEGMENT_BUCKET_SIZE>;
    using SegmentType = Segment<KeyType, ValueType,
                                MAX_SEGMENT_BUCKET_SIZE>;
    using KeyValueType = KeyValue<KeyType, ValueType>;
    using KeyValuePtrType = KeyValue<KeyType, uintptr_t>;

    BuckIndex() {
        root_ = NULL;
        num_levels_ = 0;
    }

    /**
     * Constructor which can specify the bucket sizes
     */
    BuckIndex(uint64_t dbucket_size, uint64_t sbucket_size) {
        root_ = NULL;
        //TODO: Need to figure out how to pass the parameter in the template aliasing
        exit(1);
    }
    ~BuckIndex() {
        //TODO
    }

    /**
     * Lookup function
     * @param key: lookup key
     * @param value: corresponding value to be returned
     * @return true if the key is found, else false
     */
    bool lookup(KeyType key, ValueType &value) {
        if (!root_) return false;

        uint64_t layer_idx = num_levels_ - 1;
        uintptr_t seg_ptr = (uintptr_t)root_;
        bool result = false;
        value = 0;
        while (layer_idx > 0) {
            SegmentType* segment = (SegmentType*)seg_ptr;
            result = segment->lookup(key, seg_ptr);
            if (!seg_ptr) {
                std::cerr << " failed to perform segment lookup for key: " << key << std::endl;
                return false;
            }
            layer_idx--;
        }
        DataBucketType* d_bucket = (DataBucketType *)seg_ptr;
        result = d_bucket->lookup(key, value);
        return result;
    }

    /**
    * Insert function
    * @param kv: the Key-Value pair to be inserted
    * @return true if kv in inserted, false else
    */
    bool insert(KeyValueType& kv) {
        // if (!root_) {
        //     root_ = new DataBucketType();
        //     num_levels_ = 1;
        // }

        // uint64_t layer_idx = num_levels_ - 1;
        // uintptr_t seg_ptr = (uintptr_t)root_;
        // bool result = true;
        // while (layer_idx > 0) {
        //     SegmentType* segment = (SegmentType*)seg_ptr;
        //     result = segment->lookup(kv.key_, seg_ptr);
        //     if (!seg_ptr) {
        //         std::cerr << " failed to perform segment insert for KV: (" << kv.key_ 
        //                   << ", " << kv.value_ << ")" << std::endl;
        //         return false;
        //     }

        //     if (!result) { // insert key is smaller than existing pivots
        //                    // need to update the smallest pivot of the current segment
        //                    // also need to update the entry whose key == pivot:
        //                    //      update the key to be the new pivot, but keep the value(a pointer) unchanged

        //         SegBucketType *first_bucket; // the first bucket has the smallest pivot
        //         first_bucket = segment->get_bucket(0);
        //         ValueType pivot = first_bucket->get_pivot();

        //         int pos = first_bucket->get_pos(pivot); // get the position of the pivot entry
        //         assert(pos != -1);

        //         ValueType value; // get the child pointer of the pivot entry
        //         assert(first_bucket->lookup(pivot, value) == true);

        //         // note the order of the following three opreations to support concurency
        //         first_bucket->insert(KeyValueType(kv.key_, value)); 
        //         first_bucket->set_pivot(kv.key_);
        //         first_bucket->invalidate(pos);

        //         seg_ptr = value;
        //     }

        //     layer_idx--; 
        // }

        
        // DataBucketType* d_bucket = (DataBucketType *)seg_ptr;
        // if (!d_bucket->insert(kv)) {
        //     //TODO: current bucket is full, call bucket_rebalance
        //     //TODO: if still fails after bucket_rebalance, call adjust_segment
        //     return false;
        // }
        return true;
    }
    /**
     * Bulk load the user key value onto the learned index
     * @param kvs: list of user key value to be loaded onto the learned index
     */
    void bulk_load(vector<KeyValueType> &kvs) {
        vector<KeyValuePtrType> kvptr_array[2];
        uint64_t ping = 0, pong = 1;
        num_levels_ = 0;
        run_data_layer_segmentation(kvs,
                                    kvptr_array[ping]);
        if (kvptr_array[ping].size() > 0) {
            num_levels_++;
            while (kvptr_array[ping].size() > 1) {
                run_model_layer_segmentation(kvptr_array[ping],
                                             kvptr_array[pong]);
                ping = (ping +1) % 2;
                pong = (pong +1) % 2;
                kvptr_array[pong].clear();
                level_stats_[num_levels_] = kvptr_array[ping].size();
                num_levels_++;
            }
            /*
             * Build the root
             */
            KeyValuePtrType& kv_ptr = kvptr_array[ping][0];
            root_ = (void *)kv_ptr.value_;
            dump();
        }
    }
    /**
     * Helper function to dump the index structure
     */
    void dump() {
        std::cout << "Index Structure" << std::endl;
        std::cout << "  Number of Layers: " << num_levels_ << std::endl;
        for (auto i = 0; i < num_levels_; i++) {
            std::cout << "    Layer " << i << " size: " << level_stats_[i] << std::endl;
        }
    }
private:
    /**
     * Helper function to perform bucketizaion on the data layer
     *
     * @param in_kv_array: list of key values from application
     * @return out_kv_array: list of anchors for each bucket.
     */
    void run_data_layer_segmentation(vector<KeyValueType>& in_kv_array,
                                     vector<KeyValuePtrType>& out_kv_array) {
        vector<Cut<KeyType>> out_cuts;
        uint64_t initial_bucket_occupacy = MAX_DATA_BUCKET_SIZE * FILLED_RATIO;

        Segmentation<vector<KeyValueType>, KeyType>::compute_fixed_segmentation(in_kv_array,
                                                                                out_cuts,
                                                                                initial_bucket_occupacy);
        for(auto i = 0; i<out_cuts.size(); i++) {
            uint64_t start_idx = out_cuts[i].start_;
            uint64_t length = out_cuts[i].size_;
            DataBucketType* d_bucket = new DataBucketType();

            //store the bucket anchor for the higher layer
            out_kv_array.push_back(KeyValuePtrType(in_kv_array[start_idx].key_,
                                                   (uintptr_t)d_bucket));
            //load the keys to the data bucket
            for(auto j = start_idx; j < (start_idx+length); j++) {
                d_bucket->insert(in_kv_array[j]);
            }
            //segment->dump();
        }
        level_stats_[0] = out_cuts.size();
    }

    /**
     * Helper function to perform segmentation on the model layer
     *
     * @param in_kv_array: anchor array
     * @return out_kv_array: list of anchors for the sub-segments.
     */
    void run_model_layer_segmentation(vector<KeyValuePtrType>& in_kv_array,
                                      vector<KeyValuePtrType>& out_kv_array) {
        vector<Cut<KeyType>> out_cuts;
        uint64_t initial_sbucket_occupacy = MAX_SEGMENT_BUCKET_SIZE * FILLED_RATIO;
        Segmentation<vector<KeyValuePtrType>, KeyType>::compute_dynamic_segmentation(in_kv_array,
                                                                                     out_cuts,
                                                                                     initial_sbucket_occupacy);
        for(auto i = 0; i < out_cuts.size(); i++) {
            uint64_t start_idx = out_cuts[i].start_;
            uint64_t length = out_cuts[i].size_;
            LinearModel<KeyType> m(out_cuts[i].get_model());

            SegmentType* segment = new SegmentType(length, FILLED_RATIO, m,
                                                   in_kv_array.begin() + start_idx, in_kv_array.begin() + start_idx + length);
            out_kv_array.push_back(KeyValuePtrType(in_kv_array[start_idx].key_,
                                                   (uintptr_t)segment));
        }
    }

    bool adjust_segment(SegmentType *old_seg) { //scale, run segmentation, and retrain the old_seg, and possibly split into multiple new Segment

        return true;
    }
    
    //The root of the learned index. Root can be a segment or a bucket
    void* root_;
    //Learned index constants
    static const uint8_t max_levels_ = 16;
    //const double filled_ratio_ = 0.50;
    //Statistics
    uint8_t num_levels_;
    uint64_t num_data_buckets_; //TODO: update num_data_buckets_ during bulk_load and insert
    uint64_t level_stats_[max_levels_];

};

} // end namespace buckindex
