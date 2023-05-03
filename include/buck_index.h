#pragma once

#include "bucket.h"
#include "segment.h"
#include "segmentation.h"
#include "util.h"

/**
 * Index configurations
 */
#define DEFAULT_FILLED_RATIO 0.6

namespace buckindex {


template<typename KeyType, typename ValueType, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
class BuckIndex {
public:
    //List of template aliasing
    using DataBucketType = Bucket<KeyListValueList<KeyType, ValueType, DATA_BUCKET_SIZE>,
                                  KeyType, ValueType, DATA_BUCKET_SIZE>;
    using SegBucketType = Bucket<KeyValueList<KeyType, ValueType, SEGMENT_BUCKET_SIZE>,
                                  KeyType, ValueType, SEGMENT_BUCKET_SIZE>;
    using SegmentType = Segment<KeyType, SEGMENT_BUCKET_SIZE>;
    using KeyValueType = KeyValue<KeyType, ValueType>;
    using KeyValuePtrType = KeyValue<KeyType, uintptr_t>;

    BuckIndex(double initial_filled_ratio = DEFAULT_FILLED_RATIO, bool use_linear_regression = true, bool use_SIMD = true): 
              use_linear_regression_(use_linear_regression), initial_filled_ratio_(initial_filled_ratio), use_SIMD_(use_SIMD) {
        root_ = NULL;
        SegmentType::use_linear_regression_ = use_linear_regression_;
        Segmentation<vector<KeyValueType>, KeyType>::use_linear_regression_ = use_linear_regression_;
        DataBucketType::use_SIMD_ = use_SIMD_;
        num_levels_ = 0;
    }
    ~BuckIndex() {

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
        KeyValuePtrType kv_ptr;
        while (layer_idx > 0) {
            SegmentType* segment = (SegmentType*)seg_ptr;
            result = segment->lookup(key, kv_ptr);
            seg_ptr = kv_ptr.value_;
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
     * Scan function
     * @param start_key: scan from the first key that is >= start_key
     * @param scan_num: the number of key-value pairs to be scanned
     * @param kvs: the scanned key-value pairs
     * @return the number of key-value pairs scanned(<= scan_num)
    */
    size_t scan(KeyType start_key, size_t num_to_scan, std::pair<KeyType, ValueType> *kvs) {
        if (!root_) return 0;

        int num_scanned = 0;

        // traverse to leaf and record the path
        std::vector<KeyValuePtrType> path(num_levels_);//root-to-leaf path, including the data bucket
        bool success = lookup(start_key, path);

        // get the d-bucket iterator
        DataBucketType* d_bucket = (DataBucketType *)(path[num_levels_-1]).value_;;
        auto dbuck_iter = d_bucket->lower_bound(start_key);

        while (num_scanned < num_to_scan) {
            // scan keys in the d-bucket
            while(num_scanned < num_to_scan && dbuck_iter != d_bucket->end()) {
                KeyValueType kv = (*dbuck_iter);
                kvs[num_scanned] = std::make_pair(kv.key_, kv.value_);
                num_scanned++;
                dbuck_iter++;
            }

            // get the next d-bucket
            if (num_scanned < num_to_scan) {
                do {
                    if (!find_next_d_bucket(path)) return num_scanned;
                    d_bucket = (DataBucketType *)(path[num_levels_-1]).value_;;
                    dbuck_iter = d_bucket->begin();
                } while (dbuck_iter == d_bucket->end()); // empty d-bucket, visit the next one
            }
        }
        
        return num_scanned;
    }

    /**
    * Insert function
    * @param kv: the Key-Value pair to be inserted
    * @return true if kv in inserted, false else
    */
    bool insert(KeyValueType& kv) {
        if (root_ == nullptr) { 
            std::vector<KeyValueType> kvs;
            KeyValueType kv1(std::numeric_limits<KeyType>::min(), 0);
            kvs.push_back(kv1);
            kvs.push_back(kv);
            bulk_load(kvs);
            return true;
        }

        // traverse to the leaf D-Bucket, and record the path
        std::vector<KeyValuePtrType> path(num_levels_);//root-to-leaf path, including the  data bucket
        bool success = lookup(kv.key_, path);
        assert(success);

        DataBucketType* d_bucket = (DataBucketType *)(path[num_levels_-1].value_);
        success = d_bucket->insert(kv, true);

        // TODO: need to implement the GC
        std::vector<uintptr_t> GC_segs;

        // if fail to insert, split the bucket, and add new kvptr on parent segment
        if (!success) {
            std::vector<KeyValuePtrType> pivot_list[2]; // ping-pong list
            int ping = 0, pong = 1;

            // split d_bucket
            auto new_d_buckets = d_bucket->split_and_insert(kv);
            pivot_list[ping].push_back(new_d_buckets.first);
            pivot_list[ping].push_back(new_d_buckets.second);
            KeyValuePtrType old_pivot = path[num_levels_-1];

            // propagate the insertion to the parent segments
            assert(num_levels_ >= 2); // insert into leaf segment
            int cur_level = num_levels_ - 2; // leaf_segment level
            while(cur_level >= 0) {
                SegmentType* cur_segment = (SegmentType*)(path[cur_level].value_);
                
                bool is_segment = true;
                if (cur_level == num_levels_ - 2) is_segment = false;
                if (cur_segment->batch_update(old_pivot, pivot_list[ping], is_segment)) {
                    pivot_list[ping].clear();
                    success = true;
                    break;
                }

                pivot_list[pong].clear();
                success = cur_segment->segment_and_batch_update(initial_filled_ratio_, pivot_list[ping], pivot_list[pong]);
                old_pivot = path[cur_level];
                assert(success);

                GC_segs.push_back((uintptr_t)cur_segment);
                cur_level--;
                ping = 1 - ping;
                pong = 1 - pong;
            }

            // add one more level
            assert(pivot_list[ping].size() == 0 || cur_level == -1);
            if (pivot_list[ping].size() > 0) {
                LinearModel<KeyType> model;
                if (use_linear_regression_) {
                    std::vector<KeyType> keys;
                    for (auto kv_ptr : pivot_list[ping]) {
                        keys.push_back(kv_ptr.key_);
                    }
                    model = LinearModel<KeyType>::get_regression_model(keys);
                } else {
                    double start_key = pivot_list[ping].front().key_;
                    double end_key = pivot_list[ping].back().key_;
                    double slope = 0.0, offset = 0.0;
                    if (pivot_list[ping].size() > 1) {
                        slope = (end_key - start_key) /(pivot_list[ping].size()- 1);
                        offset = -slope * start_key;
                    }
                    model = LinearModel<KeyType>(slope, offset);
                }
                root_ = new SegmentType(pivot_list[ping].size(), initial_filled_ratio_, model, 
                                    pivot_list[ping].begin(), pivot_list[ping].end());
                num_levels_++;
            }
       
            num_data_buckets_++;

            // GC
            for (auto seg_ptr : GC_segs) { // TODO: support MRSW
                SegmentType* seg = (SegmentType*)seg_ptr;
                delete seg;
            }
        }

        return success;
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
        num_data_buckets_ = kvptr_array[ping].size();
        num_levels_++;

        assert(kvptr_array[ping].size() > 0);

        do { // at least one model layer
            run_model_layer_segmentation(kvptr_array[ping],
                                            kvptr_array[pong]);
            ping = (ping +1) % 2;
            pong = (pong +1) % 2;
            kvptr_array[pong].clear();
            level_stats_[num_levels_] = kvptr_array[ping].size();
            num_levels_++;
        } while (kvptr_array[ping].size() > 1);
        
        // Build the root
        KeyValuePtrType& kv_ptr = kvptr_array[ping][0];
        root_ = (void *)kv_ptr.value_;
        dump();

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

    /**
     * Helper function to get the number of levels in the index
     *
     * @return the number of levels in the index
     */
    uint64_t get_num_levels() {
        return num_levels_;
    }

    /**
     * Helper function to get the number of levels in the index
     *
     * @return the number of data buckets in the index
     */
    uint64_t get_num_data_buckets() {
        return num_data_buckets_;
    }
private:

    /**
     * Lookup function, traverse the index to the leaf D-Bucket, and record the path
     * @param key: lookup key
     * @param path: the path from root to the leaf D-Bucket
    */
    bool lookup(KeyType key, std::vector<KeyValuePtrType> &path) {
        // traverse the index to the leaf D-Bucket, and record the path
        bool success = true;
        path[0] = KeyValuePtrType(std::numeric_limits<KeyType>::min(), (uintptr_t)root_);
        for (int i = 1; i < num_levels_; i++) {
            SegmentType* segment = (SegmentType*)path[i-1].value_;
            success &= segment->lookup(key, path[i]);
            assert((void *)path[i].value_ != nullptr);
        }
        assert(success);
        return success;
    }

    /**
     * Helper function for scan() to find the next D-Bucket
     * @param path: the path from root to the leaf D-Bucket
     * @return true if the next D-Bucket is found, else false
    */
    bool find_next_d_bucket(std::vector<KeyValuePtrType> &path) {
        assert(path.size() == num_levels_);

        int cur_level = num_levels_ - 2; // leaf_segment level
        assert(cur_level >= 0);

        while(cur_level >= 0) {
            SegmentType* cur_segment = (SegmentType*)(path[cur_level].value_);
            auto seg_iter = cur_segment->lower_bound(path[cur_level+1].key_); // find the one after path[cur_level+1]
                                                                              // TODO optimization: store the seg_iter,
                                                                              // and use it as the start point for the next call
            if (seg_iter != cur_segment->cend() && (*seg_iter).key_ == path[cur_level+1].key_) {
                seg_iter++;
            }
            if (seg_iter != cur_segment->cend()) { // found the next entry
                path[cur_level+1] = *seg_iter; // update to the next entry

                // update the lower-level path
                int tranverse_down_level = cur_level + 1;
                while(tranverse_down_level < num_levels_ - 1) {
                    SegmentType* segment = (SegmentType*)path[tranverse_down_level].value_;
                    seg_iter = segment->cbegin();
                    path[tranverse_down_level+1] = *seg_iter;
                    tranverse_down_level++;
                }

                return true;
            } else { // not found, go to the upper level
                cur_level--;
            }
        }

        return false;
    }

    /**
     * Helper function to perform bucketizaion on the data layer
     *
     * @param in_kv_array: list of key values from application
     * @return out_kv_array: list of anchors for each bucket.
     */
    void run_data_layer_segmentation(vector<KeyValueType>& in_kv_array,
                                     vector<KeyValuePtrType>& out_kv_array) {
        vector<Cut<KeyType>> out_cuts;
        uint64_t initial_bucket_occupacy = DATA_BUCKET_SIZE * initial_filled_ratio_;

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
                d_bucket->insert(in_kv_array[j], true);
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
        vector<LinearModel<KeyType>> out_models;
        uint64_t initial_sbucket_occupacy = SEGMENT_BUCKET_SIZE * initial_filled_ratio_;
        Segmentation<vector<KeyValuePtrType>, KeyType>::compute_dynamic_segmentation(in_kv_array,
                                                                                     out_cuts, out_models,
                                                                                     initial_sbucket_occupacy);
        for(auto i = 0; i < out_cuts.size(); i++) {
            uint64_t start_idx = out_cuts[i].start_;
            uint64_t length = out_cuts[i].size_;

            SegmentType* segment = new SegmentType(length, initial_filled_ratio_, out_models[i],
                                                   in_kv_array.begin() + start_idx, in_kv_array.begin() + start_idx + length);
            out_kv_array.push_back(KeyValuePtrType(in_kv_array[start_idx].key_,
                                                   (uintptr_t)segment));
        }
    }

    //The root segment of the learned index.
    void* root_;
    //Learned index constants
    static const uint8_t max_levels_ = 16;
    const double initial_filled_ratio_;
    const bool use_linear_regression_;
    const bool use_SIMD_;
    //Statistics
    uint64_t num_levels_; // the number of layers including model layers and the data layer
    uint64_t num_data_buckets_; //TODO: update num_data_buckets_ during bulk_load and insert
    uint64_t level_stats_[max_levels_]; // TODO: update level_stats_ during bulk_load and insert

};

} // end namespace buckindex
