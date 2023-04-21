#pragma once

#include "bucket.h"
#include "segment.h"
#include "segmentation.h"
#include "util.h"

namespace buckindex {
/**
 * Index configurations
 */
#define FILLED_RATIO 0.5


#ifndef DEBUG
#define DEBUG
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
        if (root_ == nullptr) { 
            std::vector<KeyValueType> kvs;
            KeyValueType kv1(std::numeric_limits<KeyType>::min(), 0);
            kvs.push_back(kv1);
            kvs.push_back(kv);
            bulk_load(kvs);
            return true;
        }

        // traverse to the leaf D-Bucket, and record the path
        std::vector<uintptr_t> seg_ptrs(num_levels_);//root-to-leaf path, including the  data bucket
        bool success = lookup(kv.key_, seg_ptrs);
        assert(success);

        DataBucketType* d_bucket = (DataBucketType *)seg_ptrs[num_levels_-1];
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
            uintptr_t old_ptr = reinterpret_cast<uintptr_t>(d_bucket);

            // propagate the insertion to the parent segments
            assert(num_levels_ >= 2); // insert into leaf segment
            int cur_level = num_levels_ - 2; // leaf_segment level
            while(cur_level >= 0) {
                SegmentType* cur_segment = (SegmentType*)seg_ptrs[cur_level];
                
                bool is_segment = true;
                if (cur_level == num_levels_ - 2) is_segment = false; // TODO: let seg.lookup() return key+value ptr instead of ptr only
                if (cur_segment->batch_update(old_ptr, pivot_list[ping], is_segment)) {
                    pivot_list[ping].clear();
                    break;
                }

                pivot_list[pong].clear();
                success = cur_segment->segment_and_batch_update(FILLED_RATIO, pivot_list[ping], pivot_list[pong]);
                old_ptr = reinterpret_cast<uintptr_t>(cur_segment);
                assert(success);

                GC_segs.push_back((uintptr_t)cur_segment);
                cur_level--;
                ping = 1 - ping;
                pong = 1 - pong;
            }

            // add one more level
            assert(pivot_list[ping].size() == 0 || cur_level == -1);
            if (pivot_list[ping].size() > 0) {
                double start_key = pivot_list[ping].front().key_;
                double end_key = pivot_list[ping].back().key_;
                double slope = 0.0, offset = 0.0;
                if (pivot_list[ping].size() > 1) {
                    slope = (end_key - start_key) /(pivot_list[ping].size()- 1);
                    offset = -slope * start_key;
                }
                LinearModel<KeyType> model(slope, offset);
                root_ = new SegmentType(pivot_list[ping].size(), FILLED_RATIO, model, 
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
     * @param seg_ptrs: the path from root to the leaf D-Bucket
    */
    bool lookup(KeyType key, std::vector<uintptr_t> &seg_ptrs) {
        // traverse the index to the leaf D-Bucket, and record the path
        bool success = true;
        seg_ptrs[0] = (uintptr_t)root_;
        for (int i = 1; i < num_levels_; i++) {
            SegmentType* segment = (SegmentType*)seg_ptrs[i-1];
            success &= segment->lookup(key, seg_ptrs[i]);
            assert((void *)seg_ptrs[i] != nullptr);
        }
        assert(success);
        return success;
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

    //The root segment of the learned index.
    void* root_;
    //Learned index constants
    static const uint8_t max_levels_ = 16;
    //const double filled_ratio_ = 0.50;
    //Statistics
    uint64_t num_levels_; // the number of layers including model layers and the data layer
    uint64_t num_data_buckets_; //TODO: update num_data_buckets_ during bulk_load and insert
    uint64_t level_stats_[max_levels_];

};

} // end namespace buckindex
