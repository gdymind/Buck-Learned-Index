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
#define MAX_DATA_BUCKET_SIZE 4
#define MAX_SEGMENT_BUCKET_SIZE 2
#define FILLED_RATIO 0.5
#else
#define MAX_DATA_BUCKET_SIZE 128
#define MAX_SEGMENT_BUCKET_SIZE 8
#define FILLED_RATIO 0.5
#endif

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
        // std::cout << "inserting key: " << kv.key_ << std::endl << std::flush;
        if (root_ == nullptr) { 
            std::vector<KeyValueType> kvs(1, kv);
            // kvs[0] = KeyValueType(std::numeric_limits<KeyType>::min(), 0);
            // kvs[1] = kv;
            bulk_load(kvs);
            return true;
        }

        // traverse to the leaf D-Bucket, and record the path
        std::vector<uintptr_t> seg_ptrs(num_levels_, (uintptr_t)nullptr);//root-to-leaf path, including the  data bucket
        bool success = lookup(kv.key_, seg_ptrs);
        assert(success);

        // std::cout << "lookup done" << std::endl;

        DataBucketType* d_bucket = (DataBucketType *)seg_ptrs[num_levels_-1];
        success = d_bucket->insert(kv, true);

        // TODO: need to implement the GC
        std::vector<uintptr_t> GC_segs;

        // if fail to insert, split the bucket, and add new kvptr on parent segment
        if (!success) {
            // split the bucket, and insert the new kv
            KeyType median_key;
            DataBucketType* new_bucket = d_bucket->split(median_key);
            if (kv.get_key() <= median_key) d_bucket->insert(kv, true);
            else new_bucket->insert(kv, true);
            KeyValuePtrType new_entry = KeyValuePtrType(new_bucket->get_pivot(), (uintptr_t)new_bucket);

            // std::cout << "split done: " << "median_key = " << median_key << std::endl;
            // std::cout << "new_entry: " << new_entry.key_ << ", " << new_entry.value_ << std::endl;
            // std::cout << "old bucket size: " << d_bucket->num_keys() << std::endl;
            // std::cout << "new bucket size: " << new_bucket->num_keys() << std::endl;

            // insert into leaf segment
            assert(num_levels_ >= 2);
            // std::cout << "num_levels_: " << num_levels_ << std::endl;
            SegmentType* leaf_segment = (SegmentType*)seg_ptrs[num_levels_-2];
            success = leaf_segment->insert(new_entry);
            if (!success) {
                std::vector<KeyValuePtrType> pivot_list[2]; // ping-pong list
                int ping = 0, pong = 1;
                pivot_list[ping].push_back(new_entry);

                SegmentType *old_segment = nullptr;
                int cur_level = num_levels_ - 2; // leaf_segment level
                while(cur_level >= 0) {
                    // std::cout << "cur_level: " << cur_level << std::endl;
                    SegmentType* cur_segment = (SegmentType*)seg_ptrs[cur_level];
                    
                    if ((cur_level != num_levels_ - 2)
                        && cur_segment->batch_update(old_segment, pivot_list[ping])) {
                        pivot_list[ping].clear();
                        break;
                    }

                    // std::cout << "did not batch update" << std::endl;

                    pivot_list[pong].clear();
                    success = cur_segment->segment_and_batch_insert(FILLED_RATIO, pivot_list[ping], pivot_list[pong]);
                    // std::cout << "segment_and_batch_insert done" << std::endl;
                    old_segment = cur_segment;
                    assert(success);

                    GC_segs.push_back((uintptr_t)cur_segment);
                    cur_level--;
                    ping = 1 - ping;
                    pong = 1 - pong;
                }

                // add one more level
                assert(pivot_list[ping].size() == 0 || cur_level == -1);
                if (pivot_list[ping].size() > 0) {
                    // std::cout << "adding one more level" << std::endl;
                    GC_segs.push_back((uintptr_t)root_);
                    double start_key = pivot_list[ping].front().key_;
                    double end_key = pivot_list[ping].back().key_;
                    double slope = (double)(pivot_list[ping].size()- 1) / (end_key - start_key);
                    double offset = -slope * start_key;
                    LinearModel<KeyType> model(slope, offset);
                    root_ = new SegmentType(pivot_list[ping].size(), FILLED_RATIO, model, 
                                        pivot_list[ping].begin(), pivot_list[ping].end());
                    num_levels_++;
                    // std::cout << "added one more level" << std::endl;
                }
            }
       
            d_bucket->invalidate_keys_gr_median(median_key);
            num_data_buckets_++;

            // GC
            // TODO: support MRSW
            // for (auto seg_ptr : GC_segs) {
            //     SegmentType* seg = (SegmentType*)seg_ptr;
            //     delete seg;
            // }
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
        // std::cout << "Finding lookup path: key = " << key << endl;
        // std::cout << "num_levels_ = " << num_levels_ << endl;
        // traverse the index to the leaf D-Bucket, and record the path
        bool success = true;
        seg_ptrs.resize(num_levels_);
        seg_ptrs[0] = (uintptr_t)root_;
        for (int i = 1; i < num_levels_; i++) {
            SegmentType* segment = (SegmentType*)seg_ptrs[i-1];
            // std::cout << "i = " << i << "; \t";
            // std::cout << "looking up key = " << key << std::endl << std::flush;
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
