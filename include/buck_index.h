#pragma once

#include <stack>

#include "../tscns.h"
#include "bucket.h"
#include "segment.h"
#include "segmentation.h"
#include "greedy_error_corridor.h"
#include "util.h"

/**
 * Index configurations
 */
#define DEFAULT_FILLED_RATIO 0.6
#define MERGE_N_SMO_THRESHOLD 2
#define MERGE_WINDOW_SIZE 2

namespace buckindex {

template<typename KeyType, typename ValueType, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
class BuckIndex {
public:
    //List of template aliasing
    // TBD: use KeyListValueList or KeyValueList
    // using DataBucketType = Bucket<KeyListValueList<KeyType, ValueType, DATA_BUCKET_SIZE>,
    //                              KeyType, ValueType, DATA_BUCKET_SIZE>;
    using DataBucketType = Bucket<KeyValueList<KeyType, ValueType, DATA_BUCKET_SIZE>,
                                  KeyType, ValueType, DATA_BUCKET_SIZE>;
    using SegBucketType = Bucket<KeyValueList<KeyType, ValueType, SEGMENT_BUCKET_SIZE>,
                                  KeyType, ValueType, SEGMENT_BUCKET_SIZE>;
    using SegmentType = Segment<KeyType, SEGMENT_BUCKET_SIZE>;
    using KeyValueType = KeyValue<KeyType, ValueType>;
    using KeyValuePtrType = KeyValue<KeyType, uintptr_t>;

    BuckIndex(double initial_filled_ratio=0.7, int error_bound=8) {
        init(initial_filled_ratio, error_bound);
#ifdef BUCKINDEX_DEBUG
        std::cout << "BLI: Debug mode" << std::endl;
#else
        std::cout << "BLI: Release mode" << std::endl;
#endif

// hint system configuration
#ifdef HINT_MOD_HASH
        std::cout << "BLI: Using mod hash" << std::endl;
#endif
#ifdef HINT_CL_HASH
        std::cout << "BLI: Using cl hash" << std::endl; 
#endif
#ifdef HINT_MURMUR_HASH
        std::cout << "BLI: Using murmur hash" << std::endl; 
#endif
#ifdef HINT_MODEL_PREDICT
        std::cout << "BLI: Using model prediction" << std::endl;
#endif
#ifdef NO_HINT
        std::cout << "BLI: Using no hash" << std::endl;
#endif


#ifdef BUCKINDEX_USE_LINEAR_REGRESSION
        std::cout << "BLI: Using linear regression" << std::endl;
#else
        std::cout << "BLI: Using endpoint linear model" << std::endl;
#endif
#ifdef BUCKINDEX_USE_SIMD
        std::cout << "BLI: Using SIMD" << std::endl;
#else
        std::cout << "BLI: Not using SIMD" << std::endl;
#endif
    }

    void init(double initial_filled_ratio, int error_bound){
        root_ = NULL;

        error_bound_ = error_bound;
        std::cout << "Segmeantation error bound = " << error_bound_ << std::endl;

        initial_filled_ratio_ = initial_filled_ratio;
        std::cout << "Initial fill ratio = " << initial_filled_ratio_ << std::endl;

#ifdef BUCKINDEX_DEBUG
        tn.init();
#endif
    }

    ~BuckIndex() { }

    /**
     * Lookup function
     * @param key: lookup key
     * @param value: corresponding value to be returned
     * @return true if the key is found, else false
     */
    bool lookup(KeyType key, ValueType &value) {
        if (!root_) return false;

        // cout << "in lookup: root_ = " << (uint64_t)root_ << endl << flush;

        //auto start = std::chrono::high_resolution_clock::now();

#ifdef BUCKINDEX_DEBUG
        auto start_time = tn.rdtsc();
#endif
        

        uintptr_t seg_ptr = (uintptr_t)root_;
        bool result = false;
        value = 0;
        KeyValuePtrType kv_ptr;
        KeyValuePtrType kv_ptr_next;
        int level = 0;
        while (true) {
            SegmentType* segment = (SegmentType*)seg_ptr;
            // cout << "before lb_lookup: level = " << level << ", key = " << key << ", seg_ptr = " << seg_ptr << endl << flush;
            result = segment->lb_lookup(key, kv_ptr, kv_ptr_next);
            seg_ptr = kv_ptr.value_;
#ifdef BUCKINDEX_DEBUG
            if (!seg_ptr) {
                std::cerr << " failed to perform segment lookup for key: " << key << std::endl;
                return false;
            }
#endif
            if (segment->is_bottom_seg()) {
                // cout << "bottom segment found:" << seg_ptr << endl << flush;
                break;
            } else {
                // cout << "level = " << level << ", key = " << key << ", kv_ptr.key_ = " << kv_ptr.key_ << ", kv_ptr.value_ = " << kv_ptr.value_ << endl << flush;
                level++;
            }
        }

#ifdef BUCKINDEX_DEBUG
        auto end_traverse_time = tn.rdtsc();
        lookup_stats_.time_traverse_to_leaf += (tn.tsc2ns(end_traverse_time) - tn.tsc2ns(start_time))/(double) 1000000000;
#endif

        // decide the hint
        size_t hint = 0;
#ifdef HINT_MOD_HASH
        hint = (key) % DATA_BUCKET_SIZE;
#endif
#ifdef HINT_CL_HASH
        hint = clhash64(key) % DATA_BUCKET_SIZE; 
#endif
#ifdef HINT_MURMUR_HASH
        hint = murmur64(key) % DATA_BUCKET_SIZE; 
#endif
#ifdef HINT_MODEL_PREDICT
        //given kv_ptr and kv_ptr_next, check their key to make a linear model
        KeyType start_key = kv_ptr.key_;
        KeyType end_key = kv_ptr_next.key_;
        double slope = (long double)DATA_BUCKET_SIZE / (long double)(end_key - start_key);
        double offset = -slope * start_key;
        hint = (size_t)(slope * key + offset);
#endif
#ifdef NO_HINT
        hint=0;
#endif

        hint = std::min(hint, DATA_BUCKET_SIZE - 1);

        DataBucketType* d_bucket = (DataBucketType *)seg_ptr;
        result = d_bucket->lookup(key, value, hint);

#ifdef BUCKINDEX_DEBUG
        auto end_time = tn.rdtsc();
        auto diff = tn.tsc2ns(end_time) - tn.tsc2ns(start_time);
        lookup_stats_.time_lookup_in_leaf += (tn.tsc2ns(end_time) - tn.tsc2ns(end_traverse_time))/(double) 1000000000;
        lookup_stats_.time_lookup += (diff/(double) 1000000000);
        lookup_stats_.num_of_lookup++;
#endif

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

        // TODO: rewrite using const_dbuck_iterator

        int num_scanned = 0;

        // traverse to leaf and record the path
        std::vector<KeyValuePtrType> path;//root-to-leaf path, including the data bucket
        LinearModel<KeyType> dummy_model;
        bool success = lookup_path(start_key, path, dummy_model);
        int num_levels = path.size();

        // get the d-bucket iterator
        DataBucketType* d_bucket = (DataBucketType *)(path[num_levels-1]).value_;
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
                    d_bucket = (DataBucketType *)(path[num_levels-1]).value_;
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
    bool insert(KeyValueType& kv) { // TODO: change to model-based insertion for d-buckets

#ifdef BUCKINDEX_DEBUG
        auto start_time = tn.rdtsc();
#endif

        if (root_ == nullptr) { 
            std::vector<KeyValueType> kvs;
            KeyValueType kv1(std::numeric_limits<KeyType>::min(), 0);
            kvs.push_back(kv1);
            kvs.push_back(kv);
            bulk_load(kvs);
            cout << "inserted key " << kv.key_ << " into empty index" << endl;
            return true;
        }

        // traverse to the leaf D-Bucket, and record the path
        std::vector<KeyValuePtrType> path;//root-to-leaf path, including the  data bucket
        LinearModel<KeyType> model;
        bool success = lookup_path(kv.key_, path, model);
        int num_levels = path.size();

        // // outuput path
        // cout << "path: ";
        // for (auto kvptr : path) {
        //     cout << "(" << kvptr.key_ << ", " << kvptr.value_ << ") ";
        // }
        // cout << endl;

        assert(success);
        size_t hint = 0;
#ifdef HINT_MOD_HASH
        hint = (kv.key_) % DATA_BUCKET_SIZE;
#endif
#ifdef HINT_CL_HASH
        hint = clhash64(kv.key_) % DATA_BUCKET_SIZE; 
#endif
#ifdef HINT_MURMUR_HASH
        hint = murmur64(kv.key_) % DATA_BUCKET_SIZE; 
#endif
#ifdef HINT_MODEL_PREDICT
        hint = model.predict(kv.key_);
#endif
#ifdef NO_HINT
        hint=0;
#endif

        hint = std::min(hint, DATA_BUCKET_SIZE - 1);
        DataBucketType* d_bucket = (DataBucketType *)(path[num_levels-1].value_);
        if(kv.key_ == std::numeric_limits<KeyType>::min()) {
            std::cout << "update key==" << std::numeric_limits<KeyType>::min() << std::endl << std::flush;
            success = d_bucket->update(kv);
            assert(success);
            return success;
        }
        else {
            success = d_bucket->insert(kv, true, hint);
        }
        
#ifdef BUCKINDEX_DEBUG
        auto insert_finish_time = tn.rdtsc();
#endif

        // if fail to insert, split the bucket, and add new kvptr on parent segment
        if (!success) {
            // cout << "failed to insert key " << kv.key_ << " into d-bucket " << path[num_levels-1].key_ << endl << flush;
            std::vector<uintptr_t> GC_segs; // TODO: need to implement the GC

            // split d_bucket
            auto new_d_buckets = d_bucket->split_and_insert(kv);
            auto old_pivot = path[num_levels-1];

            // cout << "splitting d-bucket to (" << new_d_buckets.first.key_ << ", " << new_d_buckets.first.value_ << ") and (" << new_d_buckets.second.key_ << ", " << new_d_buckets.second.value_ << ")" << endl << flush;
            // cout << "all entries in the new d-buckets: " << endl;
            // DataBucketType* dbuck2 = (DataBucketType*)new_d_buckets.second.value_;
            // for (auto it = dbuck2->begin(); it != dbuck2->end(); it++) {
            //     cout << "(" << (*it).key_ << ", " << (*it).value_ << ") ";
            // }
            // cout << endl;

            // cout << "new_d_buckets.second: " << new_d_buckets.second.key_ << ", " << new_d_buckets.second.value_ << endl;

            std::vector<KeyValuePtrType> new_pivots;
            new_pivots.push_back(new_d_buckets.first);
            new_pivots.push_back(new_d_buckets.second);
            
            SegmentType* leaf_segment = (SegmentType*)(path[num_levels - 2].value_);
            if (!leaf_segment->batch_update(old_pivot, new_pivots)) {
                // cout << "failed to batch update: old_pivot = (" << old_pivot.key_ << ", " << old_pivot.value_ << "), new_pivots = (" << new_pivots[0].key_ << ", " << new_pivots[0].value_ << "), (" << new_pivots[1].key_ << ", " << new_pivots[1].value_ << ")" << endl;
                
                // compute the indicator to decide whether to merge neighbors or not
                SegmentType* parent_segment;
                double avg_smo = 0.0;

                if (num_levels > 2) {
                    parent_segment = (SegmentType*)(path[num_levels-3].value_);
                    avg_smo = get_avg_smo_in_window(leaf_segment, parent_segment, MERGE_WINDOW_SIZE);
                }

                if (num_levels > 2 && avg_smo >= MERGE_N_SMO_THRESHOLD) { // neighbor merge; TODO: include other factors determining whether merge or not
                    n_merging_++;
                    KeyValuePtrType old_lca, new_lca;
                    int lca_level;
                    success = dbuck_merge(new_d_buckets, path, old_lca, new_lca, lca_level);
                    assert(success);
                    if (lca_level == 0) {
                        root_ = (void *)new_lca.value_;
                        // delete (SegmentType *)old_lca.value_;
                    } else {
                        SegmentType* parent_segment = (SegmentType*)(path[lca_level-1].value_);
                        parent_segment->update(old_lca, new_lca);
                    }
                } else {
                    n_non_merging_++;
                    std::vector<KeyValuePtrType> pivot_list[2]; // ping-pong list
                    int ping = 0, pong = 1;

                    pivot_list[ping].push_back(new_d_buckets.first);
                    pivot_list[ping].push_back(new_d_buckets.second);

                    // propagate the insertion to the parent segments
                    assert(num_levels >= 2); // insert into leaf segment
                    int cur_level = num_levels - 2; // leaf_segment level
                    while(cur_level >= 0) {
                        SegmentType* cur_segment = (SegmentType*)(path[cur_level].value_);
                        
                        if (cur_segment->batch_update(old_pivot, pivot_list[ping])) {
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

                    // what if there is only one node
                    if (pivot_list[ping].size() > 1) {
                        LinearModel<KeyType> model;
                    #ifdef BUCKINDEX_USE_LINEAR_REGRESSION
                        std::vector<KeyType> keys;
                        for (auto kv_ptr : pivot_list[ping]) {
                            keys.push_back(kv_ptr.key_);
                        }
                        model = LinearModel<KeyType>::get_regression_model(keys);
                    #else
                        // TODO: instead of endpoints, use linear regression
                        double start_key = pivot_list[ping].front().key_;
                        double end_key = pivot_list[ping].back().key_;
                        double slope = 0.0, offset = 0.0;
                        if (pivot_list[ping].size() > 1) {
                            slope = (long double)pivot_list[ping].size() / (long double)(end_key - start_key);
                            offset = -slope * start_key;
                        }
                        model = LinearModel<KeyType>(slope, offset);
                    #endif
                        root_ = new SegmentType(pivot_list[ping].size(), initial_filled_ratio_, model, 
                                            pivot_list[ping].begin(), pivot_list[ping].end(), false);


                    } else if (pivot_list[ping].size() == 1){
                        // GC_segs.push_back((uintptr_t)root_);
                        // TODO: original root is not deleted
                        root_ = (void*)(SegmentType*)pivot_list[ping][0].value_;
                    }

                    // GC
                    // delete d_bucket;
                    // for (auto seg_ptr : GC_segs) { // TODO: support MRSW
                    //     SegmentType* seg = (SegmentType*)seg_ptr;
                    //     delete seg;
                    // }
                }
            } else {
                success = true;
            }
        

#ifdef BUCKINDEX_DEBUG
            num_data_buckets_++;
            level_stats_[0]++;
#endif

            // GC
            delete d_bucket;
            // TODO: GC
            for (auto seg_ptr : GC_segs) { // TODO: support MRSW
                SegmentType* seg = (SegmentType*)seg_ptr;
                delete seg;
            }
#ifdef BUCKINDEX_DEBUG
            insert_stats_.num_of_SMO++;
#endif
            assert(success);
        } else {
            // cout << "inserted key " << kv.key_ << " into d-bucket " << path[num_levels-1].key_ << endl;
        }

#ifdef BUCKINDEX_DEBUG
        auto end_time = tn.rdtsc();
        insert_stats_.time_insert_in_leaf += (tn.tsc2ns(insert_finish_time) - tn.tsc2ns(start_time))/(double) 1000000000;
        insert_stats_.time_SMO += (tn.tsc2ns(end_time) - tn.tsc2ns(insert_finish_time))/(double) 1000000000;
        insert_stats_.num_of_insert++;
        num_keys_++;
#endif
        assert(success);
        return success;
    }

    /**
     * Bulk load the user key value onto the learned index
     * @param kvs: list of user key value to be loaded onto the learned index
     */
    void bulk_load(vector<KeyValueType> &kvs) { // TODO: change to model-based insertion for d-buckets
        cout << "bulk load" << endl << flush;
        if (kvs[0].key_ != std::numeric_limits<KeyType>::min()) {
            KeyValueType kv(std::numeric_limits<KeyType>::min(), 0);
            kvs.insert(kvs.begin(), kv);
        }
        vector<KeyValuePtrType> kvptr_array[2];
        uint64_t ping = 0, pong = 1;
        run_data_layer_segmentation(kvs,
                                    kvptr_array[ping]);

        // cout << "d-buckets: ";
        // for (auto kvptr : kvptr_array[ping]) {
        //     cout << "(" << kvptr.key_ << ", " << kvptr.value_ << ") ";
        // }
        // cout << endl << flush;
        #ifdef BUCKINDEX_DEBUG
            num_keys_ = kvs.size();
            num_data_buckets_ = kvptr_array[ping].size();
        #endif

        assert(kvptr_array[ping].size() > 0);

        bool is_bottom_seg = true;
        do { // at least one model layer
            run_model_layer_segmentation(kvptr_array[ping],
                                            kvptr_array[pong], is_bottom_seg);
            is_bottom_seg = false;
            // cout << "segments: ";
            // for (auto kvptr : kvptr_array[pong]) {
            //     cout << "(" << kvptr.key_ << ", " << kvptr.value_ << ", ";
            //     // output the number of s-buckets in the segment
            //     SegmentType* segment = (SegmentType*)kvptr.value_;
            //     cout << segment->num_bucket_ << ") ";
            // }
            // cout << endl << flush;

            ping = (ping +1) % 2;
            pong = (pong +1) % 2;
            kvptr_array[pong].clear();
        } while (kvptr_array[ping].size() > 1);
        
        // Build the root
        KeyValuePtrType& kv_ptr = kvptr_array[ping][0];
        root_ = (void *)kv_ptr.value_;
        cout << "bulk load is done. root_ is " << (uint64_t)root_ << endl << flush;
        dump();

    }
    
    /**
     * Helper function to dump the index structure
     */
    void dump() {
        cout << "n_merging = " << n_merging_ << ", n_non_merging = " << n_non_merging_ << endl;

        dump_fanout();
        dump_depth();

        // print all element from DataBucketType::hint_dist_count using iterator
        // std::cout << "  Hint Distribution Count (distance = actual - predict): " << std::endl;
        // std::cout << "  DataBucketType::hint_dist_count.size(): " << hint_dist_count.size() << std::endl;
        // for (auto it = hint_dist_count.begin(); it != hint_dist_count.end(); it++) {
        //     std::cout << "    " << it->first << ": " << it->second << std::endl;
        // }


    }

    /**
     * Helper function to get inner node fanout statistics
    */
    void dump_fanout() {
        std::vector<int> fanouts[max_levels_ - 1];
        int num_levels = 1;

        // Traverse the tree to visit each segment
        std::queue<std::pair<void *, int>> q; // <segment, level> pairs
        q.push(std::make_pair(root_, 0));
        while (!q.empty()) {
            auto cur = q.front();
            q.pop();

            SegmentType *segment = (SegmentType *)cur.first;
            int cnt = 0;
            for (auto it = segment->cbegin(); it != segment->cend(); it++) {
                cnt++;
            }
            fanouts[cur.second].push_back(cnt);  

            if (!segment->is_bottom_seg()) {
                for (auto it = segment->cbegin(); it != segment->cend(); it++) {
                    q.push(std::make_pair((void *)it->value_, cur.second + 1));
                }
            } else {
                num_levels = max(num_levels, cur.second + 2);
            }
        }


        std::cout << "Fanout Statistics:" << std::endl;
        for (int i = 0; i < num_levels - 1; i++) {
            auto &fanout_cur = fanouts[i];
            sort(fanout_cur.begin(), fanout_cur.end());
            // get the average fanout
            int sum = 0;
            for (auto fanout : fanout_cur) {
                sum += fanout;
            }

            std::cout << "Level #" << i << ": ";
            std::cout << "Size = " << fanout_cur.size() << ", ";
            std::cout << "[Average, median, 99th percentile, min, max fanout] fanout = [";
            std::cout << (double)sum / fanout_cur.size() << ", ";
            std::cout << fanout_cur[fanout_cur.size() / 2] << ", ";
            std::cout << fanout_cur[(double)fanout_cur.size() * 99 / 100] << ", ";
            std::cout << fanout_cur[0] << ", ";
            std::cout << fanout_cur[fanout_cur.size() - 1] << "]" << std::endl;
        }
    }

    void dump_depth() {
        int depth[max_levels_ - 1] = {0};
        int num_levels = 1;

        // Traverse the tree to visit each segment
        std::queue<std::pair<void *, int>> q; // <segment, level> pairs
        q.push(std::make_pair(root_, 0));
        while (!q.empty()) {
            auto cur = q.front();
            q.pop();

            SegmentType *segment = (SegmentType *)cur.first;
            if (!segment->is_bottom_seg()) {
                int cnt = 0;   
                for (auto it = segment->cbegin(); it != segment->cend(); it++) {
                    q.push(std::make_pair((void *)it->value_, cur.second + 1));
                }
            } else {
                num_levels = max(num_levels, cur.second + 2);
                for (auto it = segment->cbegin(); it != segment->cend(); it++) {
                    depth[cur.second+1]++; 
                }
            }
        }

        cout << "max levels: " << num_levels << endl;


        std::cout << "# of dbuck in each level:" << std::endl;
        int sum_depth = 0;
        int num_dbuck = 0;
        for (int i = 0; i < num_levels; i++) {
            cout << "level " << i << ": " << depth[i] << endl;
            sum_depth += i * depth[i];
            num_dbuck += depth[i];
        }
        cout << "Average depth: " << (double)sum_depth / num_dbuck << endl;
    }


    /**
     * Helper function to get the memory size of the index
     *
     * @return the memory size of the index
     */

    size_t mem_size () const{
        size_t mem_size = 0;
        size_t d_bucket_size = 0;

        // Traverse the tree to visit each segment
        std::queue<std::pair<void *, int>> q; // <segment, level> pairs
        q.push(std::make_pair(root_, 0));
        while (!q.empty()) {
            auto cur = q.front();
            q.pop();

            SegmentType *segment = (SegmentType *)cur.first;
            if (!segment->is_bottom_seg()) {
                mem_size += segment->mem_size(); 
                for (auto it = segment->cbegin(); it != segment->cend(); it++) {
                    q.push(std::make_pair((void *)it->value_, cur.second + 1));
                }
            }
            else{ //cur is a d-bucket
                DataBucketType *d_bucket = (DataBucketType *)cur.first;
                mem_size += d_bucket->mem_size();
                d_bucket_size += d_bucket->mem_size();
            }
        }

        typedef BuckIndex<KeyType, ValueType, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> self_type;
        std::cout << "Total memory size: " << mem_size + sizeof(self_type) << std::endl;
        std::cout << "Total data bucket size: " << d_bucket_size << std::endl;
        return mem_size + sizeof(self_type);
    }


    /**
     * Helper function to get the number of levels in the index
     *
     * @return the number of levels in the index
     */
    uint64_t get_num_levels() {
        return 0;
    }

    /**
     * Helper function to get the number of levels in the index
     *
     * @return the number of data buckets in the index
     */
    uint64_t get_num_data_buckets() {
#ifdef BUCKINDEX_DEBUG
        return num_data_buckets_;
#else
        return 0;
#endif
    }

    SegmentType* get_root() const {
        return (SegmentType*)root_;
    }

    /**
     * Helper function to get the number of keys in the index
     *
     * @return the number of keys in the index
     */
    uint64_t get_num_keys() {
        return num_keys_;
    }
    
    /**
     * Helper function to get the stat of level in the index
     *
     * @return the number of keys in the index
     */
    uint64_t get_level_stat(int level) {
// #ifdef BUCKINDEX_DEBUG
//         if(level >= num_levels_ || level < 0){
//             return 0;
//         }
//         return level_stats_[level];
// #else
//         return 0;
// #endif
        return 0;
    }

    /**
     * Helper function to dump the look up statistics
     */
    void print_lookup_stat(){
#ifdef BUCKINDEX_DEBUG
        cout<<"-----lookup stat-----"<<endl;
        cout<<"num lookups: "<<lookup_stats_.num_of_lookup<<endl;
        cout<<"avg time lookup: "<<lookup_stats_.time_lookup/lookup_stats_.num_of_lookup<<endl;
        cout<<"avg time traverse to leaf: "<<lookup_stats_.time_traverse_to_leaf/lookup_stats_.num_of_lookup<<endl;
        cout<<"avg time lookup in leaf: "<<lookup_stats_.time_lookup_in_leaf/lookup_stats_.num_of_lookup<<endl;

        cout<<"-----insert stat-----"<<endl;
        cout<<"num inserts: "<<insert_stats_.num_of_insert<<endl;
        cout<<"avg time insert: "<<insert_stats_.time_insert_in_leaf/insert_stats_.num_of_insert<<endl;
        cout<<"avg time SMO: "<<insert_stats_.time_SMO/insert_stats_.num_of_SMO<<endl;
        cout<<"num SMO: "<<insert_stats_.num_of_SMO<<endl;

        cout<<"-----segment stat-----"<<endl;
        std::cout<<"Num of fail_predict: "<< SegmentType::fail_predict<<std::endl;
        std::cout<<"avg fail distance: "<< (double)SegmentType::fail_distance/SegmentType::fail_predict<<std::endl;
        std::cout<<"Num of success_predict: "<< SegmentType::success_predict<<std::endl;
        std::cout<<"Num of locate: "<< SegmentType::num_locate<<std::endl;

        // std::cout<<"Num of fail_predict: "<< SegmentType::fail_predict_bulk<<std::endl;
        // std::cout<<"avg fail distance: "<< (double)SegmentType::fail_distance/SegmentType::fail_predict_bulk<<std::endl;
        // std::cout<<"Num of success_predict: "<< SegmentType::success_predict_bulk<<std::endl;

#endif
    }

    /**
     * Lookup function, traverse the index to the leaf D-Bucket, and record the path
     * @param key: lookup key
     * @param path: the path from root to the leaf D-Bucket
     * @param model: the endpoint model used to predict the position of the key in the leaf D-Bucket
    */
    bool lookup_path(KeyType key, std::vector<KeyValuePtrType> &path, LinearModel<KeyType> &model) {
        // traverse the index to the leaf D-Bucket, and record the path
        bool success = true;
        path.clear();
        path.push_back(KeyValuePtrType(std::numeric_limits<KeyType>::min(), (uintptr_t)root_));
        KeyValuePtrType kvptr_next; // TODO: change to the next key
        int i = 1;
        while (true) {
            SegmentType* segment = (SegmentType*)path[i-1].value_;
            path.push_back(KeyValuePtrType(0, 0));
            success &= segment->lb_lookup(key, path[i], kvptr_next);
            assert((void *)path[i].value_ != nullptr);
            if (segment->is_bottom_seg()) {
                break;
            }
            i++;
        }
#ifdef HINT_MODEL_PREDICT
        int num_levels = path.size();
        KeyType start_key = path[num_levels-1].key_;
        KeyType end_key = kvptr_next.key_;
        assert(end_key > start_key);
        double slope = (long double)DATA_BUCKET_SIZE / (long double)(end_key - start_key);
        double offset = -slope * start_key;
        model = LinearModel<KeyType>(slope, offset);
#endif
        assert(success);
        return success;
    }

    class const_dbuck_iterator {
    public:
        const_dbuck_iterator(std::vector<KeyValuePtrType> &path){
            lca_level_ = path.size() - 2; // leaf_segment level
            for (int i = 0; i <= lca_level_; i++) {
                SegmentType* segment = (SegmentType*)path[i].value_;
                cur_path_.push(segment->lower_bound(path[i+1].key_));
            }
        }

        void operator++(int) {
            find_next();
        }

        void operator--(int) {
            find_previous();
        }

        // prefix --it
        const_dbuck_iterator &operator--() {
            find_previous();
            return *this;
        }

        // prefix ++it
        const_dbuck_iterator &operator++() {
            find_next();
            return *this;
        }

        // *it
        const DataBucketType* operator*() const {
            return (DataBucketType*)(cur_path_.top()->value_);
        }

        bool operator==(const const_dbuck_iterator& rhs) const {
            return **this == *rhs;
        }

        bool operator!=(const const_dbuck_iterator& rhs) const { 
            return !(*this == rhs);
        }

        bool reach_to_end(){
            return cur_path_.size() == 0;
        }

        bool reach_to_begin(){
            return (cur_path_.size() == 0) || (*cur_path_.top()).key_ == std::numeric_limits<KeyType>::min();
            // std::stack<SegIterType> tmp_path;
            // while (cur_path_.size() > 0) {
            //     if (!cur_path_.top().reach_to_begin()) {
            //         while (tmp_path.size() > 0) {
            //             cur_path_.push(tmp_path.top());
            //             tmp_path.pop();
            //         }
            //         return false;
            //     }
            //     tmp_path.push(cur_path_.top());
            //     cur_path_.pop();
            // }

            // while (tmp_path.size() > 0) {
            //     cur_path_.push(tmp_path.top());
            //     tmp_path.pop();
            // }

            return true;
        }

        int get_lca_level(){
            return lca_level_;
        }
    private:
        using SegIterType = typename Segment<KeyType, SEGMENT_BUCKET_SIZE>::const_iterator;
        int lca_level_;

        std::stack<SegIterType> cur_path_;

        // find the next entry in the sorted list (Can cross boundary of bucket)
        inline bool find_next() {
            if (this->reach_to_end()) return false;
            cur_path_.top()++;
            while (cur_path_.top().reach_to_end()) {
                cur_path_.pop();
                if (cur_path_.size() == 0) return false;
                cur_path_.top()++;
            }

            lca_level_ = min(lca_level_, (int)cur_path_.size()-1);

            while (!cur_path_.top().segment_->is_bottom_seg()) {
                SegmentType* next_level_segment = (SegmentType*)(cur_path_.top()->value_);
                cur_path_.push(next_level_segment->cbegin());
            }

            return true;
        }

        // find the previous entry in the sorted list (Can cross boundary of bucket)
        inline bool find_previous() {
            if (this->reach_to_begin()) return false;

            while (cur_path_.top().reach_to_begin()) {
                cur_path_.pop();
                if (cur_path_.size() == 0) return false;
            }

            lca_level_ = min(lca_level_, (int)cur_path_.size()-1);

            cur_path_.top()--;

            while (!cur_path_.top().segment_->is_bottom_seg()) {
                SegmentType* next_level_segment = (SegmentType*)(cur_path_.top()->value_);
                cur_path_.push(next_level_segment->cend());
                cur_path_.top()--;
            }

            return true;
        }
    };


private:



    /**
     * Helper function for scan() to find the next D-Bucket
     * @param path: the path from root to the leaf D-Bucket
     * @return true if the next D-Bucket is found, else false
    */
    bool find_next_d_bucket(std::vector<KeyValuePtrType> &path) {
        int num_levels = path.size();

        int cur_level = num_levels - 2; // leaf_segment level
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
                while(tranverse_down_level < num_levels - 1) {
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

#ifdef HINT_MODEL_PREDICT
            KeyType start_key = in_kv_array[start_idx].key_;
            KeyType end_key = std::numeric_limits<KeyType>::max();
            if (start_idx+length < in_kv_array.size()) end_key = in_kv_array[start_idx+length-1].key_;
            assert(end_key > start_key);
            double slope = (long double)DATA_BUCKET_SIZE / (long double)(end_key - start_key);
            double offset = -slope * start_key;
#endif
            
            //load the keys to the data bucket
            for(auto j = start_idx; j < (start_idx+length); j++) { // TODO: model-based insertion
                size_t hint = 0;

#ifdef HINT_MOD_HASH
                hint = (in_kv_array[j].key_) % DATA_BUCKET_SIZE;
#endif
#ifdef HINT_CL_HASH
                hint = clhash64(in_kv_array[j].key_) % DATA_BUCKET_SIZE; 
#endif
#ifdef HINT_MURMUR_HASH
                hint = murmur64(in_kv_array[j].key_) % DATA_BUCKET_SIZE; 
#endif
#ifdef HINT_MODEL_PREDICT
                hint = (size_t)(slope * in_kv_array[j].key_ + offset);
#endif
#ifdef NO_HINT
                hint=0;
#endif
                hint = min(hint, DATA_BUCKET_SIZE-1);
                d_bucket->insert(in_kv_array[j], true, hint);
            }
            //segment->dump();
        }
        //level_stats_[0] = out_cuts.size();
    }

    /**
     * Helper function to perform segmentation on the model layer
     *
     * @param in_kv_array: anchor array
     * @return out_kv_array: list of anchors for the sub-segments.
     */
    void run_model_layer_segmentation(vector<KeyValuePtrType>& in_kv_array,
                                      vector<KeyValuePtrType>& out_kv_array, bool is_bottom_seg) {
        vector<Cut<KeyType>> out_cuts;
        vector<LinearModel<KeyType>> out_models;
        Segmentation<vector<KeyValuePtrType>, KeyType>::compute_dynamic_segmentation(in_kv_array,
                                                                                     out_cuts, out_models,
                                                                                     error_bound_);
        for(auto i = 0; i < out_cuts.size(); i++) {
            uint64_t start_idx = out_cuts[i].start_;
            uint64_t length = out_cuts[i].size_;

            SegmentType* segment = new SegmentType(length, initial_filled_ratio_, out_models[i],
                                                   in_kv_array.begin() + start_idx, in_kv_array.begin() + start_idx + length, is_bottom_seg);
            out_kv_array.push_back(KeyValuePtrType(in_kv_array[start_idx].key_,
                                                   (uintptr_t)segment));
        }
    }

    /**
     * Helper function to get the average SMO in the window of the leaf segment
     * @param leaf_segment: the leaf segment
     * @param parent_segment: the parent of the leaf segment
     * @param window_size: the number of neighboring segments to be considered
     * @return the average SMO in the window
     */
    double get_avg_smo_in_window(SegmentType *leaf_segment, SegmentType *parent_segment, int window_size) {
        KeyType leaf_segment_key = leaf_segment->get_pivot();
        double neighbor_sum_smo = 0;
        double n_neighbor = 1;
        if (leaf_segment->get_n_smo() >= MERGE_N_SMO_THRESHOLD) {
            auto pivot_iter = parent_segment->lower_bound(leaf_segment_key);
            for (int i = 0; i < window_size && pivot_iter != parent_segment->cend(); i++) {
                SegmentType* neighbor_segment = (SegmentType*)pivot_iter->value_;
                neighbor_sum_smo += neighbor_segment->get_n_smo();
                n_neighbor++;
                pivot_iter++;
            }
            pivot_iter = parent_segment->lower_bound(leaf_segment_key);
            for (int i = 0; i < window_size; i++) {
                SegmentType* neighbor_segment = (SegmentType*)pivot_iter->value_;
                neighbor_sum_smo += neighbor_segment->get_n_smo();
                n_neighbor++;
                if (pivot_iter == parent_segment->cbegin()) break;
                pivot_iter--;
            }
        }
        neighbor_sum_smo -= leaf_segment->get_n_smo();
        return  neighbor_sum_smo / n_neighbor;
    }

    /**
     * Helper function to merge neighboring data buckets
     * @param new_d_buckets: the new data buckets to be merged; they are the result of splitting the old data bucket
     * @param path: the path from root to the leaf D-Bucket. This is used to find the neighboring data buckets in both directions
     * @param old_lca: the old LCA of all the data buckets to be merged. This is a return value of the function
     * @param new_lca: the new LCA that will replace the old LCA. This is a return value of the function
     * @param lca_level: the level of the LCA. This is a return value of the function
     */
    bool dbuck_merge(std::pair<KeyValuePtrType, KeyValuePtrType> new_d_buckets, std::vector<KeyValuePtrType> &path, KeyValuePtrType &old_lca, KeyValuePtrType &new_lca, int &lca_level) {
        // cout << "calling dbuck_merge:" << endl << flush;

        lca_level = path.size() - 1;

        // // output path
        // cout << "path: ";
        // for (auto kvptr : path) {
        //     cout << "(" << kvptr.key_ << ", " << kvptr.value_ << ") ";
        // }
        // cout << endl;

        DataBucketType *center_dbuck = (DataBucketType*)path.back().value_;
        auto center_pivot = center_dbuck->get_pivot();
        // Use GreedyErrorCorridor
        GreedyErrorCorridor<KeyType> gec;
        gec.init(center_pivot, error_bound_);

        // cout << "center_pivot: " << center_pivot << endl;

        // cout << "left neighbors: ";

        // Intialize an iterator for the right neighbors
        const_dbuck_iterator left_iter(path);
        while (!left_iter.reach_to_begin()) {
            left_iter--;
            if (left_iter.reach_to_begin()) break;
            auto left_dbuck = *left_iter;
            auto left_pivot = left_dbuck->get_pivot();
            if (gec.is_bounded(left_pivot)) {
                // cout << "(" << left_pivot << ", " << (uintptr_t)left_dbuck << ") ";
            } else {
                // cout << endl;
                break;
            }
        }
        lca_level = left_iter.get_lca_level();
        // cout << "lca_level: " << lca_level << endl;
        

        // cout << "right neighbors:";
        // Intialize an iterator for the right neighbors
        const_dbuck_iterator right_iter(path);
        gec.init(center_pivot, error_bound_);
        while (!right_iter.reach_to_end()) {
            right_iter++;
            if (right_iter.reach_to_end()) break;
            auto right_dbuck = *right_iter;
            auto right_pivot = right_dbuck->get_pivot();
            if (gec.is_bounded(right_pivot)) {
                // cout << "(" << right_pivot << ", " << (uintptr_t)right_dbuck << ") ";
            } else {
                // cout << endl;
                break;
            }
        }
        lca_level = min(lca_level, right_iter.get_lca_level());
        // cout << "lca_level: " << lca_level << endl;


        vector<KeyValuePtrType> sub_tree_path;
        sub_tree_path.push_back(path[lca_level]);
        while(true) {
            SegmentType* segment = (SegmentType*)sub_tree_path.back().value_;
            sub_tree_path.push_back(*(segment->cbegin()));
            if (segment->is_bottom_seg()) {
                break;
            }
        }

        // cout << "lca-to-leaf path: ";
        // for (auto kvptr : sub_tree_path) {
        //     cout << "(" << kvptr.key_ << ", " << kvptr.value_ << ") ";
        // }

        const_dbuck_iterator it_all_dbucks(sub_tree_path);
        std::vector<KeyValuePtrType> all_dbucks;
        for (; !it_all_dbucks.reach_to_end(); it_all_dbucks++) {
            const DataBucketType* dbuck = *it_all_dbucks;
            if (dbuck->get_pivot() == new_d_buckets.first.key_) {
                all_dbucks.push_back(new_d_buckets.first);
                all_dbucks.push_back(new_d_buckets.second);
            } else {
                all_dbucks.push_back(KeyValuePtrType(dbuck->get_pivot(), (uintptr_t)dbuck));
            }
        }

        // cout << "all_dbucks: ";
        // for (auto kvptr : all_dbucks) {
        //     cout << "(" << kvptr.key_ << ", " << kvptr.value_ << ") ";
        // }
        // cout << endl;


        if (all_dbucks.size() <= 1) {
            return false;
        }

        // partial bulk load
        vector<KeyValuePtrType> kvptr_array[2];
        uint64_t ping = 0, pong = 1;
        kvptr_array[ping] = all_dbucks;

        bool is_bottom_seg = true;
        do { // at least one model layer
            run_model_layer_segmentation(kvptr_array[ping],
                                            kvptr_array[pong], is_bottom_seg);
            is_bottom_seg = false;
            ping = (ping +1) % 2;
            pong = (pong +1) % 2;
            kvptr_array[pong].clear();
        } while (kvptr_array[ping].size() > 1);

        old_lca = path[lca_level];
        new_lca = kvptr_array[ping][0];

        // LinearModel<KeyType> model;
        // lookup_path(key, path[num_levels-1].key_, model);

        return true;
    }


    //The root segment of the learned index.
    void* root_;
    //Learned index constants
    static const uint8_t max_levels_ = 32;
    double initial_filled_ratio_;

    int error_bound_;
    
    //Statistics
    
    uint64_t num_keys_; // the number of keys in the index 

    uint64_t n_merging_ = 0; // the number of merging cases when batch update fails
    uint64_t n_non_merging_ = 0; // the number of non-merging cases when batch update fails

    // NOTE: may include the dummy key 

    // uint64_t num_levels_; // the number of layers including model layers and the data layer

#ifdef BUCKINDEX_DEBUG
    uint64_t num_data_buckets_; // the number of data buckets in the data layer
    uint64_t level_stats_[max_levels_]; // the number of buckets in each layer
    // NOTE: level_stats_[0] is the number of data buckets in the data layer

    TSCNS tn;
    
    struct lookupStats {
        size_t num_of_lookup = 0; // total number of lookup

        double time_lookup = 0; // total time to perform lookup;

        double time_traverse_to_leaf = 0; // total time to traverse to leaf; 
        // need to divide by num of lookup to get average

        double time_lookup_in_leaf = 0; // total time to lookup in leaf;
    };
    lookupStats lookup_stats_;

    struct insertStats { // TODO
        double time_traverse_to_leaf = 0; // total time to traverse to leaf; 
        // need to divide by num of lookup to get average

        double time_insert_in_leaf = 0; // total time to lookup in leaf;

        int num_of_insert = 0; // total number of insert

        double time_SMO = 0; // total time to perform SMO;

        int num_of_SMO = 0; // total number of SMO;
    };
    insertStats insert_stats_;
#endif



    // public:
    // // Number of elements
    // size_t size() const { return static_cast<size_t>(stats_.num_keys); }

    // // True if there are no elements
    // bool empty() const { return (size() == 0); }

    // // This is just a function required by the STL standard. ALEX can hold more
    // // items.
    // size_t max_size() const { return size_t(-1); }

    // // Size in bytes of all the keys, payloads, and bitmaps stored in this index
    // long long data_size() const {
    //     long long size = 0;
    //     for (NodeIterator node_it = NodeIterator(this); !node_it.is_end();
    //         node_it.next()) {
    //     AlexNode<T, P>* cur = node_it.current();
    //     if (cur->is_leaf_) {
    //         size += static_cast<data_node_type*>(cur)->data_size();
    //     }
    //     }
    //     return size;
    // }

    // // Size in bytes of all the model nodes (including pointers) and metadata in
    // // data nodes
    // long long model_size() const {
    //     long long size = 0;
    //     for (NodeIterator node_it = NodeIterator(this); !node_it.is_end();
    //         node_it.next()) {
    //     size += node_it.current()->node_size();
    //     }
    //     return size;
    // }

    // // Total number of nodes in the RMI
    // int num_nodes() const {
    //     return stats_.num_data_nodes + stats_.num_model_nodes;
    // };

    // // Number of data nodes in the RMI
    // int num_leaves() const { return stats_.num_data_nodes; };

    // // Return a const reference to the current statistics
    // const struct Stats& get_stats() const { return stats_; }

};

} // end namespace buckindex
