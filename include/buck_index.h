#pragma once

#include "../tscns.h"
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

    BuckIndex(double initial_filled_ratio=0.7, int error_bound=8, int N_merge=1) {
        init(initial_filled_ratio, error_bound, N_merge);
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

    void init(double initial_filled_ratio, int error_bound, int N_merge){
        root_ = NULL;
        num_levels_ = 0;

        error_bound_ = error_bound;
        std::cout << "Segmeantation error bound = " << error_bound_ << std::endl;

        initial_filled_ratio_ = initial_filled_ratio;
        std::cout << "Initial fill ratio = " << initial_filled_ratio_ << std::endl;

        // N_merge_ = N_merge;
        // std::cout << "N_merge = " << N_merge_ << std::endl;

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

        //auto start = std::chrono::high_resolution_clock::now();

#ifdef BUCKINDEX_DEBUG
        auto start_time = tn.rdtsc();
#endif
        

        uint64_t layer_idx = num_levels_ - 1;
        uintptr_t seg_ptr = (uintptr_t)root_;
        bool result = false;
        value = 0;
        KeyValuePtrType kv_ptr;
        KeyValuePtrType kv_ptr_next;
        while (layer_idx > 0) {
            SegmentType* segment = (SegmentType*)seg_ptr;
            result = segment->lb_lookup(key, kv_ptr, kv_ptr_next);
            seg_ptr = kv_ptr.value_;
#ifdef BUCKINDEX_DEBUG
            if (!seg_ptr) {
                std::cerr << " failed to perform segment lookup for key: " << key << std::endl;
                return false;
            }
#endif
            layer_idx--;
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

        int num_scanned = 0;

        // traverse to leaf and record the path
        std::vector<KeyValuePtrType> path(num_levels_);//root-to-leaf path, including the data bucket
        LinearModel<KeyType> dummy_model;
        bool success = lookup_path(start_key, path, dummy_model);

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
            return true;
        }

        // traverse to the leaf D-Bucket, and record the path
        std::vector<KeyValuePtrType> path(num_levels_);//root-to-leaf path, including the  data bucket
        LinearModel<KeyType> model;
        bool success = lookup_path(kv.key_, path, model);
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
        DataBucketType* d_bucket = (DataBucketType *)(path[num_levels_-1].value_);
        if(kv.key_ == 0) {
            success = d_bucket->update(kv);
            //std::cout << "update key==0" << std::endl;
            return success;
        }
        else {
            success = d_bucket->insert(kv, true, hint);
        }
        
#ifdef BUCKINDEX_DEBUG
        auto insert_finish_time = tn.rdtsc();
#endif
        // TODO: need to implement the GC
        std::vector<uintptr_t> GC_segs;

        // if fail to insert, split the bucket, and add new kvptr on parent segment
        if (!success) {
            std::vector<KeyValuePtrType> old_pivot_list[2]; // ping-pong list for the pivots that need to be invalidated
            std::vector<KeyValuePtrType> new_pivot_list[2]; // ping-pong list for the parent that need to be inserted
            int ping = 0, pong = 1; // ping is the current level, pong is the parent level

            // split d_bucket
            auto new_d_buckets = d_bucket->split_and_insert(kv);

            // the old pivot list is to be replaced by the new pivot list
            old_pivot_list[ping].push_back(path[num_levels_-1]); // the old d_bucket
            new_pivot_list[ping].push_back(new_d_buckets.first);
            new_pivot_list[ping].push_back(new_d_buckets.second);


            // cout << "old_pivot_list[ping]: " << endl;
            // for (auto kv_ptr : old_pivot_list[ping]) {
            //     cout << "key = " << kv_ptr.key_ << ", value = " << kv_ptr.value_ << endl;
            // }

            // cout << "new_pivot_list[ping]: " << endl;
            // for (auto kv_ptr : new_pivot_list[ping]) {
            //     cout << "key = " << kv_ptr.key_ << ", value = " << kv_ptr.value_ << endl;
            // }

            // propagate the insertion to the parent segments
            assert(num_levels_ >= 2); // insert into leaf segment
            int cur_level = num_levels_ - 2; // leaf_segment level
            while(cur_level >= 0) {

                SegmentType* cur_segment = (SegmentType*)(path[cur_level].value_);
                // try batch inserting new pivots first
                if (cur_segment->batch_update(old_pivot_list[ping], new_pivot_list[ping])) {
                    // empty new_pivot_list[ping] to indicate that the no further propagation is needed
                    new_pivot_list[ping].clear(); 
                    success = true;
                    break;
                }


                // if batch insertion fails, re-segment along with adjcent segs
                // when performing re-segmentation, batch updating entries from old_pivot_list[ping] to new_pivot_list[ping] on the fly
                old_pivot_list[pong].clear();
                new_pivot_list[pong].clear();


                KeyValuePtrType parent_seg_kvptr;
                if (cur_level > 0)  parent_seg_kvptr = path[cur_level-1];
                else parent_seg_kvptr = KeyValuePtrType(std::numeric_limits<KeyType>::min(), (uintptr_t)nullptr);

                KeyValuePtrType cur_seg_kvptr = path[cur_level];

                resegment_adjcent_segments(parent_seg_kvptr, cur_seg_kvptr,
                                            N_merge_, N_merge_,
                                            old_pivot_list[ping], new_pivot_list[ping], // current level
                                            old_pivot_list[pong], new_pivot_list[pong]); // parent level
                
#ifdef BUCKINDEX_DEBUG
                level_stats_[num_levels_ - 1 - cur_level] += (new_pivot_list[pong].size()-old_pivot_list[pong].size());
#endif
                // GC old pivots in the current level
                if (cur_level != num_levels_ - 2) { // ensure not leaf segment
                                                    // beacuase their old pivots are buckets rather than segments
                    for (auto seg_ptr : old_pivot_list[ping]) {
                        GC_segs.push_back((uintptr_t)seg_ptr.value_);
                    }
                }
                cur_level--;
                ping = 1 - ping;
                pong = 1 - pong;
            }

            // add one more level
            assert(new_pivot_list[ping].size() == 0 || cur_level == -1);


            // what if there is only one node
            if (new_pivot_list[ping].size() > 1) {
                LinearModel<KeyType> model;
#ifdef BUCKINDEX_USE_LINEAR_REGRESSION
                std::vector<KeyType> keys;
                for (auto kv_ptr : new_pivot_list[ping]) {
                    keys.push_back(kv_ptr.key_);
                }
                model = LinearModel<KeyType>::get_regression_model(keys);
#else
                // TODO: instead of endpoints, use linear regression
                double start_key = new_pivot_list[ping].front().key_;
                double end_key = new_pivot_list[ping].back().key_;
                double slope = 0.0, offset = 0.0;
                if (new_pivot_list[ping].size() > 1) {
                    slope = (long double)new_pivot_list[ping].size() / (long double)(end_key - start_key);
                    offset = -slope * start_key;
                }
                model = LinearModel<KeyType>(slope, offset);
#endif
                root_ = new SegmentType(new_pivot_list[ping].size(), initial_filled_ratio_, model, 
                                    new_pivot_list[ping].begin(), new_pivot_list[ping].end());
#ifdef BUCKINDEX_DEBUG
                level_stats_[num_levels_] = 1;
#endif

                num_levels_++;
            } else if (new_pivot_list[ping].size() == 1){
                // GC_segs.push_back((uintptr_t)root_);
                // TODO: original root is not deleted
                root_ = (void*)(SegmentType*)new_pivot_list[ping][0].value_;
            }
#ifdef BUCKINDEX_DEBUG
            num_data_buckets_++;
            level_stats_[0]++;
            success = true;
#endif

            // GC
            delete d_bucket;
            for (auto seg_ptr : GC_segs) { // TODO: support MRSW
                SegmentType* seg = (SegmentType*)seg_ptr;
                delete seg;
            }
#ifdef BUCKINDEX_DEBUG
            insert_stats_.num_of_SMO++;
#endif
        }

#ifdef BUCKINDEX_DEBUG
        auto end_time = tn.rdtsc();
        insert_stats_.time_insert_in_leaf += (tn.tsc2ns(insert_finish_time) - tn.tsc2ns(start_time))/(double) 1000000000;
        insert_stats_.time_SMO += (tn.tsc2ns(end_time) - tn.tsc2ns(insert_finish_time))/(double) 1000000000;
        insert_stats_.num_of_insert++;
        num_keys_++;
#endif
        return success;
    }

    /**
     * Bulk load the user key value onto the learned index
     * @param kvs: list of user key value to be loaded onto the learned index
     */
    void bulk_load(vector<KeyValueType> &kvs) { // TODO: change to model-based insertion for d-buckets
        vector<KeyValuePtrType> kvptr_array[2];
        uint64_t ping = 0, pong = 1;
        num_levels_ = 0;
        run_data_layer_segmentation(kvs,
                                    kvptr_array[ping]);
        #ifdef BUCKINDEX_DEBUG
            num_keys_ = kvs.size();
            num_data_buckets_ = kvptr_array[ping].size();
            level_stats_[num_levels_] = num_data_buckets_;
        #endif
        num_levels_++;

        assert(kvptr_array[ping].size() > 0);

        do { // at least one model layer
            run_model_layer_segmentation(kvptr_array[ping],
                                            kvptr_array[pong]);
            ping = (ping +1) % 2;
            pong = (pong +1) % 2;
            kvptr_array[pong].clear();
            #ifdef BUCKINDEX_DEBUG
                level_stats_[num_levels_] = kvptr_array[ping].size();
            #endif
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
#ifdef BUCKINDEX_DEBUG
        for (auto i = 0; i < num_levels_; i++) {
            std::cout << "    Layer " << i << " size: " << level_stats_[i] << std::endl;
        }
#endif

        dump_fanout();

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
        std::vector<int> fanouts[num_levels_ - 1];

        // Traverse the tree to visit each segment
        std::queue<std::pair<void *, int>> q; // <segment, level> pairs
        q.push(std::make_pair(root_, 0));
        while (!q.empty()) {
            auto cur = q.front();
            q.pop();

            if (cur.second < num_levels_ - 1) {
                SegmentType *segment = (SegmentType *)cur.first;

                int cnt = 0;   
                for (auto it = segment->cbegin(); it != segment->cend(); it++) {
                    q.push(std::make_pair((void *)it->value_, cur.second + 1));
                    cnt++;
                }
                fanouts[cur.second].push_back(cnt);  
            }
        }

        std::cout << "Fanout Statistics:" << std::endl;
        for (int i = 0; i < num_levels_ - 1; i++) {
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

            if (cur.second < num_levels_ - 1) {
                SegmentType *segment = (SegmentType *)cur.first;
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
        return num_levels_;
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
#ifdef BUCKINDEX_DEBUG
        if(level >= num_levels_ || level < 0){
            return 0;
        }
        return level_stats_[level];
#else
        return 0;
#endif
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
\

#endif
    }
private:

    /**
     * Lookup function, traverse the index to the leaf D-Bucket, and record the path
     * @param key: lookup key
     * @param path: the path from root to the leaf D-Bucket
     * @param model: the endpoint model used to predict the position of the key in the leaf D-Bucket
    */
    bool lookup_path(KeyType key, std::vector<KeyValuePtrType> &path, LinearModel<KeyType> &model) {
        // traverse the index to the leaf D-Bucket, and record the path
        bool success = true;
        path[0] = KeyValuePtrType(std::numeric_limits<KeyType>::min(), (uintptr_t)root_);
        KeyValuePtrType kvptr_next; // TODO: change to the next key
        for (int i = 1; i < num_levels_; i++) {
            SegmentType* segment = (SegmentType*)path[i-1].value_;
            success &= segment->lb_lookup(key, path[i], kvptr_next);
            assert(success);
            assert((void *)path[i].value_ != nullptr);
        }
#ifdef HINT_MODEL_PREDICT
        KeyType start_key = path[num_levels_-1].key_;
        KeyType end_key = kvptr_next.key_;
        assert(end_key > start_key);
        double slope = (long double)DATA_BUCKET_SIZE / (long double)(end_key - start_key);
        double offset = -slope * start_key;
        model = LinearModel<KeyType>(slope, offset);
#endif
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
                                      vector<KeyValuePtrType>& out_kv_array) {
        vector<Cut<KeyType>> out_cuts;
        vector<LinearModel<KeyType>> out_models;
        Segmentation<vector<KeyValuePtrType>, KeyType>::compute_dynamic_segmentation(in_kv_array,
                                                                                     out_cuts, out_models,
                                                                                     error_bound_);
        for(auto i = 0; i < out_cuts.size(); i++) {
            uint64_t start_idx = out_cuts[i].start_;
            uint64_t length = out_cuts[i].size_;

            SegmentType* segment = new SegmentType(length, initial_filled_ratio_, out_models[i],
                                                   in_kv_array.begin() + start_idx, in_kv_array.begin() + start_idx + length);
            out_kv_array.push_back(KeyValuePtrType(in_kv_array[start_idx].key_,
                                                   (uintptr_t)segment));
        }
    }

    /**
     * @brief Resegment including adjecent segments
     * @param parent_seg_kvptr the parent segment of the cur seg. It is used to find adjcent segments of the cur seg.
     * @param cur_seg_kvptr the current segment to be resegmented
     * @param N1 the number of left segments to be merged
     * @param N2 the number of right segments to be merged
     * @param cur_old_seg_pivots the old pivots of the current segment, which will be replaced by cur_new_seg_pivots
     * @param cur_new_seg_pivots the new pivots of the current segment, which will replace cur_old_seg_pivots
     * @param par_old_seg_pivots the old pivots of the parent segment, which will be replaced in the parent segment
     * @param par_new_seg_pivots the new pivots of the parent segment, which will replace par_old_seg_pivots in the parent segment
    */
    bool resegment_adjcent_segments(KeyValuePtrType &parent_seg_kvptr, KeyValuePtrType &cur_seg_kvptr, 
                                int N1, int N2,
                                std::vector<KeyValuePtrType> &cur_old_seg_pivots,
                                std::vector<KeyValuePtrType> &cur_new_seg_pivots,
                                std::vector<KeyValuePtrType> &par_old_seg_pivots,
                                std::vector<KeyValuePtrType> &par_new_seg_pivots
                                ) {


        // TODO: if N1 and N2 are zero, call segment_and_xx directly
        // output entries in cur_old_seg_pivots and cur_new_seg_pivots
        // cout << "cur_old_seg_pivots: ";
        // for (auto it = cur_old_seg_pivots.begin(); it != cur_old_seg_pivots.end(); it++) {
        //     cout << it->key_ << " " << it->value_ << ", ";
        // }
        // cout << endl;
        // cout << "cur_new_seg_pivots: ";
        // for (auto it = cur_new_seg_pivots.begin(); it != cur_new_seg_pivots.end(); it++) {
        //     cout << it->key_ << " " << it->value_ << ", ";
        // }
        // cout << endl;

        SegmentType* parent_seg = (SegmentType*)parent_seg_kvptr.value_;
        SegmentType* cur_seg = (SegmentType*)cur_seg_kvptr.value_;
        
        KeyType cur_seg_pivot = cur_seg_kvptr.key_;
        // cout << "resegment_adjcent_segments: cur_seg_pivot = " << cur_seg_pivot << endl;

        std::vector<KeyValuePtrType> all_entries;

        // // iterate over parent_seg
        // if (parent_seg != nullptr) {
        //     cout << "parent_seg entries: ";
        //     for (auto it = parent_seg->cbegin(); it != parent_seg->cend(); it++) {
        //         cout << it->key_ << " " << it->value_ << ", ";
        //     }
        //     cout << endl << flush;
        //     cout << endl << flush;   
        // }

        // get the left N1 segment pivots
        if (parent_seg != nullptr) {
            auto it = parent_seg->lower_bound(cur_seg_pivot);
            // cout << "it(init):"; it.print();
            for (int i = 0; i < N1; i++) {
                if (it == parent_seg->cbegin()) break;
                it--;
                // cout << "it left:"; it.print();
                par_old_seg_pivots.push_back(*it);

                // get all the entries in the segment to be merged
                SegmentType* segment = (SegmentType*)it->value_;
                for (auto it2 = segment->cbegin(); it2 != segment->cend(); it2++) {
                    all_entries.push_back(*it2);
                }
            }
        }

        // // output all_entries at this point
        // cout << "all_entries(after include left N1 segs): ";
        // for (auto it = all_entries.begin(); it != all_entries.end(); it++) {
        //     cout << it->key_ << " " << it->value_ << ", ";
        // }
        // cout << endl << flush;
        // cout << endl << flush;


        // get the current segment
        par_old_seg_pivots.push_back(cur_seg_kvptr);
        // get all the entries in the current segment
        // replace entries in cur_old_seg_pivots to cur_new_seg_pivots on the fly
        KeyType start_key = cur_old_seg_pivots.front().key_;
        KeyType end_key = cur_old_seg_pivots.back().key_;
        auto it2 = cur_seg->cbegin();
        // get entries before start_key
        for (; it2 != cur_seg->cend() && it2->key_ < start_key; it2++) {
            all_entries.push_back(*it2);
        }
        // skip entries in cur_old_seg_pivots
        int cnt = 0;
        for (; it2 != cur_seg->cend() && it2->key_ <= end_key; it2++) {
            cnt++;
        }
        assert(cnt == cur_old_seg_pivots.size());
        // insert entries in cur_new_seg_pivots
        for (auto it3 = cur_new_seg_pivots.begin(); it3 != cur_new_seg_pivots.end(); it3++) {
            all_entries.push_back(*it3);
        }
        // get entries after end_key
        for (; it2 != cur_seg->cend(); it2++) {
            all_entries.push_back(*it2);
        }


        // get the right N2 segment pivots
        if (parent_seg != nullptr) {
            auto it = parent_seg->lower_bound(cur_seg_pivot);
            for (int i = 0; i < N2; i++) {
                if (it == parent_seg->cend()) break;
                it++;
                if (it == parent_seg->cend()) break;
                par_old_seg_pivots.push_back(*it);

                // get all the entries in the segment to be merged
                SegmentType* segment = (SegmentType*)it->value_;
                for (auto it2 = segment->cbegin(); it2 != segment->cend(); it2++) {
                    all_entries.push_back(*it2);
                }
            }
        }

        // // output all_entries at this point
        // cout << "all_entries(after include right N2 segs): ";
        // for (auto it = all_entries.begin(); it != all_entries.end(); it++) {
        //     cout << it->key_ << " " << it->value_ << ", ";
        // }
        // cout << endl << flush;
        // cout << endl << flush;

        // run the segmentation algorithm
        vector<Cut<KeyType>> out_cuts;
        vector<LinearModel<KeyType>> out_models;
        out_cuts.clear();
        out_models.clear();
        Segmentation<vector<KeyValuePtrType>, KeyType>::compute_dynamic_segmentation(all_entries, out_cuts, out_models, error_bound_);

        // cout << "out_cuts.size() = " << out_cuts.size() << endl;

        // generate new segments based on the cuts
        for (int i = 0; i < out_cuts.size(); i++) {
            uint64_t start_idx = out_cuts[i].start_;
            uint64_t length = out_cuts[i].size_;

            SegmentType* segment = new SegmentType(length, initial_filled_ratio_, out_models[i],
                                                   all_entries.begin() + start_idx, all_entries.begin() + start_idx + length);
            par_new_seg_pivots.push_back(KeyValuePtrType(out_cuts[i].start_key_, (uintptr_t)segment));
        }

        // // iterate over parent_seg
        // if (parent_seg != nullptr) {
        //     cout << "parent_seg entries: ";
        //     for (auto it = parent_seg->cbegin(); it != parent_seg->cend(); it++) {
        //         cout << it->key_ << " " << it->value_ << ", ";
        //     }
        //     cout << endl << flush;
        //     cout << endl << flush;   
        // }

        return true;
    }

    //The root segment of the learned index.
    void* root_;
    //Learned index constants
    static const uint8_t max_levels_ = 16;
    double initial_filled_ratio_;

    int error_bound_;
    int N_merge_;
    
    //Statistics
    
    uint64_t num_keys_; // the number of keys in the index 
    // NOTE: may include the dummy key 

    uint64_t num_levels_; // the number of layers including model layers and the data layer

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
