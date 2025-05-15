#pragma once

#include "../tscns.h"
#include "bucket.h"
#include "segment.h"
#include "segmentation.h"
#include "util.h"

#include <atomic>
#include <thread>
#include <future>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>

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
    int n_scan_ = 0;

    BuckIndex(double initial_filled_ratio=0.7, int error_bound=8) {
        init(initial_filled_ratio, error_bound);

        // Initialize worker threads
        shutdown_ = false;
        for (int i = 0; i < NUM_WORKER_THREADS; i++) {
            worker_threads_.emplace_back([this]() {
                while (!shutdown_) {  
                    SortTask task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        queue_cv_.wait(lock, [this]() { 
                            return !task_queue_.empty() || shutdown_; 
                        });
                        
                        // Check shutdown condition first
                        if (shutdown_) {
                            return;
                        }
                        
                        if (task_queue_.empty()) {
                            continue;  
                        }
                        
                        task = std::move(task_queue_.front());
                        task_queue_.pop();
                    }
                    
                    // Prepare and sort the bucket
                    task.result_vector->reserve(task.reserved_size);
                    task.bucket->get_valid_kvs(*task.result_vector);
                    std::sort(task.result_vector->begin(), task.result_vector->end());
                    task.promise.set_value();
                }
            });
        }
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

    ~BuckIndex() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            shutdown_ = true;
        }
        queue_cv_.notify_all();  // Wake up all worker threads
        
        // Wait for all worker threads to finish
        for (auto& thread : worker_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    void init(double initial_filled_ratio, int error_bound){
        root_ = NULL;
        num_levels_ = 0;

        error_bound_ = error_bound;
        std::cout << "Segmeantation error bound = " << error_bound_ << std::endl;

        initial_filled_ratio_ = initial_filled_ratio;
        std::cout << "Initial fill ratio = " << initial_filled_ratio_ << std::endl;

#ifdef BUCKINDEX_DEBUG
        tn.init();
#endif
    }

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

        n_scan_++;
        int num_scanned = 0;

        // traverse to leaf and record the path
        std::vector<KeyValuePtrType> path(num_levels_);//root-to-leaf path, including the data bucket
        LinearModel<KeyType> dummy_model;
        bool success = lookup_path(start_key, path, dummy_model);

        // get the d-bucket
        DataBucketType* d_bucket = (DataBucketType *)(path[num_levels_-1]).value_;

        while (num_scanned < num_to_scan) {
            std::vector<KeyValueType> scanned_kvs;
            d_bucket->scan_kvs(scanned_kvs, start_key, num_to_scan - num_scanned);
            for (auto &kv : scanned_kvs) {
                kvs[num_scanned] = make_pair(kv.key_, kv.value_);
                num_scanned++;
            }

            // get the next d-bucket
            if (num_scanned < num_to_scan) {
                do {
                    if (!find_next_d_bucket(path)) return num_scanned;
                    d_bucket = (DataBucketType *)(path[num_levels_-1]).value_;
                } while (d_bucket->num_keys() == 0); // empty d-bucket, visit the next one
            }
        }
        
        return num_scanned;
    }


    size_t scan_parallel(KeyType start_key, size_t num_to_scan, std::pair<KeyType, ValueType> *result) {
        if (!root_) return 0;

        // Find the starting bucket
        std::vector<KeyValuePtrType> path(num_levels_);
        LinearModel<KeyType> dummy_model;
        bool success = lookup_path(start_key, path, dummy_model);
        
        // Collect kvs from each bucket into separate vectors
        DataBucketType* curr_bucket = (DataBucketType *)(path[num_levels_-1]).value_;
        auto dbuck_iter = curr_bucket->lower_bound(start_key);

        size_t total_kvs = 0;
        std::vector<DataBucketType*> target_buckets;
        std::vector<size_t> bucket_sizes;

        while (curr_bucket && total_kvs < num_to_scan) {
            size_t bucket_size = curr_bucket->num_keys();
            if (bucket_size > 0) {
                target_buckets.push_back(curr_bucket);
                bucket_sizes.push_back(bucket_size);
                total_kvs += bucket_size;
            }
            
            if (!find_next_d_bucket(path)) break;
            curr_bucket = (DataBucketType*)(path[num_levels_-1]).value_;
        }
        
        // Pre-allocate vectors for all buckets
        std::vector<std::vector<KeyValueType>> bucket_kvs_list(target_buckets.size());
        std::vector<std::future<void>> futures;

        // Distribute sorting tasks to worker threads
        for (size_t i = 0; i < target_buckets.size(); i++) {
            std::promise<void> promise;
            futures.push_back(promise.get_future());
            
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                task_queue_.push({
                    target_buckets[i],
                    bucket_sizes[i],
                    &bucket_kvs_list[i],
                    std::move(promise)
                });
            }
            queue_cv_.notify_one();
        }

        // Wait for all sorting tasks to complete
        for (auto& future : futures) {
            future.wait();
        }

        int i = 0;
        int j = lower_bound(bucket_kvs_list[i].begin(), bucket_kvs_list[i].end(), KeyValueType(start_key, 0)) - bucket_kvs_list[i].begin();
        size_t num_copied = 0;
        while (i < bucket_kvs_list.size() && j < bucket_kvs_list[i].size() && num_copied < num_to_scan) {
            result[num_copied] = std::make_pair(bucket_kvs_list[i][j].key_, bucket_kvs_list[i][j].value_);
            num_copied++;
            j++;
        }

        // Use two for loops to copy the sorted kvs to the result array
        for (size_t i = 1; i < bucket_kvs_list.size() && num_copied < num_to_scan; i++) {
            for (size_t j = 0; j < bucket_kvs_list[i].size() && num_copied < num_to_scan; j++) {
                // convert KeyValuePtrType to pair<KeyType, ValueType>
                result[num_copied] = std::make_pair(bucket_kvs_list[i][j].key_, bucket_kvs_list[i][j].value_);
                num_copied++;
            }
        }
        return num_copied;
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
#ifdef BUCKINDEX_DEBUG
                level_stats_[num_levels_ - 1 - cur_level] += (pivot_list[pong].size()-1);
#endif
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
                                    pivot_list[ping].begin(), pivot_list[ping].end());
#ifdef BUCKINDEX_DEBUG
                level_stats_[num_levels_] = 1;
#endif

                num_levels_++;
            } else if (pivot_list[ping].size() == 1){
                // GC_segs.push_back((uintptr_t)root_);
                // TODO: original root is not deleted
                root_ = (void*)(SegmentType*)pivot_list[ping][0].value_;
            }
#ifdef BUCKINDEX_DEBUG
            num_data_buckets_++;
            level_stats_[0]++;
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

    //The root segment of the learned index.
    void* root_;
    //Learned index constants
    static const uint8_t max_levels_ = 16;
    double initial_filled_ratio_;

    int error_bound_;

    static const int NUM_WORKER_THREADS = 11;
    struct SortTask {
        DataBucketType* bucket;
        size_t reserved_size;
        std::vector<KeyValueType>* result_vector;
        std::promise<void> promise;
    };

    std::vector<std::thread> worker_threads_;
    std::queue<SortTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    bool shutdown_;
    
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
