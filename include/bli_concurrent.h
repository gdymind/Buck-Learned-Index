#include <queue>
#include <future>
#include <chrono>
#include <pthread.h>
#include <tbb/concurrent_queue.h>
#include <iostream>
#include <thread>

#include "keyvalue.h"
#include "buck_index.h"

namespace  buckindex{

/**
 * Concurrent interface of BLI; used by GRE
*/
template<typename T, typename V, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
class BLI_concurrent {
public:
    BLI_concurrent() {
    }

    ~BLI_concurrent() {
        consumer_thread->join();
        delete idx;
        delete write_queue;
    }

    void init(double fill_ratio) {
        idx = new BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE>(fill_ratio);
        write_queue = new tbb::concurrent_queue<std::pair<KeyValue<T, V>, int *>>();
       
        consumer_thread = new std::thread(run, write_queue, idx);
    }

    void bulk_load(std::vector<KeyValue<T, V>> &kvs) {
        idx->bulk_load(kvs);
    }

    bool lookup(const T key, V& value) {
       return idx->lookup(key, value);
    }

    void insert(const KeyValue<T, V> &kv, int *ret) {
        write_queue->push({kv, ret}); // std::promise is not copyable, so we need to use std::move.
        while(*ret == -1){
            std::this_thread::yield();
        }
    }

    void print_lookup_stat(){
        idx->print_lookup_stat();
    }

    void dump() {
        idx->dump();
    }
    
private:
    using PromiseWriteType = std::pair<KeyValue<T, V>, int *>;
    
    tbb::concurrent_queue<PromiseWriteType> *write_queue; // other threads to main thread queue
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx; // TODO: remove runtime stats updates in BLI, to avoid synchronization overhead.

    std::thread *consumer_thread;

    static void run(tbb::concurrent_queue<PromiseWriteType> *write_queue, BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx) {
        std::pair<KeyValue<T, V>, int *> kv_promise_pair;

        while(true){
            if(write_queue->try_pop(kv_promise_pair)){
                auto& [kv, ret_addr] = kv_promise_pair;
                *ret_addr = idx->insert(kv);
            }
            std::this_thread::yield();
        }
    }
};

} // namespace buckindex