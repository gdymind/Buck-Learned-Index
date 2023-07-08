#include <queue>
#include <future>
#include <chrono>
#include <pthread.h>
#include <boost/lockfree/queue.hpp>
#include <atomic_queue/atomic_queue.h>
#include <iostream>
#include <thread>

#include "keyvalue.h"
#include "buck_index.h"

namespace  buckindex{

template <typename T, typename V>
struct KVRet { // Boost's lock-free queue requires trivial copy
    V key;
    V value;
    int* ret;

    KVRet() = default;
    KVRet(const KVRet& other) = default;
    KVRet(KVRet&& other) = default;
    KVRet& operator=(const KVRet& other) = default;
    KVRet& operator=(KVRet&& other) = default;
    ~KVRet() = default;
};

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
        write_queue = new boost::lockfree::queue<KVRet<T, V>>(128);
       
        consumer_thread = new std::thread(run, write_queue, idx);
    }

    void bulk_load(std::vector<KeyValue<T, V>> &kvs) {
        idx->bulk_load(kvs);
    }

    bool lookup(const T key, V& value) {
       return idx->lookup(key, value);
    }

    void insert(const KeyValue<T, V> &kv, int *ret) {
        write_queue->push({kv.key_, kv.value_, ret}); // std::promise is not copyable, so we need to use std::move.
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
    
    boost::lockfree::queue<KVRet<T, V>> *write_queue; // other threads to main thread queue
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx; // TODO: remove runtime stats updates in BLI, to avoid synchronization overhead.

    std::thread *consumer_thread;

    static void run(boost::lockfree::queue<KVRet<T, V>> *write_queue, BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx) {
        KVRet<T, V> kv_ret;

        while(true){ //TODO: add condition variable to wake up the thread when there is a write request.
            if(write_queue->pop(kv_ret)){
                KeyValue kv(kv_ret.key, kv_ret.value);
                *(kv_ret.ret) = idx->insert(kv);
            }
            // std::this_thread::yield();
        }
    }
};

} // namespace buckindex