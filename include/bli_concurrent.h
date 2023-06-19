#include <queue>
#include <future>
#include <chrono>
#include <pthread.h>

#include "keyvalue.h"
#include "buck_index.h"

namespace  buckindex{

template<typename T, typename V, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
struct ThreadArgs {
    std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> *write_queue;
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx;
    std::mutex *write_queue_mutex; // std::queue is not thread safe
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
        pthread_cancel(write_worker);
    }

    void init(double fill_ratio) {
        idx.init(fill_ratio);

       
        args.write_queue = &write_queue;
        args.idx = &idx;
        args.write_queue_mutex = &write_queue_mutex;
        pthread_create(&write_worker, nullptr, reinterpret_cast<void* (*)(void*)>(run), &args);
    }

    void bulk_load(std::vector<KeyValue<T, V>> &kvs) {
        idx.bulk_load(kvs);
    }

    bool lookup(const T key, V& value) {
       return idx.lookup(key, value);
    }

    int insert(const KeyValue<T, V> &kv) {
        std::promise<int> outstanding_write;
        std::future<int> write_future = outstanding_write.get_future();
        {
            std::lock_guard<std::mutex> lock(write_queue_mutex);
            write_queue.push(make_pair(kv, std::move(outstanding_write))); // std::promise does not support copying
        }
        return write_future.get(); // wait for the previous write request to finish before sending another one.
    }

    void print_lookup_stat(){
        idx.print_lookup_stat();
    }

    void dump() {
        idx.dump();
    }
    
private:
    ThreadArgs<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> args;
    pthread_t write_worker;
    std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> write_queue; // other threads to main thread queue
    std::mutex write_queue_mutex; // std::queue is not thread safe
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> idx;

    static void* run(void *arg_list) {
        ThreadArgs<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *args = 
            reinterpret_cast<ThreadArgs<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE>*>(arg_list);
        std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> *write_queue = args->write_queue;
        std::mutex *write_queue_mutex = args->write_queue_mutex;
        BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx = args->idx;

        while(true){ //TODO: add condition variable to wake up the thread when there is a write request.
            std::pair<KeyValue<T, V>, std::promise<int>> kv_promise_pair;
            {
                std::lock_guard<std::mutex> lock(*write_queue_mutex);
                if (write_queue->empty()) {
                    pthread_yield();
                    continue;
                }
                kv_promise_pair = std::move(write_queue->front()); // get the write_request and write_promise from the queue.
                write_queue->pop();
            }

            auto& [kv, write_promise] = kv_promise_pair;
            int M2O_ret = idx->insert(kv);
            write_promise.set_value(M2O_ret); // this will unblock the thread O's write_future.get();

            pthread_yield();
        }

        return nullptr;
    }
};

} // namespace buckindex