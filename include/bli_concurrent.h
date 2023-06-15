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
};

/**
 * Concurrent interface of BLI; used by GRE
*/
template<typename T, typename V, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
class BLI_concurrent {
public:
    BLI_concurrent() {
        ThreadArgs<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> args;
        args.write_queue = &write_queue;
        args.idx = &idx;
        pthread_create(&write_worker, nullptr, reinterpret_cast<void* (*)(void*)>(run), &args);
    }

    bool read(const T key, V& value) {
       return idx.lookup(key, value);
    }

    int write(const KeyValue<T, V> &kv, size_t idx) {
        std::promise<int> outstanding_write;
        write_queue.push(make_pair(kv, outstanding_write));
        std::future<int> write_future = outstanding_write.get_future();
        return write_future.get(); // wait for the previous write request to finish before sending another one.
    }
    
private:
    pthread_t write_worker;
    std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> write_queue; // other threads to main thread queue
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> idx;

    static void* run(void *arg_list) {
        ThreadArgs<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *args = 
            reinterpret_cast<ThreadArgs<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE>*>(arg_list);
        std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> *write_queue = args->write_queue;
        BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> *idx = args->idx;
        while(true){
            while (!write_queue->empty()) { // check the O2M_queue for all the write request from all other threads.
                auto& [kv, write_promise] = write_queue->front(); // get the write_request and write_promise from the queue.
                write_queue->pop();
                int M2O_ret = idx->insert(kv);
                write_promise.set_value(M2O_ret); // this will unblock the thread O's write_future.get();
            }
            pthread_yield();
        }
    }
};

} // namespace buckindex