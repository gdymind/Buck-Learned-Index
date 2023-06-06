#include <queue>
#include <future>
#include <chrono>
#include <thread>

#include "keyvalue.h"
#include "buck_index.h"

namespace  buckindex{


template<typename T, typename V, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
class BLI_concurrent {
public:
    BLI_concurrent(): {
        pthread_create(&write_worker, nullptr, BLI_run, &write_queue);
    }

    bool read(const T key, V& value) {
       return idx.lookup(key, value);
    }

    int write(const KeyValue<T, V> &kv, size_t idx) {
        std::promise<int> outstanding_write;
        write_queue.push(make_pair(request, outstanding_write));
        write_future = outstanding_write.get_future();
        return write_future.get(); // wait for the previous write request to finish before sending another one.
    }
    
private:
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> idx;
    pthread_t write_worker;
    std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> write_queue; // other threads to main thread queue

    static void run(void *arg) {
        std::queue<std::pair<Request, std::promise<V>>> *write_queue = arg;
        while(true){
            while (!write_queue->empty()) { // check the O2M_queue for all the write request from all other threads.
                auto& [kv, write_promise] = O2M_queue.front(); // get the write_request and write_promise from the queue.
                O2M_queue.pop();
                int M2O_ret = idx.insert(kv);
                O2M_write_promise.set_value(M2O_ret); // this will unblock the thread O's write_future.get();
            }
            pthread_yield();
        }
    }
};

} // namespace buckindex