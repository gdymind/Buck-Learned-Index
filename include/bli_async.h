#include <queue>
#include <future>
#include <chrono>
#include <thread>

#include "keyvalue.h"
#include "buck_index.h"

namespace  buckindex{

// The Timer class would need to be defined by you to handle the timing of write requests
class Timer {
public:
    void set_timer(int milliseconds) {
        // Implement this method
    }

    bool expired() {
        // Implement this method
    }
};

template<typename T, typename V, size_t SEGMENT_BUCKET_SIZE, size_t DATA_BUCKET_SIZE>
class BLI_async {
    const size_t N_MAX_THREAD = 24;
public:
    BLI_async(size_t n_thread): n_thread_(n_thread) {
        //TODO: call idx.init(fill_ratio);
        for (int i = 0; i < n) {
            int ret = pthread_create(&threads[i], nullptr, run, &i);
        }
    }

private:
    size_t n_thread_ = 0; // number of background threads
    BuckIndex<T, V, SEGMENT_BUCKET_SIZE, DATA_BUCKET_SIZE> idx;
    pthread_t threads[N_MAX_THREAD]; // the first one is the RW thread
    // create n queues of n threads
    std::queue<KeyValue<T, V>> input_queues_[N_MAX_THREAD];
    std::queue<std::pair<KeyValue<T, V>, std::promise<int>>> O2M_queue; // other threads to main thread queue

void run(void *arg) {
    int idx = *((int *)arg);
    std::queue<Request> &input_queue_ = input_queues_[idx];

    std::promise<int> outstanding_write; // Only one outstanding write is allowed at a time
    std::future<int> write_future;
    bool has_outstanding_write;

    Timer timer;

    while (!input_queue_.empty()) {
        Request request = input_queue_.front();
        input_queue_.pop();
        int ret;

        if (request.type == read) {
            ret = idx.read(request);
        } else { // request.type == write
            if (idx == 0) { // main thread
                ret = idx.write(request);
            } else { // all other threads
                if (has_outstanding_write) {
                    write_future = outstanding_write.get_future();
                    int write_request_ret = write_future.get(); // wait for the previous write request to finish before sending another one.
                    has_outstanding_write = false;
                }
                O2M_queue.push({request, std::move(outstanding_write)});
                has_outstanding_write = true;
                timer.set_timer(WRITE_ETA); // Define WRITE_ETA as the timeout for write operations
            }
        }
        if (idx == 0) { // Replace "main" with the thread ID of the main thread
            if (has_outstanding_write && timer.expired()) {
                write_future = outstanding_write.get_future();
                int write_request_ret = write_future.get();
                has_outstanding_write = false;
            }
        }
    }
    if (idx == 0) { // Replace "main" with the thread ID of the main thread
        while (!O2M_queue.empty()) { // check the O2M_queue for all the write request from all other threads.
            auto& [O2M_write_request, O2M_write_promise] = O2M_queue.front(); // get the write_request and write_promise from the queue.
            O2M_queue.pop();
            int M2O_ret = bli_instance.write(O2M_write_request);
            O2M_write_promise.set_value(M2O_ret); // this will unblock the thread O's write_future.get();
        }
    }
}
};

} // namespace buckindex