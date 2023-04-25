#pragma once
#include "linear_model.h"
#include "greedy_error_corridor.h"

#define G_USE_LINEAR_REGRESSION true

namespace buckindex {
    using namespace std;
    template<typename T>
    struct Cut {
        Cut() {
            start_ = 0;
            size_ = 0;
            start_key_ = 0;
            end_key_ = 0;
        }
        Cut(uint64_t offset) {
            start_ = offset;
            size_ = 0;
        }
        Cut(const Cut<T>& c)
            : start_(c.start_),
              size_(c.size_),
              start_key_(c.start_key_),
              end_key_(c.end_key_) {}

        void add_sample(T key) {
            if (size_ == 0) {
                start_key_ = key;
            }
            end_key_ = key;
            size_++;
        }
        void set_start(uint64_t offset) {
            start_ = offset;
        }
        void set_size(uint64_t size) {
            size_ = size;
        }
        LinearModel<T> get_model() {
            if (end_key_ > start_key_) {
                double slope = (double)(size_- 1) / (end_key_ - start_key_);
                double offset = -slope*start_key_;
                return LinearModel<T>(slope, offset);
            }
            return LinearModel<T>();
        }
        uint64_t start_;
        uint64_t size_;
        T start_key_;
        T end_key_;
    };

    template<typename Container, typename KeyType>
    class Segmentation {
    public:
        static void compute_dynamic_segmentation(Container &in_kv_array,
                                                 vector<Cut<KeyType>>& out_cuts, vector<LinearModel<KeyType>> &out_models,
                                                 uint64_t error_bound) {
            GreedyErrorCorridor<KeyType> alg;
            int idx = 0;
            Cut<KeyType> c;
            if (in_kv_array.size() == 0) return;
            typename Container::const_iterator start = in_kv_array.cbegin();
            typename Container::const_iterator end = in_kv_array.cend();

            vector<KeyType> keys;

            alg.init(start->get_key(), error_bound);
            c.add_sample(start->get_key());
            if(G_USE_LINEAR_REGRESSION) keys.push_back(start->get_key());
            idx = 1;
            start++;
            while (start != end) {
                if (alg.is_bounded(start->get_key())) {
                    c.add_sample(start->get_key());
                    keys.push_back(start->get_key());
                } else {
                    out_cuts.push_back(c);
                    if(G_USE_LINEAR_REGRESSION) {
                        out_models.push_back(LinearModel<KeyType>::get_regression_model(keys));
                        keys.clear();
                    } else {
                        out_models.push_back(c.get_model());
                    }
                    alg.init(start->get_key(), error_bound);
                    if (G_USE_LINEAR_REGRESSION) keys.push_back(start->get_key());
                    c = Cut<KeyType>(idx);
                    c.add_sample(start->get_key());
                }
                idx++;
                start++;
            };
            out_cuts.push_back(c);
            if(G_USE_LINEAR_REGRESSION) {
                out_models.push_back(LinearModel<KeyType>::get_regression_model(keys));
                keys.clear();
            } else {
                out_models.push_back(c.get_model());
            }
        }

        static void compute_fixed_segmentation(Container &in_kv_array,
                                               vector<Cut<KeyType>>& out_cuts,
                                               uint64_t size) {
            Cut<KeyType> c(0);
            uint64_t idx = 0;
            uint64_t end_idx = 0;
            typename Container::const_iterator start = in_kv_array.cbegin();
            typename Container::const_iterator end = in_kv_array.cend();

            while (start != end) {
                end_idx = idx + size;
                if (end_idx < in_kv_array.size()) {
                    c.set_size(size);
                    start += size;
                } else {
                    c.set_size(in_kv_array.size() - idx);
                    end_idx = in_kv_array.size();
                    start = end;
                }
                out_cuts.push_back(c);
                idx = end_idx;
                c = Cut<KeyType>(idx);
            }
        }
    };
}
