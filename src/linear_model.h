#pragma once

namespace buckindex {
    /**
     * Linear model class
     * The linear model contains the slope and offset parameters
     */
    template <typename T>
    class __attribute__((__packed__)) LinearModel {
    public:
        LinearModel()
            :slope_(0), offset_(0)
            {}
        LinearModel(const LinearModel& m)
            :slope_(m.slope_), offset_(m.offset_)
            {}
        LinearModel(double slope, double offset)
            :slope_(slope), offset_(offset)
            {}

        static LinearModel<T> get_endpoints_model(const std::vector<T>& keys) {
            assert(keys.size() > 0);
            T start_key = keys[0];
            T end_key = keys[keys.size() - 1];
            assert(end_key >= start_key);
            if (end_key > start_key) {
                double slope = (long double)(keys.size() - 1) / (long double)(end_key - start_key);
                double offset = -slope * start_key;
                return LinearModel<T>(slope, offset);
            }
            return LinearModel<T>();
        }

        static LinearModel<T> get_regression_model(const std::vector<T>& keys) {
            if (keys.size() < 2 || keys[keys.size() - 1] == keys[0]) {
                return LinearModel<T>();
            }

            long double sum_x = 0;
            long double sum_y = 0;
            long double sum_xy = 0;
            long double sum_xx = 0;
            long double sum_yy = 0;
            for (size_t i = 0; i < keys.size(); i++) {
                sum_x += keys[i];
                sum_y += (long double)i;
                sum_xy += (long double)keys[i]*i;
                sum_xx += (long double)keys[i]*keys[i];
                sum_yy += (long double)i*i;
            }
            long double count = keys.size();
            double slope = (count*sum_xy - sum_x*sum_y) / (count*sum_xx - sum_x*sum_x);
            double offset = ((long double)sum_y - slope*sum_x) / count;

            // If floating point precision errors, fit spline
            if (slope <= 0) {
                return get_endpoints_model(keys);
            }
            return LinearModel<T>(slope, offset);
        }

        /**
         * Using the linear regression model, given a key, the function
         * returns the approximate location within the trained set.
         * @param key: user provided key
         * @return approximate location of the key within the trained set.
         */
        uint64_t predict(T key) const {
            double pos = slope_*key+offset_;
            if (pos < 0) {
                return 0;
            }
            return (uint64_t)pos;
        }
        /**
         * Scale the model
         * @param expand_ratio: scaling ratio
         */
        void expand(double expand_ratio) {
            slope_ *= expand_ratio;
            offset_ *= expand_ratio;
        }
        /**
         * Accessor of the internal slope parameter
         * @return the slope parameter
         */
        double get_slope() const {
            return slope_;
        }
        /**
         * Accessor of the internal offset parameter
         * @return the offset parameter
         */
        double get_offset() const {
            return offset_;
        }
        /**
         * Indicate whether the regression model is valid.  The learned index
         * requires the model to have positive slope
         *
         * @return true if the model is valid
         */
        inline bool is_valid() {
            return (slope_ > std::numeric_limits<double>::min());
        }
        /**
         * Helper function to dump the current model parameters
         */
        void dump() const {
            std::cout << "(slope, offset) : " << slope_ << "," <<offset_<<std::endl;
        }
    private:
        double slope_;
        double offset_;
    };
}
