#pragma once

namespace BLI {
    /**
     * Linear model class
     * The linear model contains the slope and offset parameters
     */
    template <typename KeyType>
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
        /**
         * Using the linear regression model, given a key, the function
         * returns the approximate location within the trained set.
         * @param key: user provided key
         * @return approximate location of the key within the trained set.
         */
        uint64_t predict(KeyType key) {
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
        double get_slope() {
            return slope_;
        }
        /**
         * Accessor of the internal offset parameter
         * @return the offset parameter
         */
        double get_offset() {
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
        void dump() {
            std::cout << "(slope, offset) : " << slope_ << "," <<offset_<<std::endl;
        }
    private:
        double slope_;
        double offset_;
    };
}
