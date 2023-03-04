#pragma once

namespace buckindex {

    /**
     * The greedy error corridor implementation is based of the RadixSpline
     * implementation.  The greedy error corridor algorithm itself is from
     * the Smooth Interpolating Histograms with Error Guarantees paper.
     */
    template<typename T>
    class GreedyErrorCorridor {
    public:
        /**
         * Point structure which consists of a x and y coordinates
         */
        struct Point
        {
            Point(){};
            Point(T x, uint64_t y)
                : x_(x), y_(y) {}
            Point(const Point& p)
                : x_(p.x_), y_(p.y_) {}

            T x_;
            uint64_t y_;
        };
        /**
         * Bound structure is used to track of a Point object is within the
         * bound.  A Point object is within the bound as long as it is within
         * the upper and lower lines from the based point.
         */
        struct Bound
        {
            Bound() {};
            Bound(const Point& base, const Point& next_point, uint64_t bound) {
                bound_ = bound;
                base_ = base;
                upper_ = Point(next_point.x_, next_point.y_ + bound);
                lower_ = Point(next_point.x_, 0);
            }
            Bound(const Bound& b)
                : bound_(b.bound_),
                  upper_(b.upper_),
                  lower_(b.lower_),
                  base_(b.base_){}
            Bound(uint64_t bound)
                : bound_(bound){
            }
            void set_basepoint(Point base) {
                base_ = base;
            }
            void set_bound(uint64_t bound) {
                bound_ = bound;
            }
            uint64_t get_bound() { return bound_; }
            void set_nextpoint(Point next) {
                upper_ = Point(next.x_, next.y_ + bound_);
                lower_ = Point(next.x_, 0);
            }
            uint64_t bound_;
            Point upper_;
            Point lower_;
            Point base_;
        };

        enum Orientation {
            COLINEAR = 0,
            CW = 1,
            CCW = 2
        };

        Orientation compute_orientation(double dx1, double dy1, double dx2, double dy2) {
            double expr = dy1*dx2 - dy2*dx1;
            if (expr > 0) {
                return CW;
            } else if (expr < 0) {
                return CCW;
            }
            return COLINEAR;
        }
        GreedyErrorCorridor() {};
        void init(T base, uint64_t error_bound) {
            bound_.set_basepoint(Point(base,0));
            bound_.set_bound(error_bound);
            y_ = 0;
        }
        bool is_bounded(T key) {
            if (y_ == 0) {
                y_++;
                bound_.set_nextpoint(Point(key, y_));
                return true;
            }
            y_++;
            uint64_t upper_y = y_ + bound_.get_bound();
            uint64_t lower_y =  0;
            if (y_ > bound_.get_bound()) {
                lower_y = y_ - bound_.get_bound();
            }
            uint64_t delta_x_upper_limit = bound_.upper_.x_ - bound_.base_.x_;
            uint64_t delta_x_lower_limit = bound_.lower_.x_ - bound_.base_.x_;
            uint64_t delta_x = key - bound_.base_.x_;
            uint64_t delta_y_upper_limit = bound_.upper_.y_ - bound_.base_.y_;
            uint64_t delta_y_lower_limit = bound_.lower_.y_ - bound_.base_.y_;
            uint64_t delta_y = y_ - bound_.base_.y_;

            if ((compute_orientation(delta_x_upper_limit, delta_y_upper_limit,
                                    delta_x, delta_y)!=CW) ||
                (compute_orientation(delta_x_lower_limit, delta_y_lower_limit,
                                    delta_x, delta_y)!=CCW)) {
                return false;
            }
            uint64_t delta_y_upper = upper_y - bound_.base_.y_;
            if (compute_orientation(delta_x_upper_limit, delta_y_upper_limit,
                                   delta_x, delta_y_upper) == CW) {
                bound_.upper_ = Point(key, upper_y);
            }
            uint64_t delta_y_lower = lower_y - bound_.base_.y_;
            if (compute_orientation(delta_x_lower_limit, delta_y_lower_limit,
                                   delta_x, delta_y_lower) == CCW) {
                bound_.lower_ = Point(key, lower_y);
            }
            return true;
        }

    private:
        Bound bound_;
        uint64_t y_;
    };
}
