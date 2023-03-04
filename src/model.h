#pragma once

namespace buckindex {

template<class T>
class Model {
public:
    double a_ = 0.0;
    double b_ = 0.0;
    
    Model() = default;
    Model(double a, double b) : a_(a), b_(b) {}
    Model(const Model& other) : a_(other.a), b_(other.b) {}

    void expand(double expansion_factor) {
        a_ *= expansion_factor;
        b_ *= expansion_factor;
    }
    
    inline unsigned int predict(T key) const {
        return static_cast<int>(a_ * static_cast<double>(key) + b_);
    }

    inline double predict_double(T key) const {
        return a_ * static_cast<double>(key) + b_;
    }
};

}
