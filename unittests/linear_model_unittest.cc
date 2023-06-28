#include <cmath>

#include "gtest/gtest.h"
#include "linear_model.h"

#define BUCKINDEX_DEBUG


namespace buckindex {
    TEST(LinearModel, predict) {
        LinearModel<uint64_t> m(0,0);
        uint64_t key = 100;
        uint64_t pos = 0;
        pos = m.predict(key);
        EXPECT_EQ(0u, pos);

        LinearModel<uint64_t> m2(1,0);
        pos = m2.predict(key);
        EXPECT_EQ(100u, pos);

        LinearModel<uint64_t> m3(1,-100);
        pos = m3.predict(key);
        EXPECT_EQ(0u, pos);
    }
    TEST(LinearModel, expand) {
        LinearModel<uint64_t> m(1,1);
        uint64_t key = 1;
        uint64_t pos = 0;
        m.expand(2);
        pos = m.predict(key);
        EXPECT_EQ(4u, pos);
    }

    TEST(LinearModel, get_endpoints_model) {
        LinearModel<uint64_t> m = LinearModel<uint64_t>::get_endpoints_model({0,2,4,6,8});
        EXPECT_NEAR(m.get_slope(), 0.5, 1e-6);
        EXPECT_NEAR(m.get_offset(),  0.0, 1e-6);

        LinearModel<uint64_t> m2 = LinearModel<uint64_t>::get_endpoints_model({10, 26});
        EXPECT_NEAR(m2.get_slope(), 1.0/16, 1e-6);
        EXPECT_NEAR(m2.get_offset(), -10.0/16, 1e-6);

        LinearModel<uint64_t> m3 = LinearModel<uint64_t>::get_endpoints_model({10, 100});
        EXPECT_NEAR(m3.get_slope(), 1.0/90, 1e-6);
        EXPECT_NEAR(m3.get_offset(), -1.0/9, 1e-6);
    }

    TEST(LinearModel, get_regression_model) {
        LinearModel<uint64_t> m = LinearModel<uint64_t>::get_regression_model({0,2,4,6,8});
        EXPECT_NEAR(m.get_slope(), 0.5, 1e-6);
        EXPECT_NEAR(m.get_offset(), 0.0, 1e-6);

        LinearModel<uint64_t> m2 = LinearModel<uint64_t>::get_regression_model({10, 26});
        EXPECT_NEAR(m2.get_slope(), 1.0/16, 1e-6);
        EXPECT_NEAR(m2.get_offset(), -10.0/16, 1e-6);

        LinearModel<uint64_t> m3 = LinearModel<uint64_t>::get_regression_model({10, 17, 34, 38, 55, 66, 71, 82, 100});
        EXPECT_NEAR(m3.get_slope(), 0.09029, 1e-3);
        EXPECT_NEAR(m3.get_offset(), -0.7455, 1e-3);
    }
}
