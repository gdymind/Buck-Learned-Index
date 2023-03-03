#include "gtest/gtest.h"

#include "linear_model.h"

namespace BLI {
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
}
