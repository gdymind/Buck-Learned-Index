#include <gtest/gtest.h>
#include "../src/bucket.h"

// In this example, we include the gtest header file and the header file for the code being tested. 
// We define a test case using the TEST macro, which takes two arguments: the name of the test suite (MyTestSuite), 
// and the name of the test case (MyTestCase). 
// Inside the test case, we use the EXPECT_EQ macro to compare the actual output of the function with the expected output.


// To run the tests, you can use the following commands:
// mkdir build
// cd build
// cmake ..
// make
// ctest -V
// The -V flag is used to show verbose output, including the results of each test case.

TEST(MyTestSuite, MyTestCase) {
    // EXPECT_EQ(my_function(1), 2);

}
