# BuckIndex

## Install Google's unit test tool gtest
```
 sudo apt install libgtest-dev
```

## Test a single case
```
./unittests/unittests --gtest_filter=TestName.TestCaseName

./unittests/unittests --gtest_filter=Bucket.insert_with_hint
#example
```

./unittests/unittests --gtest_filter=BuckIndex.insert_random_order > out1.txt