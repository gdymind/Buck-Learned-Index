#include<iostream>
#include<climits>

#include "util.h"
#include "buck_index.h"
#include "bli_concurrent.h"
#include "bli_async.h"

typedef unsigned long long key_type;
typedef unsigned long long value_type;

int main(int argc, char** argv) {
    string cfgfile = argv[1];

    // if (Parse(cfgfile)) {
    //     cerr<< "parse config file " << cfgfile << " failed!\n";
    //     return -1;
    // }

    buckindex::BuckIndex<key_type, value_type, 8, 256> index;

    buckindex::Segment<unsigned long long, 8> segment;

    buckindex::Bucket<buckindex::KeyValueList<key_type, value_type, 64>, key_type, value_type, 64> bucket;
    // buckindex::BuckIndex<key_type, value_type, 8, 256> buckindex::BLI_concurrent<unsigned long long, unsigned long long, 8, 256>::idx;
    buckindex::BLI_concurrent<unsigned long long, unsigned long long, 8, 256> bli_concurrent;

    return 0;
}
