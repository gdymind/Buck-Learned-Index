#include<iostream>

#include "include/global.h"
#include "segment.h"

int main(int argc, char** argv) {
    string cfgfile = argv[1];

    if (Parse(cfgfile)) {
        cerr<< "parse config file " << cfgfile << " failed!\n";
        return -1;
    }

    return 0;
}