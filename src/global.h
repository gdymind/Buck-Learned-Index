#ifndef __GLOBAL_H_
#define __GLOBAL_H_

#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <list>
#include <cmath>
#include <set>
#include <unordered_set>
#include <queue>
#include <cstdlib>
using namespace std;

#define PRIMER_CAPACITY 736

extern string g_data_path;
extern bool g_bulk_load;
extern float g_read_ratio;

int Parse(string cfgfile);

typedef std::uint64_t hash_t;
constexpr hash_t prime = 0x100000001B3ull;
constexpr hash_t basis = 0xCBF29CE484222325ull;
constexpr hash_t hash_(char const* str, hash_t last_value = basis)
{
    return *str ? hash_(str+1, (*str ^ last_value) * prime) : last_value;
}

#endif //__GLOBAL_H_