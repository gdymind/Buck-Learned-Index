
// Minimum requirement:

// node type: segment group, segment, bucket

// segment group: model, metadata, list of {bucket}

// segment: model, metadata, list of {bucket}

// bucket: metadata, list of {key,ptr}

#include <cstdint>

typedef unsigned long long key_type;
typedef unsigned long long value_type;


const int BUCKET_SIZE = 64;
const int SBUCKET_SIZE = 8;



struct KVPTR
{
    key_type key;
    uint64_t* ptr; // 8 bytes; if it points to bucket/segment/segment group, cast it to correct type
};


struct Model {
    double a;
    double b;
};


struct Bucket
{
    unsigned char write_pos; // size of already occupied slot // can be changed to a bitmap
    key_type pivot; // smallest element
    // key_type base; // key compression


    KVPTR kv_pairs[BUCKET_SIZE];
};

struct SBucket
{
    size_t write_pos; // size of already occupied slot // can be changed to a bitmap
    key_type pivot; // smallest element
    // key_type base; // key compression


    //KV_PTR* kv_pairs;
    KVPTR kv_pairs[SBUCKET_SIZE];
};

struct Segment
{
    Model m;
    size_t num_bucket; // total num of buckets
    key_type pivot; // smallest element
    // key_type base; // key compression

    Bucket* bucket_list;
};

struct SegmentGroup
{
    Model m;
    size_t num_bucket; // total num of buckets
    size_t bucket_size; // size of each bucket
    key_type pivot; // smallest element
    // key_type base; // key compression

    SBucket* bucket_list;
};
