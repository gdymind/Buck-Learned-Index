
// Minimum requirement:

// node type: segment group, segment, bucket

// segment group: model, metadata, list of {bucket}

// segment: model, metadata, list of {bucket}

// bucket: metadata, list of {key,ptr}

#include <cstdint>

#define KEY_TYPE unsigned long long 
#define VALUE_TYPE unsigned long long 

#define SIZE 64


struct KV_PTR
{
    KEY_TYPE key;
    uint64_t* ptr; // 8 bytes; if it points to bucket/segment/segment group, cast it to correct type
};


struct model {
    double a;
    double b;
};


struct bucket
{
    bool bsucket_type; //0: d-bucket; 1:s-bucket // differentiate them
    //size_t capacity; //total size of bucket // fixed size
    size_t write_pos; // size of already occupied slot // can be changed to a bitmap
    KEY_TYPE pivot; // smallest element
    // KEY_TYPE base; // key compression


    KV_PTR kv_pairs[SIZE];
    // KEY_TYPE* keys;
    // uint64_t** ptrs;
};

struct s-bucket
{
    bool bsucket_type; //0: d-bucket; 1:s-bucket // differentiate them; Why?
    size_t capacity; //total size of bucket
    size_t write_pos; // size of already occupied slot // can be changed to a bitmap
    KEY_TYPE pivot; // smallest element
    // KEY_TYPE base; // key compression


    KV_PTR* kv_pairs;
    // KEY_TYPE* keys;
    // uint64_t** ptrs;
};

struct segment
{
    bool segement_type; //0: segment; 1: segment group
                        // if it is segment, it points to d-bucket
                        // else, it points to segment/segment groups
    model m;
    size_t num_bucket; // total num of buckets
    size_t bucket_size; // size of each bucket
    KEY_TYPE pivot; // smallest element
    // KEY_TYPE base; // key compression

    //bucket* bucket_list;
    // array of anchors, should include all element in all s-buckets
    KEY_PTR* key_anchor_list;
    // KEY_TYPE* keys;
    // uint64_t** ptrs;
};

