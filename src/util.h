#pragma once

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

//https://github.com/lemire/clhash
/*
 * CLHash is a very fast hashing function that uses the
 * carry-less multiplication and SSE instructions.
 *
 * Daniel Lemire, Owen Kaser, Faster 64-bit universal hashing
 * using carry-less multiplications, Journal of Cryptographic Engineering (to appear)
 *
 * Best used on recent x64 processors (Haswell or better).
 *
 * Compile option: if you define BITMIX during compilation, extra work is done to
 * pass smhasher's avalanche test succesfully. Disabled by default.
 **/

#ifndef INCLUDE_CLHASH_H_
#define INCLUDE_CLHASH_H_


#include <stdlib.h>
#include <stdint.h> // life is short, please use a C99-compliant compiler
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {RANDOM_64BITWORDS_NEEDED_FOR_CLHASH=133,RANDOM_BYTES_NEEDED_FOR_CLHASH=133*8};



/**
 *  random : the random data source (should contain at least
 *  RANDOM_BYTES_NEEDED_FOR_CLHASH random bytes), it should
 *  also be aligned on 16-byte boundaries so that (((uintptr_t) random & 15) == 0)
 *  for performance reasons. This is usually generated once and reused with many
 *  inputs.
 *
 *
 * stringbyte : the input data source, could be anything you want to has
 *
 *
 * length : number of bytes in the string
 */
uint64_t clhash(const void* random, const char * stringbyte,
                const size_t lengthbyte);



/**
 * Convenience method. Will generate a random key from two 64-bit seeds.
 * Caller is responsible to call "free" on the result.
 */
void * get_random_key_for_clhash(uint64_t seed1, uint64_t seed2);

#ifdef __cplusplus
} // extern "C"
#endif

#ifdef __cplusplus
#include <vector>
#include <string>
#include <cstring> // For std::strlen

struct clhasher {
    const void *random_data_;
    clhasher(uint64_t seed1=137, uint64_t seed2=777): random_data_(get_random_key_for_clhash(seed1, seed2)) {}
    template<typename T>
    uint64_t operator()(const T *data, const size_t len) const {
        return clhash(random_data_, (const char *)data, len * sizeof(T));
    }
    uint64_t operator()(const char *str) const {return operator()(str, std::strlen(str));}
    template<typename T>
    uint64_t operator()(const T &input) const {
        return operator()((const char *)&input, sizeof(T));
    }
    template<typename T>
    uint64_t operator()(const std::vector<T> &input) const {
        return operator()((const char *)input.data(), sizeof(T) * input.size());
    }
    uint64_t operator()(const std::string &str) const {
        return operator()(str.data(), str.size());
    }
    ~clhasher() {
        std::free((void *)random_data_);
    }
};
#endif // #ifdef __cplusplus

#endif /* INCLUDE_CLHASH_H_ */




#include <assert.h>
#include <string.h>
#include <x86intrin.h>

#ifdef __WIN32
#define posix_memalign(p, a, s) (((*(p)) = _aligned_malloc((s), (a))), *(p) ?0 :errno)
#endif



// computes a << 1
static inline __m128i leftshift1(__m128i a) {
    const int x = 1;
    __m128i u64shift =  _mm_slli_epi64(a,x);
    __m128i topbits =  _mm_slli_si128(_mm_srli_epi64(a,64 - x),sizeof(uint64_t));
    return _mm_or_si128(u64shift, topbits);
}

// computes a << 2
static inline __m128i leftshift2(__m128i a) {
    const int x = 2;
    __m128i u64shift =  _mm_slli_epi64(a,x);
    __m128i topbits =  _mm_slli_si128(_mm_srli_epi64(a,64 - x),sizeof(uint64_t));
    return _mm_or_si128(u64shift, topbits);
}

//////////////////
// compute the "lazy" modulo with 2^127 + 2 + 1, actually we compute the
// modulo with (2^128 + 4 + 2) = 2 * (2^127 + 2 + 1) ,
// though  (2^128 + 4 + 2) is not
// irreducible, we have that
//     (x mod (2^128 + 4 + 2)) mod (2^127 + 2 + 1) == x mod (2^127 + 2 + 1)
// That's true because, in general ( x mod k y ) mod y = x mod y.
//
// Precondition:  given that Ahigh|Alow represents a 254-bit value
//                  (two highest bits of Ahigh must be zero)
//////////////////
static inline __m128i lazymod127(__m128i Alow, __m128i Ahigh) {
    ///////////////////////////////////////////////////
    // CHECKING THE PRECONDITION:
    // Important: we are assuming that the two highest bits of Ahigh
    // are zero. This could be checked by adding a line such as this one:
    // if(_mm_extract_epi64(Ahigh,1) >= (1ULL<<62)){printf("bug\n");abort();}
    //                       (this assumes SSE4.1 support)
    ///////////////////////////////////////////////////
    // The answer is Alow XOR  (  Ahigh <<1 ) XOR (  Ahigh <<2 )
    // This is correct because the two highest bits of Ahigh are
    // assumed to be zero.
    ///////////////////////////////////////////////////
    // credit for simplified implementation : Jan Wassenberg
    __m128i shift1 = leftshift1(Ahigh);
    __m128i shift2 = leftshift2(Ahigh);
    __m128i final =  _mm_xor_si128(_mm_xor_si128(Alow, shift1),shift2);
    return final;
}


// multiplication with lazy reduction
// assumes that the two highest bits of the 256-bit multiplication are zeros
// returns a lazy reduction
static inline  __m128i mul128by128to128_lazymod127( __m128i A, __m128i B) {
    __m128i Amix1 = _mm_clmulepi64_si128(A,B,0x01);
    __m128i Amix2 = _mm_clmulepi64_si128(A,B,0x10);
    __m128i Alow = _mm_clmulepi64_si128(A,B,0x00);
    __m128i Ahigh = _mm_clmulepi64_si128(A,B,0x11);
    __m128i Amix = _mm_xor_si128(Amix1,Amix2);
    Amix1 = _mm_slli_si128(Amix,8);
    Amix2 = _mm_srli_si128(Amix,8);
    Alow = _mm_xor_si128(Alow,Amix1);
    Ahigh = _mm_xor_si128(Ahigh,Amix2);
    return lazymod127(Alow, Ahigh);
}




// multiply the length and the some key, no modulo
static __m128i lazyLengthHash(uint64_t keylength, uint64_t length) {
    const __m128i lengthvector = _mm_set_epi64x(keylength,length);
    const __m128i clprod1  = _mm_clmulepi64_si128( lengthvector, lengthvector, 0x10);
    return clprod1;
}


// modulo reduction to 64-bit value. The high 64 bits contain garbage, see precompReduction64
static inline __m128i precompReduction64_si128( __m128i A) {

    //const __m128i C = _mm_set_epi64x(1U,(1U<<4)+(1U<<3)+(1U<<1)+(1U<<0)); // C is the irreducible poly. (64,4,3,1,0)
    const __m128i C = _mm_cvtsi64_si128((1U<<4)+(1U<<3)+(1U<<1)+(1U<<0));
    __m128i Q2 = _mm_clmulepi64_si128( A, C, 0x01);
    __m128i Q3 = _mm_shuffle_epi8(_mm_setr_epi8(0, 27, 54, 45, 108, 119, 90, 65, (char)216, (char)195, (char)238, (char)245, (char)180, (char)175, (char)130, (char)153),
                                  _mm_srli_si128(Q2,8));
    __m128i Q4 = _mm_xor_si128(Q2,A);
    const __m128i final = _mm_xor_si128(Q3,Q4);
    return final;/// WARNING: HIGH 64 BITS CONTAIN GARBAGE
}


static inline uint64_t precompReduction64( __m128i A) {
    return _mm_cvtsi128_si64(precompReduction64_si128(A));
}


// hashing the bits in value using the keys key1 and key2 (only the first 64 bits of key2 are used).
// This is basically (a xor k1) * (b xor k2) mod p with length component
static uint64_t simple128to64hashwithlength( const __m128i value, const __m128i key, uint64_t keylength, uint64_t length) {
    const __m128i add =  _mm_xor_si128 (value,key);
    const __m128i clprod1  = _mm_clmulepi64_si128( add, add, 0x10);
    const __m128i total = _mm_xor_si128 (clprod1,lazyLengthHash(keylength, length));
    return precompReduction64(total);
}


enum {CLHASH_DEBUG=0};

// For use with CLHASH
// we expect length to have value 128 or, at least, to be divisible by 4.
static __m128i __clmulhalfscalarproductwithoutreduction(const __m128i * randomsource, const uint64_t * string,
        const size_t length) {
    assert(((uintptr_t) randomsource & 15) == 0);// we expect cache line alignment for the keys
    // we expect length = 128, so we need  16 cache lines of keys and 16 cache lines of strings.
    if(CLHASH_DEBUG) assert((length & 3) == 0); // if not, we need special handling (omitted)
    const uint64_t * const endstring = string + length;
    __m128i acc = _mm_setzero_si128();
    // we expect length = 128
    for (; string + 3 < endstring; randomsource += 2, string += 4) {
        const __m128i temp1 = _mm_load_si128( randomsource);
        const __m128i temp2 = _mm_lddqu_si128((__m128i *) string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
        const __m128i temp12 = _mm_load_si128(randomsource + 1);
        const __m128i temp22 = _mm_lddqu_si128((__m128i *) (string + 2));
        const __m128i add12 = _mm_xor_si128(temp12, temp22);
        const __m128i clprod12 = _mm_clmulepi64_si128(add12, add12, 0x10);
        acc = _mm_xor_si128(clprod12, acc);
    }
    if(CLHASH_DEBUG) assert(string == endstring);
    return acc;
}



// the value length does not have to be divisible by 4
static __m128i __clmulhalfscalarproductwithtailwithoutreduction(const __m128i * randomsource,
        const uint64_t * string, const size_t length) {
    assert(((uintptr_t) randomsource & 15) == 0);// we expect cache line alignment for the keys
    const uint64_t * const endstring = string + length;
    __m128i acc = _mm_setzero_si128();
    for (; string + 3 < endstring; randomsource += 2, string += 4) {
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_lddqu_si128((__m128i *) string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
        const __m128i temp12 = _mm_load_si128(randomsource+1);
        const __m128i temp22 = _mm_lddqu_si128((__m128i *) (string + 2));
        const __m128i add12 = _mm_xor_si128(temp12, temp22);
        const __m128i clprod12 = _mm_clmulepi64_si128(add12, add12, 0x10);
        acc = _mm_xor_si128(clprod12, acc);
    }
    if (string + 1 < endstring) {
        if(CLHASH_DEBUG) assert(length != 128);
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_lddqu_si128((__m128i *) string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
        randomsource += 1;
        string += 2;
    }
    if (string < endstring) {
        if(CLHASH_DEBUG) assert(length != 128);
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_loadl_epi64((__m128i const*)string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
        if(CLHASH_DEBUG) ++string;
    }
    if(CLHASH_DEBUG) assert(string == endstring);
    return acc;
}
// the value length does not have to be divisible by 4
static __m128i __clmulhalfscalarproductwithtailwithoutreductionWithExtraWord(const __m128i * randomsource,
        const uint64_t * string, const size_t length, const uint64_t extraword) {
    assert(((uintptr_t) randomsource & 15) == 0);// we expect cache line alignment for the keys
    const uint64_t * const endstring = string + length;
    __m128i acc = _mm_setzero_si128();
    for (; string + 3 < endstring; randomsource += 2, string += 4) {
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_lddqu_si128((__m128i *) string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
        const __m128i temp12 = _mm_load_si128(randomsource+1);
        const __m128i temp22 = _mm_lddqu_si128((__m128i *) (string + 2));
        const __m128i add12 = _mm_xor_si128(temp12, temp22);
        const __m128i clprod12 = _mm_clmulepi64_si128(add12, add12, 0x10);
        acc = _mm_xor_si128(clprod12, acc);
    }
    if (string + 1 < endstring) {
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_lddqu_si128((__m128i *) string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
        randomsource += 1;
        string += 2;
    }
    // we have to append an extra 1
    if (string < endstring) {
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_set_epi64x(extraword,*string);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x10);
        acc = _mm_xor_si128(clprod1, acc);
    } else {
        const __m128i temp1 = _mm_load_si128(randomsource);
        const __m128i temp2 = _mm_loadl_epi64((__m128i const*)&extraword);
        const __m128i add1 = _mm_xor_si128(temp1, temp2);
        const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x01);
        acc = _mm_xor_si128(clprod1, acc);
    }
    return acc;
}


static __m128i __clmulhalfscalarproductOnlyExtraWord(const __m128i * randomsource,
        const uint64_t extraword) {
    const __m128i temp1 = _mm_load_si128(randomsource);
    const __m128i temp2 = _mm_loadl_epi64((__m128i const*)&extraword);
    const __m128i add1 = _mm_xor_si128(temp1, temp2);
    const __m128i clprod1 = _mm_clmulepi64_si128(add1, add1, 0x01);
    return clprod1;
}




#ifdef BITMIX
////////
// an invertible function used to mix the bits
// borrowed directly from murmurhash
////////
inline uint64_t fmix64 ( uint64_t k ) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}
#endif



// there always remain an incomplete word that has 1,2, 3, 4, 5, 6, 7 used bytes.
// we append 0s to it
static inline uint64_t createLastWord(const size_t lengthbyte, const uint64_t * lastw) {
    const int significantbytes = lengthbyte % sizeof(uint64_t);
    uint64_t lastword = 0;
    memcpy(&lastword,lastw,significantbytes); // could possibly be faster?
    return lastword;
}




uint64_t clhash(const void* random, const char * stringbyte,
                const size_t lengthbyte) {
    assert(sizeof(size_t)<=sizeof(uint64_t));// otherwise, we need to worry
    assert(((uintptr_t) random & 15) == 0);// we expect cache line alignment for the keys
    const unsigned int  m = 128;// we process the data in chunks of 16 cache lines
    if(CLHASH_DEBUG) assert((m  & 3) == 0); //m should be divisible by 4
    const int m128neededperblock = m / 2;// that is how many 128-bit words of random bits we use per block
    const __m128i * rs64 = (__m128i *) random;
    __m128i polyvalue =  _mm_load_si128(rs64 + m128neededperblock); // to preserve alignment on cache lines for main loop, we pick random bits at the end
    polyvalue = _mm_and_si128(polyvalue,_mm_setr_epi32(0xFFFFFFFF,0xFFFFFFFF,0xFFFFFFFF,0x3fffffff));// setting two highest bits to zero
    // we should check that polyvalue is non-zero, though this is best done outside the function and highly unlikely
    const size_t length = lengthbyte / sizeof(uint64_t); // # of complete words
    const size_t lengthinc = (lengthbyte + sizeof(uint64_t) - 1) / sizeof(uint64_t); // # of words, including partial ones

    const uint64_t * string = (const uint64_t *)  stringbyte;
    if (m < lengthinc) { // long strings // modified from length to lengthinc to address issue #3 raised by Eik List
        __m128i  acc =  __clmulhalfscalarproductwithoutreduction(rs64, string,m);
        size_t t = m;
        for (; t +  m <= length; t +=  m) {
            // we compute something like
            // acc+= polyvalue * acc + h1
            acc =  mul128by128to128_lazymod127(polyvalue,acc);
            const __m128i h1 =  __clmulhalfscalarproductwithoutreduction(rs64, string+t,m);
            acc = _mm_xor_si128(acc,h1);
        }
        const int remain = length - t;  // number of completely filled words

        if (remain != 0) {
            // we compute something like
            // acc+= polyvalue * acc + h1
            acc = mul128by128to128_lazymod127(polyvalue, acc);
            if (lengthbyte % sizeof(uint64_t) == 0) {
                const __m128i h1 =
                    __clmulhalfscalarproductwithtailwithoutreduction(rs64,
                            string + t, remain);
                acc = _mm_xor_si128(acc, h1);
            } else {
                const uint64_t lastword = createLastWord(lengthbyte,
                                          (string + length));
                const __m128i h1 =
                    __clmulhalfscalarproductwithtailwithoutreductionWithExtraWord(
                        rs64, string + t, remain, lastword);
                acc = _mm_xor_si128(acc, h1);
            }
        } else if (lengthbyte % sizeof(uint64_t) != 0) {// added to address issue #2 raised by Eik List
            // there are no completely filled words left, but there is one partial word.
            acc = mul128by128to128_lazymod127(polyvalue, acc);
            const uint64_t lastword = createLastWord(lengthbyte, (string + length));
            const __m128i h1 = __clmulhalfscalarproductOnlyExtraWord( rs64, lastword);
            acc = _mm_xor_si128(acc, h1);
        }

        const __m128i finalkey = _mm_load_si128(rs64 + m128neededperblock + 1);
        const uint64_t keylength = *(const uint64_t *)(rs64 + m128neededperblock + 2);
        return simple128to64hashwithlength(acc,finalkey,keylength, (uint64_t)lengthbyte);
    } else { // short strings
        if(lengthbyte % sizeof(uint64_t) == 0) {
            __m128i  acc = __clmulhalfscalarproductwithtailwithoutreduction(rs64, string, length);
            const uint64_t keylength = *(const uint64_t *)(rs64 + m128neededperblock + 2);
            acc = _mm_xor_si128(acc,lazyLengthHash(keylength, (uint64_t)lengthbyte));
#ifdef BITMIX
            return fmix64(precompReduction64(acc)) ;
#else
            return precompReduction64(acc) ;
#endif
        }
        const uint64_t lastword = createLastWord(lengthbyte, (string + length));
        __m128i acc = __clmulhalfscalarproductwithtailwithoutreductionWithExtraWord(
                          rs64, string, length, lastword);
        const uint64_t keylength =  *(const uint64_t *)(rs64 + m128neededperblock + 2);
        acc = _mm_xor_si128(acc,lazyLengthHash(keylength, (uint64_t)lengthbyte));
#ifdef BITMIX
        return fmix64(precompReduction64(acc)) ;
#else
        return precompReduction64(acc) ;
#endif
    }
}


/***************************
 * Rest is optional random-number generation stuff
 */


/* Keys for scalar xorshift128. Must be non-zero
These are modified by xorshift128plus.
 */
struct xorshift128plus_key_s {
    uint64_t part1;
    uint64_t part2;
};

typedef struct xorshift128plus_key_s xorshift128plus_key_t;



/**
*
* You can create a new key like so...
*   xorshift128plus_key_t mykey;
*   xorshift128plus_init(324, 4444,&mykey);
*
* or directly if you prefer:
*  xorshift128plus_key_t mykey = {.part1=324,.part2=4444}
*
*  Then you can generate random numbers like so...
*      xorshift128plus(&mykey);
* If your application is threaded, each thread should have its own
* key.
*
*
* The seeds (key1 and key2) should be non-zero. You are responsible for
* checking that they are non-zero.
*
*/
static inline void xorshift128plus_init(uint64_t key1, uint64_t key2, xorshift128plus_key_t *key) {
    key->part1 = key1;
    key->part2 = key2;
}


/*
Return a new 64-bit random number
*/
uint64_t xorshift128plus(xorshift128plus_key_t * key) {
    uint64_t s1 = key->part1;
    const uint64_t s0 = key->part2;
    key->part1 = s0;
    s1 ^= s1 << 23; // a
    key->part2 = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5); // b, c
    return key->part2 + s0;
}

void * get_random_key_for_clhash(uint64_t seed1, uint64_t seed2) {
    xorshift128plus_key_t k;
    xorshift128plus_init(seed1, seed2, &k);
    void * answer;
    if (posix_memalign(&answer, sizeof(__m128i),
                       RANDOM_BYTES_NEEDED_FOR_CLHASH)) {
        return NULL;
    }
    uint64_t * a64 = (uint64_t *) answer;
    for(uint32_t i = 0; i < RANDOM_64BITWORDS_NEEDED_FOR_CLHASH; ++i) {
        a64[i] =  xorshift128plus(&k);
    }
    while((a64[128]==0) && (a64[129]==1)) {
        a64[128] =  xorshift128plus(&k);
        a64[129] =  xorshift128plus(&k);
    }
    return answer;


}



