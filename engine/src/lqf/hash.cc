//
// Created by Harper on 4/20/20.
//

#include "hash.h"

namespace lqf {
    namespace hash {
        uint32_t knuth_hash(int32_t v) {
            return v * UINT32_C(2654435769);
            // do not comment about the lack of right shift. I'm not ignoring it. read on.
        }

        uint32_t knuth_hash(int64_t v) {
            uint64_t h = static_cast<uint64_t>(v);
            uint32_t lower = h & 0xFFFFFFFF;
            uint32_t higher = (h >> 32);
            return lower * UINT32_C(2654435769) + higher * UINT32_C(0x7ed558cd);
            // do not comment about the lack of right shift. I'm not ignoring it. read on.
        }

        uint32_t murmur3_hash(int64_t v) {
            uint64_t h = static_cast<uint64_t>(v);
            h ^= h >> 33;
            h *= 0xff51afd7ed558ccdL;
            h ^= h >> 33;
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= h >> 33;
            return h;
        }
    }
}