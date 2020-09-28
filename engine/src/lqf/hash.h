//
// Created by Harper on 4/20/20.
//

#ifndef ARROW_HASH_H
#define ARROW_HASH_H

#include <cstdint>
#include "lang.h"

namespace lqf {
    namespace hash {

        uint32_t knuth_hash(int32_t v);
        uint32_t knuth_hash(int64_t v);

        uint32_t murmur3_hash(int64_t v);
    }
}

#endif //ARROW_HASH_H
