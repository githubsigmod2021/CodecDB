//
// Created by Harper on 6/4/20.
//

#ifndef LQF_MEMORY_H
#define LQF_MEMORY_H

#include <cstdint>
#include <memory>
#include <vector>
#include <mutex>
#include <parquet/types.h>

#define SLAB_SIZE 131072

namespace lqf {
    namespace memory {

        using namespace std;

        /**
         * Store the ByteArray data used in query
         */
        class ByteArrayBuffer {
        protected:
            static thread_local uint32_t index_;
            static thread_local uint32_t offset_;

            mutex assign_lock_;
            vector<uint8_t *> buffer_;
            uint32_t buffer_watermark_;

            void new_slab();

        public:
            ByteArrayBuffer();

            ByteArrayBuffer(ByteArrayBuffer &) = delete;

            ByteArrayBuffer(ByteArrayBuffer &&) = delete;

            virtual ~ByteArrayBuffer();

            ByteArrayBuffer &operator=(ByteArrayBuffer &) = delete;

            ByteArrayBuffer &operator=(ByteArrayBuffer &&) = delete;

            void allocate(parquet::ByteArray &input);

            static ByteArrayBuffer instance;
        };

    }
}


#endif //LQF_MEMORY_H
