//
// Created by harper on 2/15/20.
//

#ifndef CHIDATA_LANG_H
#define CHIDATA_LANG_H

#include <cstdint>

namespace lqf {

    struct Int32 {
        using type = int32_t;
        static const int32_t empty;
        static const int32_t min;
        static const int32_t max;
    };

    struct Int64 {
        using type = int64_t;
        static const int64_t empty;
        static const int64_t min;
        static const int64_t max;
    };

    template<typename type>
    class Iterator {
    public:
        virtual ~Iterator() = default;

        virtual bool hasNext() = 0;

        virtual type next() = 0;
    };
}

#endif //ARROW_LANG_H
