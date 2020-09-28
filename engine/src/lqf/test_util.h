//
// Created by Harper on 6/27/20.
//

#ifndef ARROW_TEST_UTIL_H
#define ARROW_TEST_UTIL_H

template <class T>
void blackhole(T&& datum) {
    asm volatile("" : "+g" (datum));
}

#endif //ARROW_TEST_UTIL_H
