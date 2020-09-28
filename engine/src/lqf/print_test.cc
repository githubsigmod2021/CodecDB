//
// Created by harper on 2/27/20.
//

#include <gtest/gtest.h>
#include "print.h"

using namespace lqf;

TEST(PrinterTest, Print) {
    auto printer = Printer::Make(PBEGIN PI(0) PD(1) PEND);
    auto memTable = MemTable::Make(2);
    auto block = memTable->allocate(10);
    auto rows = block->rows();
    for (int i = 0; i < 10; ++i) {
        (*rows)[i][0] = i + 10;
        (*rows)[i][1] = i * 0.1;
    }
    printer->print(*memTable);
}
