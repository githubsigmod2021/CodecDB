//
// Created by harper on 2/27/20.
//

#ifndef ARROW_PRINT_H
#define ARROW_PRINT_H

#include <functional>
#include <string>
#include <iomanip>
#include "data_model.h"
#include "parallel.h"

//#define PBEGIN [=](DataRow& row) { lqf::pout <<  //std::setprecision(8) <<
//#define PEND '\n' ;}
//#define PI(x) row[x].asInt() <<
//#define PD(x) row[x].asDouble() <<
//#define PB(x) row[x].asByteArray() <<
//#define PDICT(dict, x) (*dict)[row[x].asInt()] <<

#define PBEGIN [=](DataRow& row) { cout <<  std::setprecision(8) <<
#define PEND '\n' ;}
#define PI(x) row[x].asInt() << "," <<
#define PD(x) row[x].asDouble() << "," <<
#define PB(x) row[x].asByteArray() << "," <<
#define PDICT(dict, x) (*dict)[row[x].asInt()] << "," <<

using namespace std;
using namespace std::placeholders;
namespace lqf {

    ostream &operator<<(ostream &os, const ByteArray &dt);

    ///
    /// Buffer and Print Result in tabular format
    ///
    class PrintBuffer {
    protected:
        uint8_t *data_;
        uint32_t data_pointer_;
        vector<uint32_t> data_type_;
        vector<uint32_t> output_width_;
        uint32_t position_;
        bool firstline_;
        uint32_t num_lines_;
    public:
        PrintBuffer();

        PrintBuffer(PrintBuffer &) = delete;

        PrintBuffer(PrintBuffer &&) = delete;

        virtual ~PrintBuffer();

        PrintBuffer &operator=(PrintBuffer &) = delete;

        PrintBuffer &operator=(PrintBuffer &&) = delete;

        PrintBuffer &operator<<(const int32_t value);

        PrintBuffer &operator<<(const double value);

        PrintBuffer &operator<<(const ByteArray &value);

        PrintBuffer &operator<<(const char value);

        void output();
    };

    extern PrintBuffer pout;

    using namespace parallel;

    class Printer : public Node {
    protected:
        uint64_t sum_;

        function<void(DataRow & )> linePrinter_;

        virtual void printBlock(const shared_ptr<Block> &block);

    public:
        Printer(function<void(DataRow & )> linePrinter);

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        void print(Table &table);

        static unique_ptr<Printer> Make(function<void(DataRow & )> linePrinter);
    };
}
#endif //ARROW_PRINT_H
