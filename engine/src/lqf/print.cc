//
// Created by harper on 2/27/20.
//

#include "data_model.h"
#include "print.h"

namespace lqf {

    PrintBuffer pout;

    // Honor the width and fill setting
    ostream &operator<<(ostream &os, const ByteArray &dt) {
        __ostream_insert(os, (const char *) dt.ptr, dt.len);
        return os;
    }

    PrintBuffer::PrintBuffer() : data_pointer_(0), position_(0), firstline_(true), num_lines_(0) {
        data_ = (uint8_t *) malloc(1048576);
    }

    PrintBuffer::~PrintBuffer() noexcept {
        free(data_);
    }

    PrintBuffer &PrintBuffer::operator<<(const int32_t value) {
        uint32_t width = (uint32_t) ceil(log10(value));
        if (firstline_) {
            data_type_.push_back(0);
            output_width_.push_back(width);
        } else {
            output_width_[position_] = max(width, output_width_[position_]);
            ++position_;
        }
        memcpy((void *) (data_ + data_pointer_), (void *) &value, 4);
        data_pointer_ += 4;
        return *this;
    }

    PrintBuffer &PrintBuffer::operator<<(const double value) {
        uint32_t width = 16;
        if (firstline_) {
            data_type_.push_back(1);
            output_width_.push_back(width);
        } else {
            ++position_;
        }
        memcpy((void *) (data_ + data_pointer_), (void *) &value, 8);
        data_pointer_ += 8;
        return *this;
    }

    PrintBuffer &PrintBuffer::operator<<(const ByteArray &value) {
        uint32_t width = value.len;
        if (firstline_) {
            data_type_.push_back(2);
            output_width_.push_back(width);
        } else {
            output_width_[position_] = max(width, output_width_[position_]);
            ++position_;
        }
        memcpy((void *) (data_ + data_pointer_), (void *) &value.len, 4);
        data_pointer_ += 4;
        memcpy((void *) (data_ + data_pointer_), (void *) &value.ptr, 8);
        data_pointer_ += 8;
        return *this;
    }

    PrintBuffer &PrintBuffer::operator<<(const char value) {
        if (value == '\n') {
            position_ = 0;
            firstline_ = false;
            ++num_lines_;
        }
        return *this;
    }

    void PrintBuffer::output() {
        uint8_t *data_pointer = data_;
        int intvalue;
        double dblvalue;
        ByteArray bytevalue;

        uint32_t total_width = 0;
        for (auto &width:output_width_) {
            total_width += width + 1;
        }
        total_width++;

        cout << setprecision(8);
        fill_n(std::ostream_iterator<char>(std::cout), total_width, '=');
        cout << '\n';

        for (uint32_t i = 0; i < num_lines_; ++i) {
            for (uint32_t field_idx = 0; field_idx < data_type_.size(); ++field_idx) {
                auto data_type = data_type_[field_idx];
                cout << "|";
                cout << left << setw(output_width_[field_idx]);
                switch (data_type) {
                    case 0:
                        memcpy(&intvalue, data_pointer, 4);
                        data_pointer += 4;
                        cout << intvalue;
                        break;
                    case 1:
                        memcpy(&dblvalue, data_pointer, 8);
                        data_pointer += 8;
                        cout << dblvalue;
                        break;
                    case 2:
                        memcpy(&bytevalue.len, data_pointer, 4);
                        data_pointer += 4;
                        memcpy(&bytevalue.ptr, data_pointer, 8);
                        data_pointer += 8;
                        cout << bytevalue;
                        break;
                    default:
                        break;
                }
            }
            cout << '|';
            cout << '\n';
        }
        fill_n(std::ostream_iterator<char>(std::cout), total_width, '=');
        cout << '\n';
        // Reset
        data_type_.clear();
        output_width_.clear();
        position_ = 0;
        data_pointer_ = 0;
        firstline_ = true;
        num_lines_ = 0;
    }

    void Printer::printBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto block_size = block->size();
        for (uint32_t i = 0; i < block_size; ++i) {
//            cout << left << setw(3) << i;
            linePrinter_(rows->next());
        }
//        lqf::pout.output();
        sum_ += block->size();
    }

    Printer::Printer(function<void(DataRow &)> linePrinter) : Node(1), linePrinter_(linePrinter) {}

    unique_ptr<NodeOutput> Printer::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        print(*(input0->get()));
        return nullptr;
    }

    unique_ptr<Printer> Printer::Make(function<void(DataRow &)> linePrinter) {
        Printer *printer = new Printer(linePrinter);
        return unique_ptr<Printer>(printer);
    }

    void Printer::print(Table &table) {
        sum_ = 0;
        function<void(const shared_ptr<Block> &)> printer = bind(&Printer::printBlock, this, _1);
        table.blocks()->foreach(printer);
        cout << "Total: " << sum_ << " rows" << endl;
    }
}


