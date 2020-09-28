// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>  // IWYU pragma: keep
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/string.h"
#include "arrow/vendored/datetime.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

class PrettyPrinter {
 public:
  PrettyPrinter(int indent, int indent_size, int window, bool skip_new_lines,
                std::ostream* sink)
      : indent_(indent),
        indent_size_(indent_size),
        window_(window),
        skip_new_lines_(skip_new_lines),
        sink_(sink) {}

  void Write(const char* data);
  void Write(const std::string& data);
  void WriteIndented(const char* data);
  void WriteIndented(const std::string& data);
  void Newline();
  void Indent();
  void OpenArray(const Array& array);
  void CloseArray(const Array& array);

  void Flush() { (*sink_) << std::flush; }

 protected:
  int indent_;
  int indent_size_;
  int window_;
  bool skip_new_lines_;
  std::ostream* sink_;
};

void PrettyPrinter::OpenArray(const Array& array) {
  Indent();
  (*sink_) << "[";
  if (array.length() > 0) {
    (*sink_) << "\n";
    indent_ += indent_size_;
  }
}

void PrettyPrinter::CloseArray(const Array& array) {
  if (array.length() > 0) {
    indent_ -= indent_size_;
    Indent();
  }
  (*sink_) << "]";
}

void PrettyPrinter::Write(const char* data) { (*sink_) << data; }
void PrettyPrinter::Write(const std::string& data) { (*sink_) << data; }

void PrettyPrinter::WriteIndented(const char* data) {
  Indent();
  Write(data);
}

void PrettyPrinter::WriteIndented(const std::string& data) {
  Indent();
  Write(data);
}

void PrettyPrinter::Newline() {
  if (skip_new_lines_) {
    return;
  }
  (*sink_) << "\n";
  Indent();
}

void PrettyPrinter::Indent() {
  for (int i = 0; i < indent_; ++i) {
    (*sink_) << " ";
  }
}

class ArrayPrinter : public PrettyPrinter {
 public:
  ArrayPrinter(int indent, int indent_size, int window, const std::string& null_rep,
               bool skip_new_lines, std::ostream* sink)
      : PrettyPrinter(indent, indent_size, window, skip_new_lines, sink),
        null_rep_(null_rep) {}

  template <typename FormatFunction>
  void WriteValues(const Array& array, FormatFunction&& func) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",\n";
      }
      Indent();
      if ((i >= window_) && (i < (array.length() - window_))) {
        (*sink_) << "...\n";
        i = array.length() - window_ - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        (*sink_) << null_rep_;
      } else {
        func(i);
      }
    }
    (*sink_) << "\n";
  }

  Status WriteDataValues(const BooleanArray& array) {
    WriteValues(array, [&](int64_t i) { Write(array.Value(i) ? "true" : "false"); });
    return Status::OK();
  }

  template <typename T>
  enable_if_integer<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    // Need to upcast integers to avoid selecting operator<<(char)
    WriteValues(array, [&](int64_t i) { (*sink_) << internal::UpcastInt(data[i]); });
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<typename T::TypeClass, Status> WriteDataValues(
      const T& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  template <typename T>
  enable_if_date<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    using unit = typename std::conditional<std::is_same<T, Date32Array>::value,
                                           arrow_vendored::date::days,
                                           std::chrono::milliseconds>::type;
    WriteValues(array, [&](int64_t i) { FormatDateTime<unit>("%F", data[i], true); });
    return Status::OK();
  }

  template <typename T>
  enable_if_time<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    const auto type = static_cast<const TimeType*>(array.type().get());
    WriteValues(array,
                [&](int64_t i) { FormatDateTime(type->unit(), "%T", data[i], false); });
    return Status::OK();
  }

  Status WriteDataValues(const TimestampArray& array) {
    const int64_t* data = array.raw_values();
    const auto type = static_cast<const TimestampType*>(array.type().get());
    WriteValues(array,
                [&](int64_t i) { FormatDateTime(type->unit(), "%F %T", data[i], true); });
    return Status::OK();
  }

  template <typename T>
  enable_if_duration<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  Status WriteDataValues(const DayTimeIntervalArray& array) {
    WriteValues(array, [&](int64_t i) {
      auto day_millis = array.GetValue(i);
      (*sink_) << day_millis.days << "d" << day_millis.milliseconds << "ms";
    });
    return Status::OK();
  }

  Status WriteDataValues(const MonthIntervalArray& array) {
    const auto data = array.raw_values();
    WriteValues(array, [&](int64_t i) { (*sink_) << data[i]; });
    return Status::OK();
  }

  template <typename T>
  enable_if_string_like<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << "\"" << array.GetView(i) << "\""; });
    return Status::OK();
  }

  // Binary
  template <typename T>
  enable_if_binary_like<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << HexEncode(array.GetView(i)); });
    return Status::OK();
  }

  Status WriteDataValues(const Decimal128Array& array) {
    WriteValues(array, [&](int64_t i) { (*sink_) << array.FormatValue(i); });
    return Status::OK();
  }

  template <typename T>
  enable_if_list_like<typename T::TypeClass, Status> WriteDataValues(const T& array) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",\n";
      }
      if ((i >= window_) && (i < (array.length() - window_))) {
        Indent();
        (*sink_) << "...\n";
        i = array.length() - window_ - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        Indent();
        (*sink_) << null_rep_;
      } else {
        std::shared_ptr<Array> slice =
            array.values()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(PrettyPrint(*slice, {indent_, window_}, sink_));
      }
    }
    (*sink_) << "\n";
    return Status::OK();
  }

  Status WriteDataValues(const MapArray& array) {
    bool skip_comma = true;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (skip_comma) {
        skip_comma = false;
      } else {
        (*sink_) << ",\n";
      }
      if ((i >= window_) && (i < (array.length() - window_))) {
        Indent();
        (*sink_) << "...\n";
        i = array.length() - window_ - 1;
        skip_comma = true;
      } else if (array.IsNull(i)) {
        Indent();
        (*sink_) << null_rep_;
      } else {
        Indent();
        (*sink_) << "keys:\n";
        auto keys_slice =
            array.keys()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(PrettyPrint(*keys_slice, {indent_, window_}, sink_));
        (*sink_) << "\n";
        Indent();
        (*sink_) << "values:\n";
        auto values_slice =
            array.items()->Slice(array.value_offset(i), array.value_length(i));
        RETURN_NOT_OK(PrettyPrint(*values_slice, {indent_, window_}, sink_));
      }
    }
    (*sink_) << "\n";
    return Status::OK();
  }

  Status Visit(const NullArray& array) {
    (*sink_) << array.length() << " nulls";
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_base_of<PrimitiveArray, T>::value ||
                  std::is_base_of<FixedSizeBinaryArray, T>::value ||
                  std::is_base_of<BinaryArray, T>::value ||
                  std::is_base_of<LargeBinaryArray, T>::value ||
                  std::is_base_of<ListArray, T>::value ||
                  std::is_base_of<LargeListArray, T>::value ||
                  std::is_base_of<MapArray, T>::value ||
                  std::is_base_of<FixedSizeListArray, T>::value,
              Status>
  Visit(const T& array) {
    OpenArray(array);
    if (array.length() > 0) {
      RETURN_NOT_OK(WriteDataValues(array));
    }
    CloseArray(array);
    return Status::OK();
  }

  Status Visit(const ExtensionArray& array) { return Print(*array.storage()); }

  Status WriteValidityBitmap(const Array& array);

  Status PrintChildren(const std::vector<std::shared_ptr<Array>>& fields, int64_t offset,
                       int64_t length) {
    for (size_t i = 0; i < fields.size(); ++i) {
      Newline();
      std::stringstream ss;
      ss << "-- child " << i << " type: " << fields[i]->type()->ToString() << "\n";
      Write(ss.str());

      std::shared_ptr<Array> field = fields[i];
      if (offset != 0) {
        field = field->Slice(offset, length);
      }

      RETURN_NOT_OK(PrettyPrint(*field, indent_ + indent_size_, sink_));
    }
    return Status::OK();
  }

  Status Visit(const StructArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.field(i));
    }
    return PrintChildren(children, 0, array.length());
  }

  Status Visit(const UnionArray& array) {
    RETURN_NOT_OK(WriteValidityBitmap(array));

    Newline();
    Write("-- type_ids: ");
    UInt8Array type_codes(array.length(), array.type_codes(), nullptr, 0, array.offset());
    RETURN_NOT_OK(PrettyPrint(type_codes, indent_ + indent_size_, sink_));

    if (array.mode() == UnionMode::DENSE) {
      Newline();
      Write("-- value_offsets: ");
      Int32Array value_offsets(array.length(), array.value_offsets(), nullptr, 0,
                               array.offset());
      RETURN_NOT_OK(PrettyPrint(value_offsets, indent_ + indent_size_, sink_));
    }

    // Print the children without any offset, because the type ids are absolute
    std::vector<std::shared_ptr<Array>> children;
    children.reserve(array.num_fields());
    for (int i = 0; i < array.num_fields(); ++i) {
      children.emplace_back(array.child(i));
    }
    return PrintChildren(children, 0, array.length() + array.offset());
  }

  Status Visit(const DictionaryArray& array) {
    Newline();
    Write("-- dictionary:\n");
    RETURN_NOT_OK(PrettyPrint(*array.dictionary(), indent_ + indent_size_, sink_));

    Newline();
    Write("-- indices:\n");
    return PrettyPrint(*array.indices(), indent_ + indent_size_, sink_);
  }

  Status Print(const Array& array) {
    RETURN_NOT_OK(VisitArrayInline(array, this));
    Flush();
    return Status::OK();
  }

 private:
  template <typename Unit>
  void FormatDateTime(const char* fmt, int64_t value, bool add_epoch) {
    if (add_epoch) {
      (*sink_) << arrow_vendored::date::format(fmt, epoch_ + Unit{value});
    } else {
      (*sink_) << arrow_vendored::date::format(fmt, Unit{value});
    }
  }

  void FormatDateTime(TimeUnit::type unit, const char* fmt, int64_t value,
                      bool add_epoch) {
    switch (unit) {
      case TimeUnit::NANO:
        FormatDateTime<std::chrono::nanoseconds>(fmt, value, add_epoch);
        break;
      case TimeUnit::MICRO:
        FormatDateTime<std::chrono::microseconds>(fmt, value, add_epoch);
        break;
      case TimeUnit::MILLI:
        FormatDateTime<std::chrono::milliseconds>(fmt, value, add_epoch);
        break;
      case TimeUnit::SECOND:
        FormatDateTime<std::chrono::seconds>(fmt, value, add_epoch);
        break;
    }
  }

  static arrow_vendored::date::sys_days epoch_;
  std::string null_rep_;
};

arrow_vendored::date::sys_days ArrayPrinter::epoch_ =
    arrow_vendored::date::sys_days{arrow_vendored::date::jan / 1 / 1970};

Status ArrayPrinter::WriteValidityBitmap(const Array& array) {
  Indent();
  Write("-- is_valid:");

  if (array.null_count() > 0) {
    Newline();
    BooleanArray is_valid(array.length(), array.null_bitmap(), nullptr, 0,
                          array.offset());
    return PrettyPrint(is_valid, indent_ + indent_size_, sink_);
  } else {
    Write(" all not null");
    return Status::OK();
  }
}

Status PrettyPrint(const Array& arr, int indent, std::ostream* sink) {
  ArrayPrinter printer(indent, 2, 10, "null", false, sink);
  return printer.Print(arr);
}

Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  ArrayPrinter printer(options.indent, options.indent_size, options.window,
                       options.null_rep, options.skip_new_lines, sink);
  return printer.Print(arr);
}

Status PrettyPrint(const Array& arr, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(arr, options, &sink));
  *result = sink.str();
  return Status::OK();
}

Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  int num_chunks = chunked_arr.num_chunks();
  int indent = options.indent;
  int window = options.window;

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "[\n";
  bool skip_comma = true;
  for (int i = 0; i < num_chunks; ++i) {
    if (skip_comma) {
      skip_comma = false;
    } else {
      (*sink) << ",\n";
    }
    if ((i >= window) && (i < (num_chunks - window))) {
      for (int i = 0; i < indent; ++i) {
        (*sink) << " ";
      }
      (*sink) << "...\n";
      i = num_chunks - window - 1;
      skip_comma = true;
    } else {
      ArrayPrinter printer(indent + options.indent_size, options.indent_size, window,
                           options.null_rep, options.skip_new_lines, sink);
      RETURN_NOT_OK(printer.Print(*chunked_arr.chunk(i)));
    }
  }
  (*sink) << "\n";

  for (int i = 0; i < indent; ++i) {
    (*sink) << " ";
  }
  (*sink) << "]";

  return Status::OK();
}

Status PrettyPrint(const ChunkedArray& chunked_arr, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(chunked_arr, options, &sink));
  *result = sink.str();
  return Status::OK();
}

Status PrettyPrint(const RecordBatch& batch, int indent, std::ostream* sink) {
  for (int i = 0; i < batch.num_columns(); ++i) {
    const std::string& name = batch.column_name(i);
    (*sink) << name << ": ";
    RETURN_NOT_OK(PrettyPrint(*batch.column(i), indent + 2, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status PrettyPrint(const RecordBatch& batch, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  for (int i = 0; i < batch.num_columns(); ++i) {
    const std::string& name = batch.column_name(i);
    PrettyPrintOptions column_options = options;
    column_options.indent += 2;

    (*sink) << name << ": ";
    RETURN_NOT_OK(PrettyPrint(*batch.column(i), column_options, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status PrettyPrint(const Table& table, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  RETURN_NOT_OK(PrettyPrint(*table.schema(), options, sink));
  (*sink) << "\n";
  (*sink) << "----\n";

  PrettyPrintOptions column_options = options;
  column_options.indent += 2;
  for (int i = 0; i < table.num_columns(); ++i) {
    for (int j = 0; j < options.indent; ++j) {
      (*sink) << " ";
    }
    (*sink) << table.schema()->field(i)->name() << ":\n";
    RETURN_NOT_OK(PrettyPrint(*table.column(i), column_options, sink));
    (*sink) << "\n";
  }
  (*sink) << std::flush;
  return Status::OK();
}

Status DebugPrint(const Array& arr, int indent) {
  return PrettyPrint(arr, indent, &std::cout);
}

class SchemaPrinter : public PrettyPrinter {
 public:
  SchemaPrinter(const Schema& schema, int indent, int indent_size, int window,
                bool skip_new_lines, std::ostream* sink)
      : PrettyPrinter(indent, indent_size, window, skip_new_lines, sink),
        schema_(schema) {}

  Status PrintType(const DataType& type, bool nullable);
  Status PrintField(const Field& field);

  Status Print() {
    for (int i = 0; i < schema_.num_fields(); ++i) {
      if (i > 0) {
        Newline();
      } else {
        Indent();
      }
      RETURN_NOT_OK(PrintField(*schema_.field(i)));
    }
    Flush();
    return Status::OK();
  }

 private:
  const Schema& schema_;
};

Status SchemaPrinter::PrintType(const DataType& type, bool nullable) {
  Write(type.ToString());
  if (!nullable) {
    Write(" not null");
  }
  for (int i = 0; i < type.num_children(); ++i) {
    Newline();

    std::stringstream ss;
    ss << "child " << i << ", ";

    indent_ += indent_size_;
    WriteIndented(ss.str());
    RETURN_NOT_OK(PrintField(*type.child(i)));
    indent_ -= indent_size_;
  }
  return Status::OK();
}

Status SchemaPrinter::PrintField(const Field& field) {
  Write(field.name());
  Write(": ");
  return PrintType(*field.type(), field.nullable());
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::ostream* sink) {
  SchemaPrinter printer(schema, options.indent, options.indent_size, options.window,
                        options.skip_new_lines, sink);
  return printer.Print();
}

Status PrettyPrint(const Schema& schema, const PrettyPrintOptions& options,
                   std::string* result) {
  std::ostringstream sink;
  RETURN_NOT_OK(PrettyPrint(schema, options, &sink));
  *result = sink.str();
  return Status::OK();
}

}  // namespace arrow
