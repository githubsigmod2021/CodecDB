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

#include "arrow/python/pyarrow.h"

#include <memory>

#include "arrow/array.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"

#include "arrow/python/common.h"
#include "arrow/python/datetime.h"
namespace {
#include "arrow/python/pyarrow_api.h"
}

namespace arrow {
namespace py {

static Status UnwrapError(PyObject* obj, const char* expected_type) {
  return Status::TypeError("Could not unwrap ", expected_type,
                           " from Python object of type '", Py_TYPE(obj)->tp_name, "'");
}

int import_pyarrow() {
  internal::InitDatetime();
  return ::import_pyarrow__lib();
}

bool is_buffer(PyObject* buffer) { return ::pyarrow_is_buffer(buffer) != 0; }

Status unwrap_buffer(PyObject* buffer, std::shared_ptr<Buffer>* out) {
  *out = ::pyarrow_unwrap_buffer(buffer);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(buffer, "Buffer");
  }
}

PyObject* wrap_buffer(const std::shared_ptr<Buffer>& buffer) {
  return ::pyarrow_wrap_buffer(buffer);
}

bool is_data_type(PyObject* data_type) { return ::pyarrow_is_data_type(data_type) != 0; }

Status unwrap_data_type(PyObject* object, std::shared_ptr<DataType>* out) {
  *out = ::pyarrow_unwrap_data_type(object);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(object, "DataType");
  }
}

PyObject* wrap_data_type(const std::shared_ptr<DataType>& type) {
  return ::pyarrow_wrap_data_type(type);
}

bool is_field(PyObject* field) { return ::pyarrow_is_field(field) != 0; }

Status unwrap_field(PyObject* field, std::shared_ptr<Field>* out) {
  *out = ::pyarrow_unwrap_field(field);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(field, "Field");
  }
}

PyObject* wrap_field(const std::shared_ptr<Field>& field) {
  return ::pyarrow_wrap_field(field);
}

bool is_schema(PyObject* schema) { return ::pyarrow_is_schema(schema) != 0; }

Status unwrap_schema(PyObject* schema, std::shared_ptr<Schema>* out) {
  *out = ::pyarrow_unwrap_schema(schema);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(schema, "Schema");
  }
}

PyObject* wrap_schema(const std::shared_ptr<Schema>& schema) {
  return ::pyarrow_wrap_schema(schema);
}

bool is_array(PyObject* array) { return ::pyarrow_is_array(array) != 0; }

Status unwrap_array(PyObject* array, std::shared_ptr<Array>* out) {
  *out = ::pyarrow_unwrap_array(array);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(array, "Array");
  }
}

PyObject* wrap_array(const std::shared_ptr<Array>& array) {
  return ::pyarrow_wrap_array(array);
}

PyObject* wrap_chunked_array(const std::shared_ptr<ChunkedArray>& array) {
  return ::pyarrow_wrap_chunked_array(array);
}

bool is_tensor(PyObject* tensor) { return ::pyarrow_is_tensor(tensor) != 0; }

Status unwrap_tensor(PyObject* tensor, std::shared_ptr<Tensor>* out) {
  *out = ::pyarrow_unwrap_tensor(tensor);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(tensor, "Tensor");
  }
}

PyObject* wrap_tensor(const std::shared_ptr<Tensor>& tensor) {
  return ::pyarrow_wrap_tensor(tensor);
}

bool is_sparse_coo_tensor(PyObject* sparse_tensor) {
  return ::pyarrow_is_sparse_coo_tensor(sparse_tensor) != 0;
}

Status unwrap_sparse_coo_tensor(PyObject* sparse_tensor,
                                std::shared_ptr<SparseCOOTensor>* out) {
  *out = ::pyarrow_unwrap_sparse_coo_tensor(sparse_tensor);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(sparse_tensor, "SparseCOOTensor");
  }
}

PyObject* wrap_sparse_coo_tensor(const std::shared_ptr<SparseCOOTensor>& sparse_tensor) {
  return ::pyarrow_wrap_sparse_coo_tensor(sparse_tensor);
}

bool is_sparse_csr_matrix(PyObject* sparse_tensor) {
  return ::pyarrow_is_sparse_csr_matrix(sparse_tensor) != 0;
}

Status unwrap_sparse_csr_matrix(PyObject* sparse_tensor,
                                std::shared_ptr<SparseCSRMatrix>* out) {
  *out = ::pyarrow_unwrap_sparse_csr_matrix(sparse_tensor);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(sparse_tensor, "SparseCSRMatrix");
  }
}

PyObject* wrap_sparse_csr_matrix(const std::shared_ptr<SparseCSRMatrix>& sparse_tensor) {
  return ::pyarrow_wrap_sparse_csr_matrix(sparse_tensor);
}

bool is_table(PyObject* table) { return ::pyarrow_is_table(table) != 0; }

Status unwrap_table(PyObject* table, std::shared_ptr<Table>* out) {
  *out = ::pyarrow_unwrap_table(table);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(table, "Table");
  }
}

PyObject* wrap_table(const std::shared_ptr<Table>& table) {
  return ::pyarrow_wrap_table(table);
}

bool is_record_batch(PyObject* batch) { return ::pyarrow_is_batch(batch) != 0; }

Status unwrap_record_batch(PyObject* batch, std::shared_ptr<RecordBatch>* out) {
  *out = ::pyarrow_unwrap_batch(batch);
  if (*out) {
    return Status::OK();
  } else {
    return UnwrapError(batch, "RecordBatch");
  }
}

PyObject* wrap_record_batch(const std::shared_ptr<RecordBatch>& batch) {
  return ::pyarrow_wrap_batch(batch);
}

namespace internal {

int check_status(const Status& status) { return ::pyarrow_internal_check_status(status); }

}  // namespace internal
}  // namespace py
}  // namespace arrow
