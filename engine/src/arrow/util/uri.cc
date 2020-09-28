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

#include "arrow/util/uri.h"

#include <cstring>
#include <sstream>
#include <vector>

#include "arrow/util/parsing.h"
#include "arrow/util/string_view.h"
#include "arrow/vendored/uriparser/Uri.h"

namespace arrow {
namespace internal {

namespace {

util::string_view TextRangeToView(const UriTextRangeStructA& range) {
  if (range.first == nullptr) {
    return "";
  } else {
    return {range.first, static_cast<size_t>(range.afterLast - range.first)};
  }
}

std::string TextRangeToString(const UriTextRangeStructA& range) {
  return std::string(TextRangeToView(range));
}

// There can be a difference between an absent field and an empty field.
// For example, in "unix:/tmp/foo", the host is absent, while in
// "unix:///tmp/foo", the host is empty but present.
// This function helps distinguish.
bool IsTextRangeSet(const UriTextRangeStructA& range) { return range.first != nullptr; }

}  // namespace

struct Uri::Impl {
  Impl() : string_rep_(""), port_(-1) { memset(&uri_, 0, sizeof(uri_)); }

  ~Impl() { uriFreeUriMembersA(&uri_); }

  void Reset() {
    uriFreeUriMembersA(&uri_);
    memset(&uri_, 0, sizeof(uri_));
    data_.clear();
    port_ = -1;
  }

  const std::string& KeepString(const std::string& s) {
    data_.push_back(s);
    return data_.back();
  }

  UriUriA uri_;
  // Keep alive strings that uriparser stores pointers to
  std::vector<std::string> data_;
  std::string string_rep_;
  int32_t port_;
};

Uri::Uri() : impl_(new Impl) {}

Uri::~Uri() {}

Uri::Uri(Uri&& u) : impl_(std::move(u.impl_)) {}

Uri& Uri::operator=(Uri&& u) {
  impl_ = std::move(u.impl_);
  return *this;
}

std::string Uri::scheme() const { return TextRangeToString(impl_->uri_.scheme); }

std::string Uri::host() const { return TextRangeToString(impl_->uri_.hostText); }

bool Uri::has_host() const { return IsTextRangeSet(impl_->uri_.hostText); }

std::string Uri::port_text() const { return TextRangeToString(impl_->uri_.portText); }

int32_t Uri::port() const { return impl_->port_; }

std::string Uri::path() const {
  // Gather path segments
  std::vector<util::string_view> segments;
  auto path_seg = impl_->uri_.pathHead;
  while (path_seg != nullptr) {
    segments.push_back(TextRangeToView(path_seg->text));
    path_seg = path_seg->next;
  }

  std::stringstream ss;
  if (impl_->uri_.absolutePath == URI_TRUE) {
    ss << "/";
  } else if (has_host() && segments.size() > 0) {
    // When there's a host (even empty), uriparser considers the path relative.
    // Several URI parsers for Python all consider it absolute, though.
    // For example, the path for "file:///tmp/foo" is "/tmp/foo", not "tmp/foo".
    // Similarly, the path for "file://localhost/" is "/".
    // However, the path for "file://localhost" is "".
    ss << "/";
  }
  bool first = true;
  for (const auto seg : segments) {
    if (!first) {
      ss << "/";
    }
    first = false;
    ss << seg;
  }
  return ss.str();
}

std::string Uri::query_string() const { return TextRangeToString(impl_->uri_.query); }

Result<std::vector<std::pair<std::string, std::string>>> Uri::query_items() const {
  const auto& query = impl_->uri_.query;
  UriQueryListA* query_list;
  int item_count;
  std::vector<std::pair<std::string, std::string>> items;

  if (query.first == nullptr) {
    return items;
  }
  if (uriDissectQueryMallocA(&query_list, &item_count, query.first, query.afterLast) !=
      URI_SUCCESS) {
    return Status::Invalid("Cannot parse query string: '", query_string(), "'");
  }
  std::unique_ptr<UriQueryListA, decltype(&uriFreeQueryListA)> query_guard(
      query_list, uriFreeQueryListA);

  items.reserve(item_count);
  while (query_list != nullptr) {
    if (query_list->value != nullptr) {
      items.emplace_back(query_list->key, query_list->value);
    } else {
      items.emplace_back(query_list->key, "");
    }
    query_list = query_list->next;
  }
  return items;
}

const std::string& Uri::ToString() const { return impl_->string_rep_; }

Status Uri::Parse(const std::string& uri_string) {
  impl_->Reset();

  const auto& s = impl_->KeepString(uri_string);
  impl_->string_rep_ = s;
  const char* error_pos;
  if (uriParseSingleUriExA(&impl_->uri_, s.data(), s.data() + s.size(), &error_pos) !=
      URI_SUCCESS) {
    return Status::Invalid("Cannot parse URI: '", uri_string, "'");
  }
  // Parse port number
  auto port_text = TextRangeToView(impl_->uri_.portText);
  if (port_text.size()) {
    StringConverter<UInt16Type> port_converter;
    uint16_t port_num;
    if (!port_converter(port_text.data(), port_text.size(), &port_num)) {
      return Status::Invalid("Invalid port number '", port_text, "' in URI '", uri_string,
                             "'");
    }
    impl_->port_ = port_num;
  }

  return Status::OK();
}

}  // namespace internal
}  // namespace arrow
