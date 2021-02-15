<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# CodecDB Engine

This directory contains the code and build system for the CodecDB Storage and Query Engine.

## Build 

Run cmake from this folder to make a build. Please note this project requires the support of AVX512. We recommend building it with a Skylake CPU.

CMake Build Options:
```
-DARROW_PARQUET=ON -DARROW_BUILD_BENCHMARKS=ON -DARROW_WITH_SNAPPY=ON -DLQF_PARALLEL=ON
```

