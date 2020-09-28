/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Author - initial API and implementation
 */

package codecdb.query;

import org.apache.parquet.io.api.Binary;

public class Row {

    private Object[] data;

    public Object[] getData() { return data; }

    public Row(int colCount) {
        this.data = new Object[colCount];
    }

    public void add(int index, Binary value) {
        data[index] = value;
    }

    public void add(int index, boolean value) {
        data[index] = value;
    }

    public void add(int index, double value) {
        data[index] = value;
    }

    public void add(int index, float value) {
        data[index] = value;
    }

    public void add(int index, int value) {
        data[index] = value;
    }

    public void add(int index, long value) {
        data[index] = value;
    }
}
