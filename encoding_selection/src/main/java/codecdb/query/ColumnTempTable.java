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
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

public class ColumnTempTable extends GroupConverter implements TempTable {

    protected MessageType schema;

    protected ColumnPrimitiveConverter[] converters;

    protected Column[] columns;

    protected Map<ColumnKey, Integer> pathMaps;

    public ColumnTempTable(MessageType schema) {
        this.schema = schema;

        converters = new ColumnPrimitiveConverter[schema.getColumns().size()];
        columns = new Column[converters.length];
        pathMaps = new HashMap<>();
        for (int i = 0; i < converters.length; i++) {
            converters[i] = new ColumnPrimitiveConverter(this, i,
                    schema.getType(i).asPrimitiveType());
            columns[i] = new Column();
            pathMaps.put(new ColumnKey(schema.getColumns().get(i).getPath()), i);
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    public Converter getConverter(String[] path) {
        return getConverter(pathMaps.get(new ColumnKey(path)));
    }

    public Column[] getColumns() {
        return columns;
    }

    @Override
    public Object[] getRecord(int index) {
        Object[] result = new Object[columns.length];
        for(int i = 0 ; i < result.length;i++) {
            result[i] = columns[i].getData().get(index);
        }
        return result;
    }

    @Override
    public void start() {

    }

    @Override
    public void end() {

    }

    public void add(int index, Binary value) {
        columns[index].add(value);
    }

    public void add(int index, boolean value) {
        columns[index].add(value);
    }

    public void add(int index, double value) {
        columns[index].add(value);
    }

    public void add(int index, float value) {
        columns[index].add(value);
    }

    public void add(int index, int value) {
        columns[index].add(value);
    }

    public void add(int index, long value) {
        columns[index].add(value);
    }
}
