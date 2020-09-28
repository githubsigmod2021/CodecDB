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

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;

public class RowFieldPrimitiveConverter extends PrimitiveConverter {

    private int index;

    private Dictionary dictionary;

    private RowTempTable parent;

    private PrimitiveType type;

    public RowFieldPrimitiveConverter(RowTempTable parent, int index, PrimitiveType type) {
        this.parent = parent;
        this.index = index;
        this.type = type;
    }

    @Override
    public boolean hasDictionarySupport() {
        return dictionary != null;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        switch (this.type.getPrimitiveTypeName()) {
            case BINARY:
                addBinary(this.dictionary.decodeToBinary(dictionaryId));
                break;
            case FLOAT:
                addFloat(this.dictionary.decodeToFloat(dictionaryId));
                break;
            case DOUBLE:
                addDouble(this.dictionary.decodeToDouble(dictionaryId));
                break;
            case INT32:
                addInt(this.dictionary.decodeToInt(dictionaryId));
                break;
            case INT64:
                addLong(this.dictionary.decodeToLong(dictionaryId));
                break;
            case BOOLEAN:
                addBoolean(this.dictionary.decodeToBoolean(dictionaryId));
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void addBinary(Binary value) {
        parent.getCurrentRecord().add(index, value);
    }

    @Override
    public void addBoolean(boolean value) {
        parent.getCurrentRecord().add(index, value);
    }

    @Override
    public void addDouble(double value) {
        parent.getCurrentRecord().add(index, value);
    }

    @Override
    public void addFloat(float value) {
        parent.getCurrentRecord().add(index, value);
    }

    @Override
    public void addInt(int value) {
        parent.getCurrentRecord().add(index, value);
    }

    @Override
    public void addLong(long value) {
        parent.getCurrentRecord().add(index, value);
    }

}
