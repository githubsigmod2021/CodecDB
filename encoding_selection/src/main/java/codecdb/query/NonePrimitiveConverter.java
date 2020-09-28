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

public class NonePrimitiveConverter extends PrimitiveConverter {

    public static NonePrimitiveConverter INSTANCE = new NonePrimitiveConverter();

    private Dictionary dictionary;

    public NonePrimitiveConverter() {
    }

    @Override
    public void addBinary(Binary value) {
    }

    @Override
    public void addBoolean(boolean value) {
    }

    @Override
    public void addDouble(double value) {
    }

    @Override
    public void addFloat(float value) {
    }

    @Override
    public void addInt(int value) {
    }

    @Override
    public void addLong(long value) {
    }
}
