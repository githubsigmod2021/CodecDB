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
 * under the License,
 *
 * Contributors:
 *     Author - initial API and implementation
 *
 */
package codecdb.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringWriteSupport extends WriteSupport<List<String>> {
    Logger logger = LoggerFactory.getLogger(getClass());
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    public StringWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer r) {
        recordConsumer = r;
    }

    @Override
    public void write(List<String> values) {
        if (values.size() != cols.size()) {
            throw new ParquetEncodingException("Invalid input data. Expecting " + cols.size() + " columns. Input had "
                    + values.size() + " columns (" + cols + ") : " + values);
        }

        recordConsumer.startMessage();
        for (int i = 0; i < cols.size(); ++i) {
            String val = values.get(i).trim();
            // val.length() == 0 indicates a NULL value.

            if (val.length() > 0) {
                recordConsumer.startField(cols.get(i).getPath()[0], i);
                try {
                    switch (cols.get(i).getType()) {
                        case BOOLEAN:
                            String lower = val.toLowerCase();
                            recordConsumer.addBoolean("true".equals(lower) || "yes".equals(lower) || "1".equals(lower));
                            break;
                        case FLOAT:
                            recordConsumer.addFloat(Float.parseFloat(val));
                            break;
                        case DOUBLE:
                            recordConsumer.addDouble(Double.parseDouble(val));
                            break;
                        case INT32:
                            recordConsumer.addInteger(Integer.parseInt(val));
                            break;
                        case INT64:
                            recordConsumer.addLong(Long.parseLong(val));
                            break;
                        case BINARY:
                            recordConsumer.addBinary(stringToBinary(val));
                            break;
                        default:
                            throw new ParquetEncodingException("Unsupported column type: " + cols.get(i).getType());
                    }
                } catch (Exception e) {
                    logger.warn("Malformated data encountered and skipping:" + val, e);
                }
                recordConsumer.endField(cols.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }

    @Override
    public FinalizedWriteContext finalizeWrite() {
        Map<String, String> extrameta = new HashMap<>();
        // Write encoding context information

        EncContext.context.get().entrySet().forEach((Map.Entry<String, Object[]> entry) -> {
            extrameta.put(entry.getKey() + ".0", entry.getValue()[0].toString());
            extrameta.put(entry.getKey() + ".1", entry.getValue()[1].toString());
        });

        return new FinalizedWriteContext(extrameta);
    }
}
