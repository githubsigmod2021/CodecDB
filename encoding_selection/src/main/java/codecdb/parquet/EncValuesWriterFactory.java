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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BitPackingValuesWriter;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesWriter;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.factory.DefaultV2ValuesWriterFactory;
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory;
import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;

/**
 * This Factory reads information from ThreadLocal to choose encodings
 */
public class EncValuesWriterFactory implements ValuesWriterFactory {

    private ParquetProperties parquetProperties;

    @Override
    public void initialize(ParquetProperties parquetProperties) {
        this.parquetProperties = parquetProperties;
        this.delegate.initialize(parquetProperties);
    }

    private Encoding getEncodingForDataPage() {
        return RLE_DICTIONARY;
    }

    private Encoding getEncodingForDictionaryPage() {
        return PLAIN;
    }

    private ValuesWriterFactory delegate = new DefaultV2ValuesWriterFactory();

    @Override
    public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
        Encoding enc = EncContext.encoding.get().get(descriptor.toString());
        if (null == enc) {
            if (descriptor.getType() == BOOLEAN) {
                return getBooleanValuesWriter();
            }
            return delegate.newValuesWriter(descriptor);
        }
        if (enc.usesDictionary()) {
            return dictionaryWriter(descriptor, parquetProperties, getEncodingForDictionaryPage(),
                    getEncodingForDataPage());
        }

        switch (descriptor.getType()) {
            case BOOLEAN:
                return getBooleanValuesWriter();
            case FIXED_LEN_BYTE_ARRAY:
                return getFixedLenByteArrayValuesWriter(descriptor);
            case BINARY:
                return getBinaryValuesWriter(descriptor, enc);
            case INT32:
                return getInt32ValuesWriter(descriptor, enc);
            case INT64:
                return getInt64ValuesWriter(descriptor, enc);
            case INT96:
                return getInt96ValuesWriter(descriptor);
            case DOUBLE:
                return getDoubleValuesWriter(descriptor);
            case FLOAT:
                return getFloatValuesWriter(descriptor);
            default:
                throw new IllegalArgumentException("Unknown type " + descriptor.getType());
        }
    }

    private ValuesWriter getBooleanValuesWriter() {
        // no dictionary encoding for boolean
        return new RunLengthBitPackingHybridValuesWriter(1, parquetProperties.getInitialSlabSize(), parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    }

    private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path) {
        return delegate.newValuesWriter(path);
    }

    private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path, Encoding enc) {
        switch (enc) {
            case DELTA_BYTE_ARRAY:
                return new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case DELTA_LENGTH_BYTE_ARRAY:
                return new DeltaLengthByteArrayValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }

    }

    private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path, Encoding enc) {
        Object[] params = EncContext.context.get().get(path.toString());
        int intBitLength = Integer.valueOf(params[0].toString());
        int intBound = Integer.valueOf(params[1].toString());
        switch (enc) {
            case RLE:
                return new RunLengthBitPackingHybridValuesWriter(intBitLength, parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case BIT_PACKED:
                if (intBitLength <= 8) {
                    return new BitPackingValuesWriter(intBound, parquetProperties.getInitialSlabSize(),
                            parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
                } else {
                    return new ByteBitPackingValuesWriter(intBound, Packer.BIG_ENDIAN);
                }
            case DELTA_BINARY_PACKED:
                return new DeltaBinaryPackingValuesWriterForInteger(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }
    }

    private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path, Encoding enc) {
        switch (enc) {
            case DELTA_BINARY_PACKED:
                return new DeltaBinaryPackingValuesWriterForLong(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }
    }

    private ValuesWriter getInt96ValuesWriter(ColumnDescriptor path) {
        return delegate.newValuesWriter(path);
    }

    private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path) {
        return delegate.newValuesWriter(path);
    }

    private ValuesWriter getFloatValuesWriter(ColumnDescriptor path) {
        return delegate.newValuesWriter(path);
    }

    static DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path, ParquetProperties properties,
                                                   Encoding dictPageEncoding, Encoding dataPageEncoding) {
        switch (path.getType()) {
            case BOOLEAN:
                throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
            case BINARY:
                return new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
                        properties.getAllocator());
            case INT32:
                return new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
                        properties.getAllocator());
            case INT64:
                return new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
                        properties.getAllocator());
            case INT96:
                return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), 12, dataPageEncoding, dictPageEncoding,
                        properties.getAllocator());
            case DOUBLE:
                return new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
                        properties.getAllocator());
            case FLOAT:
                return new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
                        properties.getAllocator());
            case FIXED_LEN_BYTE_ARRAY:
                return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
                        properties.getDictionaryPageSizeThreshold(), path.getTypeLength(), dataPageEncoding,
                        dictPageEncoding, properties.getAllocator());
            default:
                throw new IllegalArgumentException("Unknown type " + path.getType());
        }
    }
}
