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

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

import codecdb.model.LongEncoding;
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
import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import codecdb.model.FloatEncoding;
import codecdb.model.IntEncoding;
import codecdb.model.StringEncoding;

/**
 * This Factory is intended for single column encoding.
 *
 * @see EncValuesWriterFactory
 * @deprecated
 */
public class AdaptiveValuesWriterFactory implements ValuesWriterFactory {

    private ParquetProperties parquetProperties;

    public static ThreadLocal<EncodingSetting> encodingSetting = ThreadLocal.withInitial(() -> new EncodingSetting());

    @Override
    public void initialize(ParquetProperties parquetProperties) {
        this.parquetProperties = parquetProperties;
    }

    private Encoding getEncodingForDataPage() {
        return RLE_DICTIONARY;
    }

    private Encoding getEncodingForDictionaryPage() {
        return PLAIN;
    }

    @Override
    public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
        if (parquetProperties.isEnableDictionary()) {
            return dictionaryWriter(descriptor, parquetProperties, getEncodingForDictionaryPage(),
                    getEncodingForDataPage());
        }
        switch (descriptor.getType()) {
            case BOOLEAN:
                return getBooleanValuesWriter();
            case FIXED_LEN_BYTE_ARRAY:
                return getFixedLenByteArrayValuesWriter(descriptor);
            case BINARY:
                return getBinaryValuesWriter(descriptor);
            case INT32:
                return getInt32ValuesWriter(descriptor);
            case INT64:
                return getInt64ValuesWriter(descriptor);
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
        return new RunLengthBitPackingHybridValuesWriter(1, parquetProperties.getInitialSlabSize(),
                parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    }

    private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path) {
        return new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(),
                parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    }

    private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path) {
        EncodingSetting es = encodingSetting.get();
        switch (es.stringEncoding) {
            case DELTA:
                return new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case DELTAL:
                return new DeltaLengthByteArrayValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }

    }

    private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path) {
        EncodingSetting es = encodingSetting.get();
        switch (es.intEncoding) {
            case RLE:
                return new RunLengthBitPackingHybridValuesWriter(es.intBitLength, parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case BP:
                if (es.intBitLength <= 8) {
                    return new BitPackingValuesWriter(es.intBound(), parquetProperties.getInitialSlabSize(),
                            parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
                } else {
                    return new ByteBitPackingValuesWriter(es.intBound(), Packer.BIG_ENDIAN);
                }
            case DELTABP:
                return new DeltaBinaryPackingValuesWriterForInteger(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }
    }

    private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path) {
        EncodingSetting es = encodingSetting.get();
        switch (es.longEncoding) {
		/*case RLE:
			if (es.longBitLength > 32)
				throw new IllegalArgumentException("RLE for long does not support over 32 bit length");
			return new RunLengthBitPackingHybridValuesWriter(es.longBitLength, parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
					*/
		/*case BP:
			if (es.longBitLength <= 8) {
				return new BitPackingValuesWriter((int) es.longBound(), parquetProperties.getInitialSlabSize(),
						parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
			} else if (es.longBitLength <= 32) {
				return new ByteBitPackingValuesWriter((int) es.longBound(), Packer.BIG_ENDIAN);
			} else {
				throw new IllegalArgumentException("BP for long does not support over 32 bit length");
			}*/
            case DELTABP:
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
        return new FixedLenByteArrayPlainValuesWriter(12, parquetProperties.getInitialSlabSize(),
                parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
    }

    private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path) {
        EncodingSetting es = encodingSetting.get();
        switch (es.floatEncoding) {
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }
    }

    private ValuesWriter getFloatValuesWriter(ColumnDescriptor path) {
        EncodingSetting es = encodingSetting.get();
        switch (es.floatEncoding) {
            case PLAIN:
                return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
                        parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
            default:
                throw new IllegalArgumentException("Unsupported type " + path.getType());
        }
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

    public static class EncodingSetting {
        public IntEncoding intEncoding = IntEncoding.PLAIN;
        public LongEncoding longEncoding = LongEncoding.PLAIN;
        public int intBitLength = 0;
        public int longBitLength = 0;
        public StringEncoding stringEncoding = StringEncoding.PLAIN;
        public FloatEncoding floatEncoding = FloatEncoding.PLAIN;

        public int intBound() {
            if (intBitLength == 0)
                return 0;
            return 1 << (intBitLength - 1);
        }

        public long longBound() {
            if (longBitLength == 0)
                return 0;
            return 1L << (longBitLength - 1);
        }
    }

}
