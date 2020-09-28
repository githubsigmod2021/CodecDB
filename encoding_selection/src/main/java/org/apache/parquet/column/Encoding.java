package org.apache.parquet.column;

import codecdb.parquet.EncContext;
import codecdb.parquet.EncReaderProcessor;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.ZeroIntegerValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;

import static org.apache.parquet.column.values.bitpacking.Packer.BIG_ENDIAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;

public enum Encoding {

    PLAIN {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
            switch (descriptor.getType()) {
                case BOOLEAN:
                    return new BooleanPlainValuesReader();
                case BINARY:
                    return new BinaryPlainValuesReader();
                case FLOAT:
                    return new PlainValuesReader.FloatPlainValuesReader();
                case DOUBLE:
                    return new PlainValuesReader.DoublePlainValuesReader();
                case INT32:
                    return new PlainValuesReader.IntegerPlainValuesReader();
                case INT64:
                    return new PlainValuesReader.LongPlainValuesReader();
                case INT96:
                    return new FixedLenByteArrayPlainValuesReader(12);
                case FIXED_LEN_BYTE_ARRAY:
                    return new FixedLenByteArrayPlainValuesReader(descriptor.getTypeLength());
                default:
                    throw new ParquetDecodingException("no plain reader for type " + descriptor.getType());
            }
        }

        @Override
        public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
            switch (descriptor.getType()) {
                case BINARY:
                    return new PlainValuesDictionary.PlainBinaryDictionary(dictionaryPage);
                case FIXED_LEN_BYTE_ARRAY:
                    return new PlainValuesDictionary.PlainBinaryDictionary(dictionaryPage, descriptor.getTypeLength());
                case INT96:
                    return new PlainValuesDictionary.PlainBinaryDictionary(dictionaryPage, 12);
                case INT64:
                    return new PlainValuesDictionary.PlainLongDictionary(dictionaryPage);
                case DOUBLE:
                    return new PlainValuesDictionary.PlainDoubleDictionary(dictionaryPage);
                case INT32:
                    return new PlainValuesDictionary.PlainIntegerDictionary(dictionaryPage);
                case FLOAT:
                    return new PlainValuesDictionary.PlainFloatDictionary(dictionaryPage);
                default:
                    throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
            }

        }
    },

    /**
     * Actually a combination of bit packing and run length encoding.
     * TODO: Should we rename this to be more clear?
     */
    RLE {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
            int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
            if(bitWidth == 0) {
                return new ZeroIntegerValuesReader();
            }
            return new RunLengthBitPackingHybridValuesReader(bitWidth);
        }
    },

    /**
     * @deprecated This is no longer used, and has been replaced by {@link #RLE}
     * which is combination of bit packing and rle
     */
    @Deprecated
    BIT_PACKED {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
            return new ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), BIG_ENDIAN);
        }
    },

    /**
     * @deprecated now replaced by RLE_DICTIONARY for the data page encoding and PLAIN for the dictionary page encoding
     */
    @Deprecated
    PLAIN_DICTIONARY {
        @Override
        public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
            return RLE_DICTIONARY.getDictionaryBasedValuesReader(descriptor, valuesType, dictionary);
        }

        @Override
        public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
            return PLAIN.initDictionary(descriptor, dictionaryPage);
        }

        @Override
        public boolean usesDictionary() {
            return true;
        }

    },

    /**
     * Delta encoding for integers. This can be used for int columns and works best
     * on sorted data
     */
    DELTA_BINARY_PACKED {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
            if(descriptor.getType() != INT32 && descriptor.getType() != INT64) {
                throw new ParquetDecodingException("Encoding DELTA_BINARY_PACKED is only supported for type INT32 and INT64");
            }
            return new DeltaBinaryPackingValuesReader();
        }
    },

    /**
     * Encoding for byte arrays to separate the length values and the data. The lengths
     * are encoded using DELTA_BINARY_PACKED
     */
    DELTA_LENGTH_BYTE_ARRAY {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor,
                                            ValuesType valuesType) {
            if (descriptor.getType() != BINARY) {
                throw new ParquetDecodingException("Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
            }
            return new DeltaLengthByteArrayValuesReader();
        }
    },

    /**
     * Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
     * Suffixes are stored as delta length byte arrays.
     */
    DELTA_BYTE_ARRAY {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor,
                                            ValuesType valuesType) {
            if (descriptor.getType() != BINARY && descriptor.getType() != FIXED_LEN_BYTE_ARRAY) {
                throw new ParquetDecodingException("Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
            }
            return new DeltaByteArrayReader();
        }
    },

    /**
     * Dictionary encoding: the ids are encoded using the RLE encoding
     */
    RLE_DICTIONARY {

        @Override
        public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
            switch (descriptor.getType()) {
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                case INT96:
                case INT64:
                case DOUBLE:
                case INT32:
                case FLOAT:
                    return new DictionaryValuesReader(dictionary);
                default:
                    throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
            }
        }

        @Override
        public boolean usesDictionary() {
            return true;
        }

    };

    /**
     * For RLE, BIT-PACKED, max level is equivalent to max integer bound
     * Read this information from EncContext, which can be loaded by EncReaderProcessor
     * @see EncReaderProcessor
     * @param descriptor
     * @param valuesType
     * @return
     */
    int getMaxLevel(ColumnDescriptor descriptor, ValuesType valuesType) {
        int maxLevel;
        switch (valuesType) {
            case REPETITION_LEVEL:
                maxLevel = descriptor.getMaxRepetitionLevel();
                break;
            case DEFINITION_LEVEL:
                maxLevel = descriptor.getMaxDefinitionLevel();
                break;
            case VALUES:
                if(descriptor.getType() == BOOLEAN) {
                    maxLevel = 1;
                    break;
                }
                Object[] params = EncContext.context.get().get(descriptor.toString());
                if(params!= null) {
                    maxLevel = Integer.valueOf(params[1].toString());
                    break;
                }
            default:
                throw new ParquetDecodingException("Unsupported encoding for values: " + this);
        }
        return maxLevel;
    }

    /**
     * @return whether this encoding requires a dictionary
     */
    public boolean usesDictionary() {
        return false;
    }

    /**
     * initializes a dictionary from a page
     * @param dictionaryPage
     * @return the corresponding dictionary
     */
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
        throw new UnsupportedOperationException(this.name() + " does not support dictionary");
    }

    /**
     * To read decoded values that don't require a dictionary
     *
     * @param descriptor the column to read
     * @param valuesType the type of values
     * @return the proper values reader for the given column
     * @throw {@link UnsupportedOperationException} if the encoding is dictionary based
     */
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
        throw new UnsupportedOperationException("Error decoding " + descriptor + ". " + this.name() + " is dictionary based");
    }

    /**
     * To read decoded values that require a dictionary
     *
     * @param descriptor the column to read
     * @param valuesType the type of values
     * @param dictionary the dictionary
     * @return the proper values reader for the given column
     * @throw {@link UnsupportedOperationException} if the encoding is not dictionary based
     */
    public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
        throw new UnsupportedOperationException(this.name() + " is not dictionary based");
    }

}
