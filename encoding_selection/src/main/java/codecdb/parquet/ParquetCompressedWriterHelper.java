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

import codecdb.model.FloatEncoding;
import codecdb.model.IntEncoding;
import codecdb.model.LongEncoding;
import codecdb.model.StringEncoding;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class ParquetCompressedWriterHelper {

    public static URI singleColumnBoolean(URI input, CompressionCodecName codec) throws IOException {
        File output = ParquetWriterHelper.genOutput(input, String.format("PLAIN_%s", codec.name()));
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "value"));

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(
                new Path(output.toURI()), schema, codec);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnInt(URI input, IntEncoding encoding, CompressionCodecName codec) throws IOException {
        File output = ParquetWriterHelper.genOutput(input, String.format("%s_%s", encoding.name(), codec.name()));
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());
        int bitLength = ParquetWriterHelper.scanIntBitLength(input);
        int bound = (1 << bitLength) - 1;
        EncContext.context.get().put(type, new Object[]{String.valueOf(bitLength), String.valueOf(bound)});

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(
                new Path(output.toURI()), schema, codec);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnLong(URI input, LongEncoding encoding, CompressionCodecName codec)
            throws IOException {
        File output = ParquetWriterHelper.genOutput(input, String.format("%s_%s", encoding.name(), codec.name()));
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());
        int bitLength = ParquetWriterHelper.scanLongBitLength(input);
        int bound = (1 << bitLength) - 1;
        EncContext.context.get().put(type, new Object[]{String.valueOf(bitLength), String.valueOf(bound)});

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(
                new Path(output.toURI()), schema, codec);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnString(URI input, StringEncoding encoding, CompressionCodecName codec)
            throws IOException {
        File output = ParquetWriterHelper.genOutput(input, String.format("%s_%s", encoding.name(), codec.name()));
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(
                new Path(output.toURI()), schema, codec);


        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnDouble(URI input, FloatEncoding encoding, CompressionCodecName codec)
            throws IOException {
        File output = ParquetWriterHelper.genOutput(input, String.format("%s_%s", encoding.name(), codec.name()));
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(
                new Path(output.toURI()), schema, codec);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnFloat(URI input, FloatEncoding encoding, CompressionCodecName codec)
            throws IOException {
        File output = ParquetWriterHelper.genOutput(input, String.format("%s_%s", encoding.name(), codec.name()));
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(
                new Path(output.toURI()), schema, codec);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }
}
