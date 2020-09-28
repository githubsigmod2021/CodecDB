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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetWriterHelper {

    public static File genOutput(URI input, String suffix) {
        try {
            if (input.getPath().endsWith("\\.data")) {
                return new File(new URI(input.toString().replaceFirst("data$", suffix)));
            }
            return new File(new URI(input.toString() + "." + suffix));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Scan the file containing integer/long and determine the bit length
     *
     * @param input the file to scan
     * @return correct int bit length
     */
    public static int scanIntBitLength(URI input) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(input)));
            int maxBitLength = 0;
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;
                int number = Integer.parseInt(line);
                int bitLength = 32 - Integer.numberOfLeadingZeros(number);
                if (bitLength > maxBitLength)
                    maxBitLength = bitLength;
            }
            br.close();
            return maxBitLength;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int scanLongBitLength(URI input) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(input)));
            int maxBitLength = 0;
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;
                long number = Long.parseLong(line);
                int bitLength = 64 - Long.numberOfLeadingZeros(number);
                if (bitLength > maxBitLength)
                    maxBitLength = bitLength;
            }
            br.close();
            return maxBitLength;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void write(URI input, MessageType schema, URI output,
                             String split, boolean skipHeader) throws IOException {
        write(input, schema, output, split, skipHeader, CompressionCodecName.UNCOMPRESSED);
    }

    public static void write(URI input, MessageType schema,
                             URI output, String split, boolean skipHeader,
                             CompressionCodecName codec) throws IOException {
        File outfile = new File(output);
        if (outfile.exists())
            outfile.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildCompressed(new Path(output), schema, codec);

        // Skip header line
        String line = skipHeader ? reader.readLine() : null;

        while ((line = reader.readLine()) != null) {
            String[] dataArray = line.trim().split(split);
            List<String> data = Arrays.asList(dataArray);
            writer.write(data);
        }

        reader.close();
        writer.close();
    }

    public static URI singleColumnBoolean(URI input) throws IOException {
        File output = genOutput(input, "PLAIN");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "value"));

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

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

    public static URI singleColumnInt(URI input, IntEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());
        int bitLength = scanIntBitLength(input);
        int bound = (1 << bitLength) - 1;
        EncContext.context.get().put(type, new Object[]{String.valueOf(bitLength), String.valueOf(bound)});

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

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

    public static URI singleColumnLong(URI input, LongEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());
        int bitLength = scanLongBitLength(input);
        int bound = (1 << bitLength) - 1;
        EncContext.context.get().put(type, new Object[]{String.valueOf(bitLength), String.valueOf(bound)});

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

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

    public static URI singleColumnString(URI input, StringEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);


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

    public static URI singleColumnDouble(URI input, FloatEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

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

    public static URI singleColumnFloat(URI input, FloatEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

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
