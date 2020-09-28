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

package codecdb.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public abstract class EncReaderProcessor implements ReaderProcessor {

    protected MessageType schema;

    /**
     * Load data from file footer and put that in ThreadLocal for decoding purpose
     *
     * @param footer
     */
    @Override
    public void processFooter(Footer footer) {
        Map<String, String> meta = footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData();
        schema = footer.getParquetMetadata().getFileMetaData().getSchema();

        schema.getColumns().forEach(col -> {
            if (meta.containsKey(String.format("%s.0", col.toString()))) {
                String data1 = meta.get(String.format("%s.0", col.toString()));
                String data2 = meta.get(String.format("%s.1", col.toString()));
                EncContext.context.get().put(col.toString(), new Object[]{data1, data2});
            }
        });

    }

    public static final Object[] getContext(URI encodedFile) throws IOException {
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(),
                new Path(encodedFile), ParquetMetadataConverter.NO_FILTER);
        MessageType schema = footer.getFileMetaData().getSchema();
        Map<String, String> meta = footer.getFileMetaData().getKeyValueMetaData();

        Object[] result = new Object[schema.getFields().size()];
        for (int i = 0; i < result.length; i++) {
            ColumnDescriptor col = schema.getColumns().get(i);
            if (meta.containsKey(String.format("%s.0", col.toString()))) {
                String data1 = meta.get(String.format("%s.0", col.toString()));
                String data2 = meta.get(String.format("%s.1", col.toString()));
                result[i] = new Object[]{data1, data2};
            }
        }
        return result;
    }

    private int expectNumThread = 0;
    public int expectNumThread() {
        return expectNumThread;
    }
    public void setExpectNumThread(int e) {
        this.expectNumThread = e;
    }
}
