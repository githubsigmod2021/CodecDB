package codecdb.query.tpch;

import codecdb.parquet.EncReaderProcessor;
import codecdb.parquet.ParquetReaderHelper;
import codecdb.util.perf.ProfileBean;
import codecdb.model.FloatEncoding;
import codecdb.model.IntEncoding;
import codecdb.model.StringEncoding;
import codecdb.parquet.EncReaderProcessor;
import codecdb.parquet.ParquetReaderHelper;
import codecdb.util.perf.ProfileBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TPCHWorker {

    private CompressionCodecName[] codecs =
            {CompressionCodecName.UNCOMPRESSED, CompressionCodecName.GZIP};

    private EncReaderProcessor processor;

    private MessageType schema;

    private Configuration configuration;

    public TPCHWorker(EncReaderProcessor processor, MessageType schema) {
        this(new Configuration(), processor, schema);
    }

    public TPCHWorker(Configuration conf, EncReaderProcessor processor, MessageType schema) {
        this.configuration = conf;
        this.processor = processor;
        this.schema = schema;
    }

    public void work(String filePath) throws Exception {
        for (int i = 0; i < schema.getColumns().size(); i++) {
            ColumnDescriptor cd = schema.getColumns().get(i);
            readColumn(filePath, i + 1, cd);
        }
    }

    protected void readColumn(String main, int index, ColumnDescriptor cd) throws Exception {
        List<String> encodings = null;
        switch (cd.getType()) {
            case BINARY:
                encodings = Arrays.stream(StringEncoding.values())
                        .filter(p -> p.parquetEncoding() != null).map(e -> e.name())
                        .collect(Collectors.toList());
                break;
            case INT32:
                encodings = Arrays.stream(IntEncoding.values())
                        .filter(p -> p.parquetEncoding() != null).map(e -> e.name())
                        .collect(Collectors.toList());
                break;
            case DOUBLE:
                encodings = Arrays.stream(FloatEncoding.values())
                        .filter(p -> p.parquetEncoding() != null).map(e -> e.name())
                        .collect(Collectors.toList());
                break;
            default:
                encodings = Collections.<String>emptyList();
                break;
        }

        for (String e : encodings) {
            for (CompressionCodecName codec : codecs) {
                String fileName = MessageFormat.format("{0}.col{1}.{2}_{3}",
                        main, index, e, codec.name());
                URI fileURI = new URI(fileName);
                FileSystem fs = FileSystem.get(fileURI, configuration);
                Path filePath = new Path(fileURI);
                long fileLength;
                // Skip non-existing and empty file
                if (!fs.exists(filePath) || (fileLength = fs.getFileStatus(filePath).getLen()) == 0) {
                    continue;
                }
                try {
                    ProfileBean loadTime = ParquetReaderHelper.profile(
                            configuration, fileURI, processor);
                    System.out.println(MessageFormat.format("{0}, {1}, {2}, {3}, {4,number,#.###}",
                            index, e, codec.name(),
                            String.valueOf(loadTime.wallclock()),
                            ((double) fileLength) / (1000 * loadTime.wallclock())));
                } catch (Exception ex) {
                    // ignore
                    ex.printStackTrace();
                }
            }
        }
    }
}
