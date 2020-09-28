package codecdb.parquet;

import codecdb.util.perf.ProfileBean;
import codecdb.util.perf.Profiler;
import codecdb.util.perf.ProfileBean;
import codecdb.util.perf.Profiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParquetReaderHelper {

    public static void read(URI file, ReaderProcessor processor) throws IOException,
            VersionParser.VersionParseException {
        read(new Configuration(), file, processor);
    }

    public static void read(Configuration conf, URI file, ReaderProcessor processor) throws IOException,
            VersionParser.VersionParseException {
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader
                .readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return;
        }

        ExecutorService threadPool = processor.expectNumThread() > 0 ?
                Executors.newFixedThreadPool(processor.expectNumThread()) :
                null;

        for (Footer footer : footers) {
            processor.processFooter(footer);
            VersionParser.ParsedVersion version = VersionParser.parse(
                    footer.getParquetMetadata().getFileMetaData().getCreatedBy());

            ParquetFileReader fileReader = ParquetFileReader
                    .open(conf, footer.getFile(), footer.getParquetMetadata());
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata()
                    .getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                final PageReadStore thisrowgroup = rowGroup;
                BlockMetaData blockMeta = footer.getParquetMetadata().getBlocks().get(blockCounter);
                if (processor.expectNumThread() == 0) {
                    processor.processRowGroup(version, blockMeta, thisrowgroup);
                } else {
                    threadPool.submit(() -> processor.processRowGroup(version, blockMeta, thisrowgroup));
                }
                blockCounter++;
            }
        }
        if (null != threadPool) {
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static ProfileBean profile(URI file, ReaderProcessor processor) throws IOException,
            VersionParser.VersionParseException {
        return profile(new Configuration(), file, processor);
    }

    public static ProfileBean profile(Configuration conf,
                                      URI file, ReaderProcessor processor) throws IOException,
            VersionParser.VersionParseException {
        Profiler p = new Profiler();
        p.mark();
        read(conf, file, processor);
        return p.stop();
    }

    public static void readColumn(PrimitiveType type, ColumnDescriptor cd, PageReader pageReader,
                                  VersionParser.ParsedVersion version,
                                  PrimitiveConverter converter) {
        ColumnReaderImpl reader = new ColumnReaderImpl(cd, pageReader, converter, version);
        if (type.getRepetition() == Type.Repetition.REQUIRED) {
            for (int i = 0; i < pageReader.getTotalValueCount(); i++) {
                reader.writeCurrentValueToConverter();
                reader.consume();
            }
        } else {
            for (int i = 0; i < pageReader.getTotalValueCount(); i++) {
                if (reader.getCurrentDefinitionLevel() < cd.getMaxDefinitionLevel()) {
                    reader.skip();
                } else {
                    reader.writeCurrentValueToConverter();
                }
                reader.consume();
            }
        }
    }

}
