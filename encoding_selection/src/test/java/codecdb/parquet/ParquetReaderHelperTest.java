package codecdb.parquet;

import codecdb.query.MemBufferPrimitiveConverter;
import codecdb.query.MemBufferPrimitiveConverter;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.io.File;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class ParquetReaderHelperTest {

    @Test
    public void testRead() throws Exception {
        URI input = new File("src/test/resource/query_select/customer_100.parquet").toURI();

        MemBufferPrimitiveConverter converter = new MemBufferPrimitiveConverter();

        ParquetReaderHelper.read(input,
                new EncReaderProcessor() {

                    @Override
                    public int expectNumThread() {
                        return 20;
                    }

                    @Override
                    public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {
                        ColumnDescriptor cd = schema.getColumns().get(2);
                        ColumnReaderImpl impl = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd), converter, version);

                        for (int i = 0; i < rowGroup.getRowCount(); i++) {
                            if (impl.getCurrentDefinitionLevel() < cd.getMaxDefinitionLevel()) {
                                impl.skip();
                            } else {
                                impl.writeCurrentValueToConverter();
                            }
                            impl.consume();
                        }
                    }
                });
        assertEquals(100, converter.getBuffer().size());
    }
}
