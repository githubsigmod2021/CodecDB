package codecdb.parquet;

import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static org.junit.Assert.assertEquals;

public class ParquetTupleVReaderTest {

    @Test
    public void testRead() throws Exception {
        ParquetTupleVReader reader = new ParquetTupleVReader(
                new File("src/test/resource/subtable/part_20.parquet").toURI());

        BufferedReader originReader = new BufferedReader(new FileReader(new File("src/test/resource/subtable/part_20")));

        for (int i = 0; i < 20; i++) {
            String[] origin = originReader.readLine().split("\\|");
            Object[] encoded = reader.read();
            for (int j = 0; j < origin.length; j++) {
                String o = origin[j];
                if (encoded[j] instanceof Binary) {
                    assertEquals(o.trim(), ((Binary) encoded[j]).toStringUsingUTF8());
                } else if (encoded[j] instanceof Double) {
                    assertEquals(Double.valueOf(o), ((Double) encoded[j]), 0.001);
                } else {
                    assertEquals(o, encoded[j].toString());
                }
            }
        }
        reader.close();
        originReader.close();
    }

    @Test
    public void testSubtable() throws Exception {
        ParquetTupleVReader reader = new ParquetTupleVReader(
                new File("src/test/resource/subtable/subtable").toURI());
        Object[] result = null;
        int counter = 0;
        while ((result = reader.read()) != null) {
            counter++;
        }
        assertEquals(reader.getNumOfRecords(), counter);
    }
}
