package codecdb.dataset.feature.resource

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

class EncFileSizeTest {

  @Test
  def testExtractInt: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(5, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(new File("src/test/resource/coldata/test_col_int.data.PLAIN").length(), fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(new File("src/test/resource/coldata/test_col_int.data.DICT").length(), fa(1).value, 0.001)

    assertTrue(fa(2).featureType.equals("EncFileSize"))
    assertEquals("BP_file_size", fa(2).name)
    assertEquals(new File("src/test/resource/coldata/test_col_int.data.BP").length(), fa(2).value, 0.001)

    assertTrue(fa(3).featureType.equals("EncFileSize"))
    assertEquals("RLE_file_size", fa(3).name)
    assertEquals(new File("src/test/resource/coldata/test_col_int.data.RLE").length(), fa(3).value, 0.001)

    assertTrue(fa(4).featureType.equals("EncFileSize"))
    assertEquals("DELTABP_file_size", fa(4).name)
    assertEquals(new File("src/test/resource/coldata/test_col_int.data.DELTABP").length(), fa(4).value, 0.001)
  }

  @Test
  def testExtractLong: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
    col.colFile = new File("src/test/resource/coldata/test_col_long.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(3, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(new File("src/test/resource/coldata/test_col_long.data.PLAIN").length(), fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(new File("src/test/resource/coldata/test_col_long.data.DICT").length(), fa(1).value, 0.001)

    assertTrue(fa(2).featureType.equals("EncFileSize"))
    assertEquals("DELTABP_file_size", fa(2).name)
    assertEquals(new File("src/test/resource/coldata/test_col_long.data.DELTABP").length(), fa(2).value, 0.001)

  }

  @Test
  def testExtractDouble: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(2, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(new File("src/test/resource/coldata/test_col_double.data.PLAIN").length(), fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(new File("src/test/resource/coldata/test_col_double.data.DICT").length(), fa(1).value, 0.001)

  }

  @Test
  def testExtractString:Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(4, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(new File("src/test/resource/coldata/test_col_str.data.PLAIN").length(), fa(0).value, 0.001)

    assertTrue(fa(1).featureType.equals("EncFileSize"))
    assertEquals("DICT_file_size", fa(1).name)
    assertEquals(new File("src/test/resource/coldata/test_col_str.data.DICT").length(), fa(1).value, 0.001)

    assertTrue(fa(2).featureType.equals("EncFileSize"))
    assertEquals("DELTA_file_size", fa(2).name)
    assertEquals(new File("src/test/resource/coldata/test_col_str.data.DELTA").length(), fa(2).value, 0.001)

    assertTrue(fa(3).featureType.equals("EncFileSize"))
    assertEquals("DELTAL_file_size", fa(3).name)
    assertEquals(new File("src/test/resource/coldata/test_col_str.data.DELTAL").length(), fa(3).value, 0.001)

  }

  @Test
  def testExtractBoolean:Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.BOOLEAN)
    col.colFile = new File("src/test/resource/coldata/test_col_boolean.data").toURI

    val feature = ParquetEncFileSize.extract(col)
    assertEquals(1, feature.size)
    val fa = feature.toArray

    assertTrue(fa(0).featureType.equals("EncFileSize"))
    assertEquals("PLAIN_file_size", fa(0).name)
    assertEquals(new File("src/test/resource/coldata/test_col_boolean.data.PLAIN").length(), fa(0).value, 0.001)

  }
}