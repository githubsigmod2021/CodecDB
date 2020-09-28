package codecdb.dataset.feature.compress

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class ParquetCompressFileSizeTest {

  val codecs = Array("SNAPPY", "GZIP", "LZO")

  @Test
  def testExtractInt: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = ParquetCompressFileSize.extract(col)
    assertEquals(15, feature.size)
    val fa = feature.toArray

    val encs = Array("PLAIN", "DICT", "BP", "RLE", "DELTABP")

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertTrue(fa(p._2).featureType.equals("CompressEncFileSize"))
      assertEquals("%s_file_size".format(name), fa(p._2).name)
      assertEquals(new File("src/test/resource/coldata/test_col_int.data.%s".format(name)).length(), fa(p._2).value, 0.001)
    })
  }

  @Test
  def testExtractString: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

    val feature = ParquetCompressFileSize.extract(col)
    assertEquals(12, feature.size)
    val fa = feature.toArray

    val encs = Array("PLAIN", "DICT", "DELTA", "DELTAL")

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertTrue(fa(p._2).featureType.equals("CompressEncFileSize"))
      assertEquals("%s_file_size".format(name), fa(p._2).name)
      assertEquals(new File("src/test/resource/coldata/test_col_str.data.%s".format(name)).length(), fa(p._2).value, 0.001)
    })
  }

  @Test
  def testExtractLong: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
    col.colFile = new File("src/test/resource/coldata/test_col_long.data").toURI

    val feature = ParquetCompressFileSize.extract(col)
    assertEquals(9, feature.size)
    val fa = feature.toArray

    val encs = Array("PLAIN", "DICT", "DELTABP")

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertTrue(fa(p._2).featureType.equals("CompressEncFileSize"))
      assertEquals("%s_file_size".format(name), fa(p._2).name)
      assertEquals(new File("src/test/resource/coldata/test_col_long.data.%s".format(name)).length(), fa(p._2).value, 0.001)
    })
  }

  @Test
  def testExtractDouble: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

    val feature = ParquetCompressFileSize.extract(col)
    assertEquals(6, feature.size)
    val fa = feature.toArray

    val encs = Array("PLAIN", "DICT")

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertTrue(fa(p._2).featureType.equals("CompressEncFileSize"))
      assertEquals("%s_file_size".format(name), fa(p._2).name)
      assertEquals(new File("src/test/resource/coldata/test_col_double.data.%s".format(name)).length(), fa(p._2).value, 0.001)
    })
  }

  @Test
  def testExtractBoolean: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.BOOLEAN)
    col.colFile = new File("src/test/resource/coldata/test_col_boolean.data").toURI

    val feature = ParquetCompressFileSize.extract(col)
    assertEquals(3, feature.size)
    val fa = feature.toArray

    val encs = Array("PLAIN")

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertTrue(fa(p._2).featureType.equals("CompressEncFileSize"))
      assertEquals("%s_file_size".format(name), fa(p._2).name)
      assertEquals(new File("src/test/resource/coldata/test_col_boolean.data.%s".format(name)).length(), fa(p._2).value, 0.001)
    })

  }

  @Test
  def testExtractFloat: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.FLOAT)
    col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

    val feature = ParquetCompressFileSize.extract(col)
    assertEquals(6, feature.size)
    val fa = feature.toArray

    val encs = Array("PLAIN", "DICT")

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertTrue(fa(p._2).featureType.equals("CompressEncFileSize"))
      assertEquals("%s_file_size".format(name), fa(p._2).name)
      assertEquals(new File("src/test/resource/coldata/test_col_double.data.%s".format(name)).length(), fa(p._2).value, 0.001)
    })
  }
}
