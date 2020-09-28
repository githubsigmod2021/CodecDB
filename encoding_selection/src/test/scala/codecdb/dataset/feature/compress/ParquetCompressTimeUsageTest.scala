package codecdb.dataset.feature.compress

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

class ParquetCompressTimeUsageTest {

  val codecs = Array("SNAPPY", "GZIP", "LZO")

  @Test
  def testExtractInt: Unit = {
    val encs = Array("PLAIN", "DICT", "BP", "RLE", "DELTABP")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = ParquetCompressTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("CompressTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractString: Unit = {
    val encs = Array("PLAIN", "DICT", "DELTA", "DELTAL")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

    val feature = ParquetCompressTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("CompressTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractLong: Unit = {
    val encs = Array("PLAIN", "DICT", "DELTABP")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
    col.colFile = new File("src/test/resource/coldata/test_col_long.data").toURI

    val feature = ParquetCompressTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("CompressTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractDouble: Unit = {
    val encs = Array("PLAIN", "DICT")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

    val feature = ParquetCompressTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("CompressTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractBoolean: Unit = {
    val encs = Array("PLAIN")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.BOOLEAN)
    col.colFile = new File("src/test/resource/coldata/test_col_boolean.data").toURI

    val feature = ParquetCompressTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("CompressTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }
}
