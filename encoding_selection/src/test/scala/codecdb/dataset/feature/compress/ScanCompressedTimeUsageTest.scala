package codecdb.dataset.feature.compress

import java.io.File

import codecdb.dataset.column.Column
import codecdb.dataset.feature.Feature
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test


//object ScanCompressedTimeUsageTest {
//
//  @BeforeClass
//  def warmup: Unit = {
//    val select = new VerticalSelect() {
//      override def createRecorder(schema: MessageType) = new NostoreColumnTempTable(schema)
//    };
//    val anyfile = new File("src/test/resource/scantime/double.data.DICT").toURI
//    val schema = new MessageType("default",
//      new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value")
//    )
//    val p = (data: Any) => {
//      true
//    }
//    select.select(anyfile, new VColumnPredicate(p, 0), schema, Array(0))
//  }
//}

class ScanCompressedTimeUsageTest {
  val codecs = Array("SNAPPY", "GZIP", "LZO")

  @Test
  def testExtractInt: Unit = {
    val encs = Array("PLAIN", "DICT", "BP", "RLE", "DELTABP")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/scantime/int.data").toURI

    col.features.add(new Feature(ParquetCompressFileSize.featureType, "demo", 0))

    val feature = ScanCompressedTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("ScanTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wallclock".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cpu".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_user".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractString: Unit = {
    val encs = Array("PLAIN", "DICT", "DELTA", "DELTAL")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/scantime/str.data").toURI

    col.features.add(new Feature(ParquetCompressFileSize.featureType, "demo", 0))

    val feature = ScanCompressedTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("ScanTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wallclock".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cpu".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_user".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractLong: Unit = {
    val encs = Array("PLAIN", "DICT", "DELTABP")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
    col.colFile = new File("src/test/resource/scantime/long.data").toURI

    col.features.add(new Feature(ParquetCompressFileSize.featureType, "demo", 0))

    val feature = ScanCompressedTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("ScanTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wallclock".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cpu".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_user".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }

  @Test
  def testExtractDouble: Unit = {
    val encs = Array("PLAIN", "DICT")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/scantime/double.data").toURI

    col.features.add(new Feature(ParquetCompressFileSize.featureType, "demo", 0))

    val feature = ScanCompressedTimeUsage.extract(col)
    assertEquals(encs.size * codecs.size * 3, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("ScanTimeUsage", fa(p._2 * 3).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 1).featureType)
      assertEquals("ScanTimeUsage", fa(p._2 * 3 + 2).featureType)
      assertEquals("%s_wallclock".format(name), fa(p._2 * 3).name)
      assertEquals("%s_cpu".format(name), fa(p._2 * 3 + 1).name)
      assertEquals("%s_user".format(name), fa(p._2 * 3 + 2).name)
      assertTrue(fa(p._2 * 3).value > 0)
      assertTrue(fa(p._2 * 3 + 1).value > 0)
      assertTrue(fa(p._2 * 3 + 2).value >= 0)
    })
  }
}
