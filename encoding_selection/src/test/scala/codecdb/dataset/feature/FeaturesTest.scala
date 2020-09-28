package codecdb.dataset.feature

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

import scala.io.Source

class FeaturesTest {

  @Test
  def testFeatures: Unit = {
    val col = new Column(new File("src/test/resource/coldata/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val features = Features.extract(col)
    val fa = features.toArray

    assertTrue(fa(5).featureType.equals("Sparsity"))
    assertEquals("count", fa(5).name)
    assertEquals(13.0, fa(5).value, 0.001)

    assertTrue(fa(6).featureType.equals("Sparsity"))
    assertEquals("empty_count", fa(6).name)
    assertEquals(0.0, fa(6).value, 0.001)

    assertTrue(fa(7).featureType.equals("Sparsity"))
    assertEquals("valid_ratio", fa(7).name)
    assertEquals(1.0, fa(7).value, 0.001)

    assertTrue(fa(8).featureType.equals("Entropy"))
    assertEquals("line_max", fa(8).name)
    assertEquals(1.332, fa(8).value, 0.001)

    assertTrue(fa(9).featureType.equals("Entropy"))
    assertEquals("line_min", fa(9).name)
    assertEquals(0.636, fa(9).value, 0.001)

  }

  @Test
  def testFeaturesWithFilter: Unit = {
    val col = new Column(new File("src/test/resource/coldata/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val filter = Filter.firstNFilter(5)

    val features = Features.extract(col, filter, "ak_")
    val fa = features.toArray

    assertEquals(29, fa.size)

    assertEquals("ak_Sparsity", fa(0).featureType)
    assertEquals("count", fa(0).name)
    assertEquals(5.0, fa(0).value, 0.001)

    assertEquals("ak_Sparsity", fa(1).featureType)
    assertEquals("empty_count", fa(1).name)
    assertEquals(0.0, fa(1).value, 0.001)

    assertEquals("ak_Sparsity", fa(2).featureType)
    assertEquals("valid_ratio", fa(2).name)
    assertEquals(1.0, fa(2).value, 0.001)

    assertEquals("ak_Entropy", fa(3).featureType)
    assertEquals("line_max", fa(3).name)
    assertEquals(1.055, fa(3).value, 0.001)

    assertEquals("ak_Entropy", fa(4).featureType)
    assertEquals("line_min", fa(4).name)
    assertEquals(0.693, fa(4).value, 0.001)

  }

  @Test
  def testFilterFile: Unit = {
    Features.filterFile(new File("src/test/resource/coldata/test_col_int.data").toURI,
      new File("src/test/resource/coldata/test_col_int.data.filtered").toURI, Filter.firstNFilter(5))

    val lines = Source.fromFile(new File("src/test/resource/coldata/test_col_int.data.filtered")).getLines().toArray
    assertEquals(5,lines.size)

    assertEquals("34",lines(0))
    assertEquals("24",lines(1))
    assertEquals("24",lines(2))
    assertEquals("2424",lines(3))
    assertEquals("23234",lines(4))
  }
}

class FeatureTest {
  @Test
  def testEqualHash: Unit = {

    val fa = new Feature
    val fb = new Feature

    fa.featureType = "ABC"
    fa.name = "ttt"

    fb.featureType = "ABC"
    fb.name = "ttt"

    assertTrue(fa.equals(fb))
    assertEquals(fa.hashCode(), fb.hashCode())
  }
}