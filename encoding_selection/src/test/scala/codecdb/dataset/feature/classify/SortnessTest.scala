package codecdb.dataset.feature.classify

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Created by harper on 5/3/17.
  */
class SortnessTest {

  @Test
  def testExtract: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI
    val features = new Sortness(2).extract(col).toArray

    assertEquals(5, features.length)

    assertEquals("Sortness", features(0).featureType)
    assertEquals("totalpair_2", features(0).name)
    assertEquals(6, features(0).value, 0.001)

    assertEquals("Sortness", features(1).featureType)
    assertEquals("ivpair_2", features(1).name)
    assertEquals(0.6667, features(1).value, 0.001)

    assertEquals("Sortness", features(2).featureType)
    assertEquals("kendalltau_2", features(2).name)
    assertEquals(-0.3333, features(2).value, 0.001)

    assertEquals("Sortness", features(3).featureType)
    assertEquals("numitem_2", features(3).name)
    assertEquals(13, features(3).value, 0.001)

    assertEquals("Sortness", features(4).featureType)
    assertEquals("spearmanrho_2", features(4).name)
    assertEquals(0.9780, features(4).value, 0.001)

    val features2 = new Sortness(-1).extract(col).toArray

    assertEquals(5, features2.length)

    assertEquals("Sortness", features2(0).featureType)
    assertEquals("totalpair_-1", features2(0).name)
    assertEquals(78, features2(0).value, 0.001)

    assertEquals("Sortness", features2(1).featureType)
    assertEquals("ivpair_-1", features2(1).name)
    assertEquals(0.6923, features2(1).value, 0.001)

    assertEquals("Sortness", features2(2).featureType)
    assertEquals("kendalltau_-1", features2(2).name)
    assertEquals(0.3077, features2(2).value, 0.001)

    assertEquals("Sortness", features2(3).featureType)
    assertEquals("numitem_-1", features2(3).name)
    assertEquals(13, features2(3).value, 0.001)

    assertEquals("Sortness", features2(4).featureType)
    assertEquals("spearmanrho_-1", features2(4).name)
    assertEquals(0.4286, features2(4).value, 0.001)
  }

  @Test
  def testExtractEmptyColumn: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_empty.dat").toURI
    val features = new Sortness(2).extract(col).toArray

    assertEquals(5, features.length)

    assertEquals("Sortness", features(0).featureType)
    assertEquals("totalpair_2", features(0).name)
    assertEquals(12, features(0).value, 0.001)

    assertEquals("Sortness", features(1).featureType)
    assertEquals("ivpair_2", features(1).name)
    assertEquals(0, features(1).value, 0.001)

    assertEquals("Sortness", features(2).featureType)
    assertEquals("kendalltau_2", features(2).name)
    assertEquals(1, features(2).value, 0.001)

    assertEquals("Sortness", features(3).featureType)
    assertEquals("numitem_2", features(3).name)
    assertEquals(24, features(3).value, 0.001)

    assertEquals("Sortness", features(4).featureType)
    assertEquals("spearmanrho_2", features(4).name)
    assertEquals(1, features(4).value, 0.001)
  }

  @Test
  def testComputeInvertPair: Unit = {
    val input = Seq(1, 3, 0, 2, 5, 7, 8, 9, 6)

    val (inv, total) = Sortness.computeInvertPair(input.map(_.toString), DataType.INTEGER.comparator())

    assertEquals(6, inv)
    assertEquals(36, total)
  }

  @Test
  def testComputeDiffRank: Unit = {
    val input = Seq(1, 4, 3, 2, 6, 7, 5, 9)

    val diffRank = Sortness.computeDiffRank(input.map(_.toString), DataType.INTEGER.comparator())

    assertEquals(14, diffRank)
  }
}
