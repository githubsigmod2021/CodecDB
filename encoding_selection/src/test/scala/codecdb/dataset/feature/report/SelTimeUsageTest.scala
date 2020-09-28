package codecdb.dataset.feature.report

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

class SelTimeUsageTest {

  @Test
  def testExtractInt: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = SelTimeUsage.extract(col)
    assertEquals(6, feature.size)
    val fa = feature.toArray

    val names = Array("cpu1", "wc1", "user1", "cpu2", "wc2", "user2")
    for (i <- 0 to 5) {
      assertEquals("SelTimeUsage", fa(i).featureType)
      assertEquals(names(i), fa(i).name)
    }
    // Do it again
    val feature2 = SelTimeUsage.extract(col)
    assertEquals(6, feature2.size)
    val fa2 = feature2.toArray

    for (i <- 0 to 5) {
      assertEquals("SelTimeUsage", fa2(i).featureType)
      assertEquals(names(i), fa2(i).name)
    }
  }

  @Test
  def testExtractString: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

    val feature = SelTimeUsage.extract(col)
    assertEquals(6, feature.size)
    val fa = feature.toArray

    val names = Array("cpu1", "wc1", "user1", "cpu2", "wc2", "user2")
    for (i <- 0 to 5) {
      assertEquals("SelTimeUsage", fa(i).featureType)
      assertEquals(names(i), fa(i).name)
    }
  }
}
