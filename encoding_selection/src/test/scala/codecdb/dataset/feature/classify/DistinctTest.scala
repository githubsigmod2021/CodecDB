package codecdb.dataset.feature.classify

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/14/17.
  */
class DistinctTest {

  @Test
  def testExtract: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_str2.data").toURI
    val features = Distinct.extract(col).toArray

    assertEquals(2, features.length)
    assertEquals("Distinct", features(0).featureType)
    assertEquals("count", features(0).name)
    assertEquals(7, features(0).value, 0.001)

    assertEquals("Distinct", features(1).featureType)
    assertEquals("ratio", features(1).name)
    assertEquals(0.7, features(1).value, 0.001)
  }

  @Test
  def testEmptyInput: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_empty.dat").toURI
    val features = Distinct.extract(col, "abc_").toArray

    assertEquals(2, features.length)
    assertEquals("abc_Distinct", features(0).featureType)
    assertEquals("count", features(0).name)
    assertEquals(1, features(0).value, 0.001)

    assertEquals("abc_Distinct", features(1).featureType)
    assertEquals("ratio", features(1).name)
    assertEquals(0.0417, features(1).value, 0.001)
  }
}
