package codecdb.dataset.feature.classify

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

class LengthTest {
  @Test
  def testRun: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_str2.data").toURI

    val features = Length.extract(col).toArray

    assertEquals(4, features.length)
    assertEquals("max", features(0).name)
    assertEquals(32, features(0).value, 0.001)
    assertEquals("min", features(1).name)
    assertEquals(8, features(1).value, 0.001)
    assertEquals("mean", features(2).name)
    assertEquals(23.167, features(2).value, 0.001)
    assertEquals("variance", features(3).name)
    assertEquals(68.4723, features(3).value, 0.001)
  }

  @Test
  def testEmptyInput: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_empty.dat").toURI

    val features = Length.extract(col).toArray

    assertEquals(4, features.length)
    assertEquals("max", features(0).name)
    assertEquals(0, features(0).value, 0.001)
    assertEquals("min", features(1).name)
    assertEquals(0, features(1).value, 0.001)
    assertEquals("mean", features(2).name)
    assertEquals(0, features(2).value, 0.001)
    assertEquals("variance", features(3).name)
    assertEquals(0, features(3).value, 0.001)
  }
}