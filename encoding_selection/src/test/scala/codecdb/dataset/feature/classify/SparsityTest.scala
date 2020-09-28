package codecdb.dataset.feature.classify

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

class SparsityTest {

  @Test
  def testRun(): Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/feature/test_col_sparsity.data").toURI

    val features = Sparsity.extract(col)
    assertEquals(3, features.size)

    val farray = features.toArray

    assertEquals("Sparsity", farray(0).featureType)
    assertEquals("count", farray(0).name)
    assertEquals(14, farray(0).value, 0.01)

    assertEquals("Sparsity", farray(1).featureType)
    assertEquals("empty_count", farray(1).name)
    assertEquals(4, farray(1).value, 0.01)

    assertEquals("Sparsity", farray(2).featureType)
    assertEquals("valid_ratio", farray(2).name)
    assertEquals(0.7142, farray(2).value, 0.01)
  }

  @Test
  def testEmptyInput: Unit = {
    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_empty.dat").toURI

    val features = Sparsity.extract(col, "")
    assertEquals(3, features.size)

    val farray = features.toArray

    assertEquals("Sparsity", farray(0).featureType)
    assertEquals("count", farray(0).name)
    assertEquals(24, farray(0).value, 0.01)

    assertEquals("Sparsity", farray(1).featureType)
    assertEquals("empty_count", farray(1).name)
    assertEquals(24, farray(1).value, 0.01)

    assertEquals("Sparsity", farray(2).featureType)
    assertEquals("valid_ratio", farray(2).name)
    assertEquals(0, farray(2).value, 0.01)
  }
}