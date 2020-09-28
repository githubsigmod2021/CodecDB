package codecdb.dataset.feature.classify

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

class AvgRunLengthTest {

  @Test
  def testExtract: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/feature/test_col_avgrl.data").toURI
    val features = AvgRunLength.extract(col).toArray

    assertEquals(1, features.length)
    assertEquals("AvgRunLength", features(0).featureType)
    assertEquals("value", features(0).name)
    assertEquals(1.9, features(0).value, 0.001)

  }
}
