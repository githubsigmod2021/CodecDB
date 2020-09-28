package codecdb.util

import org.junit.Test
import org.junit.Assert._
import org.nd4j.linalg.factory.Nd4j

class WordUtilsTest {

  @Test
  def testLevDist2: Unit = {
    val a = "actual"
    val b = "actua"

    assertEquals(0.79, WordUtils.levDistance2(a, b), 0.01)
  }


}