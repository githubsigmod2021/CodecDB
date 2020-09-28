package codecdb

import java.io.File
import java.util.zip.ZipFile

import org.junit.Assert._
import org.junit.Test

class ConfigTest {

  @Test
  def testExtract: Unit = {
    val dest = new File("src/test/resource/extractzip/int_model")

    dest.deleteOnExit()
    ZipUtils.extractFolder(new ZipFile(new File("src/test/resource/extractzip/Archive.zip")),
      "int_model", dest)

    assertEquals(40255, new File("src/test/resource/extractzip/int_model/saved_model.pb").length())
    assertEquals(511, new File("src/test/resource/extractzip/int_model/variables/variables.index").length())
    assertEquals(300068, new File("src/test/resource/extractzip/int_model/variables/variables.data-00000-of-00001").length())
  }
}
