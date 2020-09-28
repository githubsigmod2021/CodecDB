package codecdb.ptnmining.preprocess

import codecdb.wordvec.FileWordSource
import org.junit.Assert._
import org.junit.Test

object WordEmbedDictTest {
  val file = "src/test/resource/wordvec/glove"
}

class WordEmbedDictTest {

  @Test
  def testFind: Unit = {
    val words = new WordEmbedDict(new FileWordSource(WordEmbedDictTest.file))

    assertTrue(words.find("any").isDefined)
  }

  @Test
  def testCompare: Unit = {
    val words = new WordEmbedDict(new FileWordSource(WordEmbedDictTest.file))

    assertTrue(0.5 < words.compare("when", "who"))
  }

  @Test
  def testAddPhrase: Unit = {

    val words = new WordEmbedDict(new FileWordSource(WordEmbedDictTest.file))
    assertTrue(words.find("any how").isEmpty)

    words.addPhrase("any how", Array("any", "how"))

    assertTrue(words.find("any how").isDefined)

    val result = words.find("any how").get
    val good = words.find("any").get
    val man = words.find("how").get

    assertEquals(result, good.add(man))
  }
}