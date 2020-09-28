package codecdb.ptnmining.validation

import codecdb.ptnmining.parser.Tokenizer
import org.junit.Test

/**
  * Created by harper on 3/31/17.
  */
class PatternValidatorTest {

  @Test
  def testValidate: Unit = {

    val lines = Array("00E5867YL1DB226913E",
      "00E5867YL1DB226913E",
      "00E5867YL1DB226913E",
      "00E5867YL1DB226913E",
      "00E5870YL1DB22923A9",
      "00E5867YL1DB226913E",
      "00E5867YL1DB226913E")

    val tokens = lines.map(Tokenizer.tokenize)


  }
}
