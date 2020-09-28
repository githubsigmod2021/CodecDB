package codecdb.ptnmining.eval

import codecdb.ptnmining.parser.{TInt, TSymbol, TWord}
import codecdb.ptnmining.{PEmpty, PIntAny, PIntRange, PSeq, PToken, PUnion, PWordAny}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.TSymbol
import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 4/6/17.
  */
class PatternEvaluatorTest {

  @Test
  def testEvaluate: Unit = {
    // Pattern Size is 63
    val pattern1 = new PSeq(Seq(
      new PToken(new TWord("good")),
      new PUnion(Seq(
        new PToken(new TInt("1")),
        new PToken(new TInt("2")),
        new PToken(new TInt("3"))
      )),
      new PToken(new TSymbol("-")),
      new PUnion(Seq(
        new PToken(new TWord("ASM")),
        new PToken(new TWord("MTM")),
        new PToken(new TWord("DDTE")),
        new PSeq(Seq(
          new PToken(new TWord("CHO")),
          new PToken(new TSymbol("-")),
          new PUnion(Seq(
            new PToken(new TWord("A")),
            new PToken(new TWord("B")),
            new PToken(new TWord("C")),
            new PToken(new TWord("D"))
          ))
        )))
      ),
      new PIntRange(100, 150)
    ))

    val dataset = Seq("good1-ASM127", "good2-MTM101", "good2-DDTE122",
      "good3-CHO-A134", "good3-CHO-B120", "good3-ASM149", "ttmdpt-dawee-323")

    val result = PatternEvaluator.evaluate(pattern1, dataset)

    assertEquals(85, result, 0.01)

    val pattern2 = PSeq.collect(new PWordAny, new PIntAny, new PToken(new TSymbol("-")),
      new PWordAny,
      PUnion.collect(PEmpty, PSeq.collect(new PWordAny, new PToken(new TSymbol("-")), new PWordAny)),
      new PIntAny)

    val result2 = PatternEvaluator.evaluate(pattern2, dataset)

    assertEquals(147, result2, 0.01)
  }
}
