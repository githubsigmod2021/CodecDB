package codecdb.dataset.schema

import org.junit.Test
import org.junit.Assert._
import java.io.File

import codecdb.model.DataType

class SchemaGuesserTest {

  @Test
  def testTestType(): Unit = {
    val guess = new SchemaGuesser

    assertEquals(DataType.INTEGER, guess.testType("22", DataType.BOOLEAN))
    assertEquals(DataType.INTEGER, guess.testType("22,232", DataType.BOOLEAN))
    assertEquals(DataType.BOOLEAN, guess.testType("1", DataType.BOOLEAN))
    assertEquals(DataType.BOOLEAN, guess.testType("0", DataType.BOOLEAN))
    assertEquals(DataType.BOOLEAN, guess.testType("true", DataType.BOOLEAN))
    assertEquals(DataType.BOOLEAN, guess.testType("False", DataType.BOOLEAN))
    assertEquals(DataType.BOOLEAN, guess.testType("Yes", DataType.BOOLEAN))
    assertEquals(DataType.BOOLEAN, guess.testType("nO", DataType.BOOLEAN))

    assertEquals(DataType.INTEGER, guess.testType("22,322", DataType.INTEGER))
    assertEquals(DataType.LONG, guess.testType("311,131,322,322", DataType.INTEGER))
    assertEquals(DataType.DOUBLE, guess.testType("22.54", DataType.INTEGER))
    assertEquals(DataType.DOUBLE, guess.testType("33,222.54", DataType.INTEGER))
    assertEquals(DataType.STRING, guess.testType("Goodman", DataType.INTEGER))

    assertEquals(DataType.LONG, guess.testType("32", DataType.LONG))
    assertEquals(DataType.LONG, guess.testType("32,942", DataType.LONG))
    assertEquals(DataType.LONG, guess.testType("32,323,232,234,234", DataType.LONG))
    assertEquals(DataType.STRING, guess.testType("32,323,232,234,234,432,234,234,234,234,234,234", DataType.LONG))
    assertEquals(DataType.STRING, guess.testType("Goodman", DataType.LONG))

    assertEquals(DataType.DOUBLE, guess.testType("22.5", DataType.DOUBLE))
    assertEquals(DataType.DOUBLE, guess.testType(".5", DataType.DOUBLE))
    assertEquals(DataType.DOUBLE, guess.testType(".5E2", DataType.DOUBLE))
    assertEquals(DataType.DOUBLE, guess.testType("-3.5", DataType.DOUBLE))
    assertEquals(DataType.DOUBLE, guess.testType("3234", DataType.DOUBLE))
    assertEquals(DataType.DOUBLE, guess.testType("83,323.4", DataType.DOUBLE))
    assertEquals(DataType.DOUBLE, guess.testType("5", DataType.DOUBLE))
    assertEquals(DataType.STRING, guess.testType("Goews", DataType.DOUBLE))

    assertEquals(DataType.STRING, guess.testType("Goews", DataType.STRING))
    assertEquals(DataType.STRING, guess.testType("32", DataType.STRING))
    assertEquals(DataType.STRING, guess.testType("32.323", DataType.STRING))
    assertEquals(DataType.STRING, guess.testType("E5230", DataType.DOUBLE))
  }

  @Test
  def testGuessSchema(): Unit = {
    val guess = new SchemaGuesser()

    val csvSchema = guess.guessSchema(new File("src/test/resource/schema/test_guess_schema.csv").toURI)

    assertEquals(5, csvSchema.columns.length)
    assertEquals("A_K", csvSchema.columns(0)._2)
    assertEquals("B_M", csvSchema.columns(1)._2)
    assertEquals("CWD", csvSchema.columns(2)._2)
    assertEquals("DEE", csvSchema.columns(3)._2)
    assertEquals("E", csvSchema.columns(4)._2)
    assertEquals(DataType.DOUBLE, csvSchema.columns(0)._1)
    assertEquals(DataType.STRING, csvSchema.columns(1)._1)
    assertEquals(DataType.LONG, csvSchema.columns(2)._1)
    assertEquals(DataType.STRING, csvSchema.columns(3)._1)
    assertEquals(DataType.INTEGER, csvSchema.columns(4)._1)

    val xlsxSchema = guess.guessSchema(new File("src/test/resource/schema/test_guess_schema.xlsx").toURI)

    assertEquals(4, xlsxSchema.columns.length)

    assertEquals(DataType.DOUBLE, xlsxSchema.columns(0)._1)
    assertEquals(DataType.STRING, xlsxSchema.columns(1)._1)
    assertEquals(DataType.LONG, xlsxSchema.columns(2)._1)
    assertEquals(DataType.STRING, xlsxSchema.columns(3)._1)

    val jsonSchema = guess.guessSchema(new File("src/test/resource/schema/test_guess_schema.json").toURI)

    assertEquals(4, jsonSchema.columns.length)

    assertEquals(DataType.DOUBLE, jsonSchema.columns(0)._1)
    assertEquals(DataType.STRING, jsonSchema.columns(1)._1)
    assertEquals(DataType.INTEGER, jsonSchema.columns(2)._1)
    assertEquals(DataType.STRING, jsonSchema.columns(3)._1)

    val tsvSchema = guess.guessSchema(new File("src/test/resource/schema/test_guess_schema.tsv").toURI)

    assertEquals(5, tsvSchema.columns.length)

    assertEquals(DataType.DOUBLE, tsvSchema.columns(0)._1)
    assertEquals(DataType.STRING, tsvSchema.columns(1)._1)
    assertEquals(DataType.LONG, tsvSchema.columns(2)._1)
    assertEquals(DataType.INTEGER, tsvSchema.columns(3)._1)
    assertEquals(DataType.STRING, tsvSchema.columns(4)._1)

  }
}