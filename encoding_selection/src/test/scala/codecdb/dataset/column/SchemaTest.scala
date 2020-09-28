package codecdb.dataset.column

import org.junit.Test
import org.junit.Assert._
import java.net.URI
import java.io.File

import codecdb.dataset.schema.Schema
import codecdb.model.DataType

class SchemaTest {

  @Test
  def testFromParquetFile(): Unit = {
    val schema = Schema.fromParquetFile(new File("src/test/resource/schema/test.schema").toURI)
    assertEquals(22, schema.columns.length)
    assertTrue(schema.hasHeader)

    assertEquals(DataType.STRING, schema.columns(3)._1)
    assertEquals("Block", schema.columns(3)._2)
  }

  @Test
  def testFromParquetFile2(): Unit = {
    val schema = Schema.fromParquetFile(new File("src/test/resource/schema/test2.schema").toURI)
    assertEquals(7, schema.columns.length)
    assertTrue(schema.hasHeader)

    assertEquals(DataType.STRING, schema.columns(1)._1)
    assertEquals("Case_number", schema.columns(1)._2)

    assertEquals(DataType.STRING, schema.columns(3)._1)
    assertEquals("Primary_type", schema.columns(3)._2)

    assertEquals(DataType.STRING, schema.columns(5)._1)
    assertEquals("Loc_description", schema.columns(5)._2)

    assertEquals(DataType.STRING, schema.columns(6)._1)
    assertEquals("update_on", schema.columns(6)._2)
  }

  @Test
  def testFromParquetFile3(): Unit = {
    val schema = Schema.fromParquetFile(new File("src/test/resource/schema/test3.schema").toURI)
    assertEquals(22, schema.columns.length)
    assertFalse(schema.hasHeader)

    assertEquals(DataType.STRING, schema.columns(3)._1)
    assertEquals("Block", schema.columns(3)._2)
  }

}