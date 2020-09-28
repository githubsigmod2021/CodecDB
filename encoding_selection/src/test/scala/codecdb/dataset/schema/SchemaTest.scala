package codecdb.dataset.schema

import java.io.File

import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

import scala.io.Source

class SchemaTest {
  @Test
  def testGetSchema(): Unit = {
    var schema = Schema.getSchema(new File("src/test/resource/schema/find_schema.csv").toURI)
    assertEquals(3, schema.columns.length)

    schema = Schema.getSchema(new File("src/test/resource/schema/find_schema2.tsv").toURI)
    assertEquals(6, schema.columns.length)

    schema = Schema.getSchema(new File("src/test/resource/schema/fuzzy_find_schema_3.csv").toURI)
    assertEquals(5, schema.columns.length)
  }

  @Test
  def testToSchema: Unit = {
    val schema = new Schema()
    schema.columns = Array((DataType.INTEGER, "Wa"), (DataType.DOUBLE, "Ma"), (DataType.STRING, "Ka"))
    schema.hasHeader = true

    Schema.toParquetFile(schema, new File("src/test/resource/schema/test_to_schema.schema.gen").toURI)

    val origin = Source.fromFile(new File("src/test/resource/schema/test_to_schema.schema")).getLines().toArray
    val gened = Source.fromFile(new File("src/test/resource/schema/test_to_schema.schema.gen")).getLines().toArray

    assertArrayEquals(gened.toArray[Object], origin.toArray[Object])
  }
}