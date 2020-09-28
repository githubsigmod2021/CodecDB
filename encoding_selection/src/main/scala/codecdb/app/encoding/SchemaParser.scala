package codecdb.app.encoding

import codecdb.dataset.schema.Schema
import codecdb.model.DataType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import scala.collection.JavaConversions._

object SchemaParser {

  def toParquetSchema(schema: Schema): MessageType = {
    new MessageType("default", schema.columns.map(f => {
      new PrimitiveType(Repetition.REQUIRED, f._1 match {
        case DataType.INTEGER => PrimitiveTypeName.INT32
        case DataType.LONG => PrimitiveTypeName.INT64
        case DataType.FLOAT => PrimitiveTypeName.FLOAT
        case DataType.DOUBLE => PrimitiveTypeName.DOUBLE
        case DataType.BOOLEAN => PrimitiveTypeName.BOOLEAN
        case DataType.STRING => PrimitiveTypeName.BINARY
      }, f._2)
    }).toList)
  }
}
