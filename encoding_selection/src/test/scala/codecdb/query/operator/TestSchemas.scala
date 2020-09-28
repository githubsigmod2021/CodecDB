package codecdb.query.operator

import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

object TestSchemas {
  val personSchema = new MessageType("person",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "id"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "year"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "gender")
  )

  val contactSchema = new MessageType("contact",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "id"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "person_id"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "address")
  )
}
