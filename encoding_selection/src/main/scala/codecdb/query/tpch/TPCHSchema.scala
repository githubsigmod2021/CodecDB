package codecdb.query.tpch

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

object TPCHSchema {

  val customerSchema = new MessageType("customer",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cust_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "address"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "nation_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "phone"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "acct_bal"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "mkt_segment"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val nationSchema = new MessageType("nation",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "nation_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "region_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val regionSchema = new MessageType("region",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "region_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val supplierSchema = new MessageType("supplier",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "address"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "nation_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "phone"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "acct_bal"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val partSchema = new MessageType("part",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "name"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "mfgr"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "brand"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "type"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "size"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "container"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "retail_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val partsuppSchema = new MessageType("partsupp",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "avail_qty"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "supply_cost"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val lineitemSchema = new MessageType("lineitem",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "order_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "line_number"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "quantity"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "extended_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "discount"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "tax"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "return_flag"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "line_status"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "commit_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "receipt_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_instruct"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_mode"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val lineitemOptSchema = new MessageType("lineitem",
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "order_key"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "part_key"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "supp_key"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "line_number"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "quantity"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "extended_price"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "discount"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "tax"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "return_flag"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "line_status"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "ship_date"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "commit_date"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "receipt_date"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "ship_instruct"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "ship_mode"),
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "comment")
  )

  val orderSchema = new MessageType("orders",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "order_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, "cust_key"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "order_status"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, "total_price"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "order_date"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "order_priority"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "clerk"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "ship_priority"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "comment")
  )

  val schemas = Array(customerSchema, nationSchema,
    regionSchema, supplierSchema, partSchema,
    partsuppSchema, lineitemSchema, orderSchema)
}
