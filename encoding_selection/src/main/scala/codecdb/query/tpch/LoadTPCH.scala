/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package codecdb.query.tpch

import java.io.File

import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import LoadTPCH.{folder, inputsuffix, outputsuffix}
import codecdb.parquet.{EncContext, EncReaderProcessor, ParquetReaderHelper, ParquetWriterHelper}
import codecdb.query.NonePrimitiveConverter
import org.apache.parquet.VersionParser
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.metadata.BlockMetaData

import scala.collection.JavaConversions._

object LoadTPCH extends App {

  val folder = "/home/harper/TPCH/"
  //  val folder = args(0)
  val inputsuffix = ".tbl"
  val outputsuffix = ".parquet"

  // Load TPCH
  TPCHSchema.schemas.foreach(schema => {
    ParquetWriterHelper.write(
      new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
      schema,
      new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI, "\\|", false)
  })
}

object LoadPart20 extends App {
  val schema = TPCHSchema.partSchema
  var counter = 0
  schema.getColumns.forEach(cd => {
    EncContext.encoding.get().put(cd.toString, Encoding.PLAIN_DICTIONARY)
    EncContext.context.get().put(cd.toString, Array[AnyRef](counter.toString, (counter * 10).toString))
    counter += 1
  })

  ParquetWriterHelper.write(
    new File("src/test/resource/parquet/part_20").toURI,
    schema,
    new File("src/test/resource/parquet/part_20.parquet").toURI, "\\|", false)
}

object LoadTPCH4Offheap extends App {
  EncContext.encoding.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Encoding.BIT_PACKED)
  EncContext.context.get().put(TPCHSchema.lineitemSchema.getColumns()(4).toString, Array[AnyRef]("6", "50"))
  ParquetWriterHelper.write(new File("/home/harper/TPCH/lineitem.tbl").toURI, TPCHSchema.lineitemSchema,
    new File("/home/harper/TPCH/offheap/lineitem.parquet").toURI, "\\|", false)
}

object LoadLineItem extends App {
  val schema = TPCHSchema.orderSchema

  EncContext.encoding.get().put(schema.getColumns()(1).toString, Encoding.PLAIN)

  ParquetWriterHelper.write(
    new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
    schema,
    new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI, "\\|", false)
}


object SelectLineitem extends App {
  val schema = TPCHSchema.lineitemOptSchema
  val start = System.currentTimeMillis()
  ParquetReaderHelper.read(new File("/home/harper/TPCH/opt/lineitem.parquet").toURI, new EncReaderProcessor() {

    override def processRowGroup(version: VersionParser.ParsedVersion,
                                 meta: BlockMetaData, rowGroup: PageReadStore): Unit = {

      val readers = schema.getColumns.map(col => {
        new ColumnReaderImpl(col, rowGroup.getPageReader(col), new NonePrimitiveConverter, version)
      })

      readers.foreach(reader => {
        for (i <- 0L until rowGroup.getRowCount) {
          if (reader.getCurrentDefinitionLevel > 0)
            reader.readValue()
          reader.consume()
        }
      })
    }
  })
  println(System.currentTimeMillis() - start)
}