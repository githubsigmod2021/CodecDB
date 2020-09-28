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

package codecdb.query.tpch.scripts

import java.io.File

import codecdb.parquet.{EncContext, EncReaderProcessor, ParquetReaderHelper, ParquetWriterHelper}
import codecdb.query.NonePrimitiveConverter
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper
import org.apache.parquet.VersionParser
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import scala.collection.JavaConversions._

object Sketch extends App {

  val schema = new MessageType("default",
    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "v1"),

    new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "v2")
  )

  write
  read

  def write: Unit = {
    EncContext.encoding.get().put(schema.getColumns()(0).toString, Encoding.PLAIN)
    EncContext.context.get().put(schema.getColumns()(0).toString, Array[AnyRef]("0", "0"));
    EncContext.encoding.get().put(schema.getColumns()(1).toString, Encoding.PLAIN)
    EncContext.context.get().put(schema.getColumns()(1).toString, Array[AnyRef]("0", "0"));
    ParquetWriterHelper.write(new File("/home/harper/temp/test.tmp").toURI, schema,
      new File("/home/harper/temp/test.tmp.PLAIN").toURI, ",", false)
  }

  def read: Unit = {
    ParquetReaderHelper.read(new File("/home/harper/temp/test.tmp.PLAIN").toURI,
      new EncReaderProcessor() {

        override def processRowGroup(version: VersionParser.ParsedVersion,
                                     meta: BlockMetaData, rowGroup: PageReadStore): Unit = {
          val col = schema.getColumns()(1)
          val colreader = new ColumnReaderImpl(col, rowGroup.getPageReader(col), new NonePrimitiveConverter, version)
          for (i <- 0L until colreader.getTotalValueCount) {
            if (colreader.getCurrentDefinitionLevel == 1)
              colreader.getInteger()
            colreader.consume()
          }
        }
      })
  }
}
