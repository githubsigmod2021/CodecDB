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

import codecdb.parquet.{EncReaderProcessor, ParquetReaderHelper}
import codecdb.query.NonePrimitiveConverter
import codecdb.query.tpch.TPCHSchema
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor
import org.apache.parquet.VersionParser
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.Footer
import org.apache.parquet.hadoop.metadata.BlockMetaData

import scala.collection.JavaConversions._

object MetaPrint extends App {

  var rgCounter = 0

  ParquetReaderHelper.read(new File("/home/harper/TPCH/lineitem.parquet").toURI,
    new EncReaderProcessor() {
      override def processFooter(footer: Footer): Unit = {
        super.processFooter(footer)
        println(footer.getParquetMetadata.getBlocks.map(_.getRowCount).sum)
      }

      override def processRowGroup(version: VersionParser.ParsedVersion, meta: BlockMetaData, rowGroup: PageReadStore): Unit = {

        val cd = TPCHSchema.lineitemSchema.getColumns()(5)
        val reader = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd), new NonePrimitiveConverter(), version);
        print(rowGroup.getRowCount == meta.getRowCount)
        for (i <- 0L until rowGroup.getRowCount) {
          reader.readValue()
          reader.consume()
        }
        println(rgCounter)
        rgCounter += 1
      }
    })
}
