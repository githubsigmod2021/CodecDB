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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.VersionParser
import org.apache.parquet.column.page.{DataPage, PageReadStore}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HiddenFileFilter

import scala.collection.JavaConversions._

object ReadTPCH extends App {
  val folder = args(0)
  val schema = TPCHSchema.customerSchema
  val file = "%s%s.parquet".format(folder,schema.getName)
  val conf = new Configuration
  val path = new Path(file)
  val fs = path.getFileSystem(conf)
  val statuses = fs.listStatus(path, HiddenFileFilter.INSTANCE).toIterator.toList
  val footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false)
  if (footers.isEmpty)
    throw new IllegalArgumentException();

  for (footer <- footers) {
    val version = VersionParser.parse(footer.getParquetMetadata.getFileMetaData.getCreatedBy)
    val fileReader = ParquetFileReader.open(conf, footer.getFile, footer.getParquetMetadata)
    var blockCounter = 0
    val cols = footer.getParquetMetadata.getFileMetaData.getSchema.getColumns

    var rowGroup: PageReadStore = null;
    var dataPage: DataPage = null

    rowGroup = fileReader.readNextRowGroup()
    while (rowGroup != null) {
      println("Row Group:" + blockCounter)
      val blockMeta = footer.getParquetMetadata.getBlocks.get(blockCounter)
      // Read each column
      for (cd <- cols) {
        println("Column:" + ","+cd.getPath.mkString(","))
        val pageReader = rowGroup.getPageReader(cd)
        var pageCounter = 0
        dataPage = pageReader.readPage()
        while (dataPage != null) {
          println("Page %d, count %d".format(pageCounter, dataPage.getValueCount))
          pageCounter += 1
          dataPage = pageReader.readPage()
        }
      }
      blockCounter += 1
      rowGroup = fileReader.readNextRowGroup()
    }
  }
}
