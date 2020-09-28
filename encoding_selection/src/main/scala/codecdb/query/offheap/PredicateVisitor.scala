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

package codecdb.query.offheap

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ValuesType.{DEFINITION_LEVEL, REPETITION_LEVEL}
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page.{DataPageV1, DataPageV2}
import org.apache.parquet.io.ParquetDecodingException

class PredicateVisitor(path: ColumnDescriptor, pred: Predicate) extends Visitor[ByteBuffer] {
  override def visit(page: DataPageV1): ByteBuffer = {
    val rlReader = page.getRlEncoding.getValuesReader(path, REPETITION_LEVEL)
    val dlReader = page.getDlEncoding.getValuesReader(path, DEFINITION_LEVEL)
    val pageValueCount = page.getValueCount
    try {
      val bytes = page.getBytes.toByteBuffer
      rlReader.initFromPage(pageValueCount, bytes, 0)
      var next = rlReader.getNextOffset
      dlReader.initFromPage(pageValueCount, bytes, next)
      next = dlReader.getNextOffset
      // Read data from bytes starting at next
      // Generate a byte buffer containing the equality
      return pred.execute(bytes, next, pageValueCount);
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }

  override def visit(page: DataPageV2): ByteBuffer = {
    try {
      // Read data from bytes starting at 0
      val bytes = page.getData.toByteBuffer
      return pred.execute(bytes, 0, page.getValueCount)
    } catch {
      case e: IOException =>
        throw new ParquetDecodingException("could not read page " + page + " in col " + path, e)
    }
  }
}
