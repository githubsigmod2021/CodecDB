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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package codecdb.ptnmining.compose

import org.apache.parquet.column.ColumnReader
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._

trait ChildColumnLoader {

  def next: Seq[String]

  def skip: Unit
}

class ParquetChildColumnReader(val children: Seq[ColumnReader]) {

  val reader = children.map(child => child.getDescriptor.getType match {
    case INT32 => {
      () => {
        child.getInteger.toString
      }
    }
    case INT64 => {
      () => {
        child.getLong.toString
      }
    }
    case BINARY => {
      () => {
        child.getBinary.toStringUsingUTF8
      }
    }
    case DOUBLE => {
      () => {
        child.getDouble.toString
      }
    }
    case BOOLEAN => {
      () => {
        child.getBoolean.toString
      }
    }
    case FLOAT => {
      () => {
        child.getFloat.toString
      }
    }
    case _ => {
      () => {
        ""
      }
    }
  })

  def next: Seq[String] = reader.map(_.apply())

  def skip: Unit = children.foreach(_.skip)

}