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

package codecdb.query.util

import java.util.ArrayList

import org.apache.parquet.schema.{MessageType, Type}

import scala.collection.JavaConversions._

object SchemaUtils {

  def project(origin:MessageType, project:Array[Int]):MessageType = {
   new MessageType("%s_P".format(origin.getName),project.zipWithIndex.map(p=>{
     origin.getType(p._1).withId(p._2)
   }).toList)
  }

  def join(left: MessageType, right: MessageType,
           leftProject: Array[Int], rightProject: Array[Int]): MessageType = {
    val types = new ArrayList[Type](leftProject.length + rightProject.length + 1)

    leftProject.foreach(i => {
      types.add(left.getType(i).withId(types.size()))
    })
    rightProject.foreach(i => {
      types.add(right.getType(i).withId(types.size()))
    })

    new MessageType("%s_J_%s".format(left.getName, right.getName), types)
  }
}
