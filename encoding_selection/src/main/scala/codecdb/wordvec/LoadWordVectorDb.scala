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

package codecdb.wordvec

import java.nio.charset.StandardCharsets
import java.sql._
import java.util.Properties
import java.{sql, util}
import java.util.concurrent.Executor
import javax.persistence.Persistence

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}

object LoadWordVectorDb extends App {

  val src = args.length match {
    case 0 => "/home/harper/Downloads/glove.42B.300d.txt"
    case _ => args(0)
  }

  // Check dup
  //  val limit = 12795
  var counter = 0
  Class.forName("com.mysql.jdbc.Driver")
  val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wordvec?characterEncoding=UTF-8", "wordvec", "wordvec")
  connection.setAutoCommit(false)
  val stmt = connection.prepareStatement("insert into word_vec (word, vector) values (?, ?)")

  Source.fromFile(src)(new Codec(StandardCharsets.UTF_8)).getLines().foreach(line => {
    try {
      val res = line.split("\\s+", 2)
      //      if (counter > limit) {
      if (res(0).length < 50) {
        stmt.setString(1, res(0))
        stmt.setString(2, res(1))
        stmt.addBatch()
        counter += 1
      }
      if (counter % 1000 == 0) {
        stmt.executeBatch()
        connection.commit()
      }
      //      }
    } catch {
      case e: Exception => {
        println(counter)
        println(line)
        throw e
      }
    }
  })
  stmt.close()
  connection.close()
}
