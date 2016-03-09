/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.combient.sparkjob

import org.apache.spark.sql.SQLContext


// $example off$
import org.apache.spark.{SparkConf, SparkContext}

object SimpleExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleExample")
    val sc = new SparkContext(conf)

    val schema = Seq("id", "cykle", "value")
    val data = Seq(
      (1, 1, 1),
      (1, 2, 11),
      (1, 3, 1),
      (1, 4, 11),
      (1, 5, 1),
      (1, 6, 11),
      (2, 1, 1),
      (2, 2, 11),
      (2, 3, 1),
      (2, 4, 11),
      (2, 5, 1),
      (2, 6, 11)
    )

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dft = sc.parallelize(data).toDF(schema: _*)

    dft.write.parquet("/user/xadmin/pm.scaledfeatures")

    println(s"Count: ${dft.count()}")

  }
}
// scalastyle:on println
