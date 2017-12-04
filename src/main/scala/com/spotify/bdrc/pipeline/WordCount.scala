/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Example: Classic Word Count
package com.spotify.bdrc.pipeline

import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object WordCount {

  // ## Scalding
  def scalding(input: TypedPipe[String]): TypedPipe[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // `groupBy` is lazy
      .groupBy(identity)
      // Operations like `size` after `groupBy` can be lifted into the map phase
      .size
      .toTypedPipe
  }

  // ## Scio
  def scio(input: SCollection[String]): SCollection[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
  }

  // ## Spark Transformation
  def sparkTransformation(input: RDD[String]): RDD[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // There is no `countByValue` transformation in Spark although it is equivalent to mapping
      // into initial count of `1` and reduce with addition
      .map((_, 1L))
      // `reduceByKey` can lift function into the map phase
      .reduceByKey(_ + _)
  }

  // ## Spark Action
  def sparkAction(input: RDD[String]): Seq[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      // `countByValue` is an action and collects data back to the driver node
      .countByValue()
      .toSeq
  }

}
