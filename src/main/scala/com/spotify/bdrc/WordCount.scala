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

package com.spotify.bdrc

import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Classic word count.
 *
 * Input is a collection of lines from a text.
 */
object WordCount {

  def scalding(input: TypedPipe[String]): TypedPipe[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .groupBy(identity)  // word -> [word, word, ...]
      .size
      .toTypedPipe
  }

  def scio(input: SCollection[String]): SCollection[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
  }

  /** Result is distributed. */
  def sparkTransformation(input: RDD[String]): RDD[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .map((_, 1L))
      .reduceByKey(_ + _)
  }

  /** Result is collected to driver node. */
  def sparkAction(input: RDD[String]): Seq[(String, Long)] = {
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .toSeq
  }

}
