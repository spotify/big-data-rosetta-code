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

// Example: Compute Top K Items Globally
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object TopItemsPerUser {

  val topK = 100

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[Rating] = {
    input
      .groupBy(_.user)
      // Take top K per group with a priority queue
      .sortedReverseTake(topK)(Ordering.by(_.score))
      // Drop user key
      .values
      // Flatten result `Seq[Rating]`
      .flatten
  }

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[Rating] = {
    input
      .keyBy(_.user)
      // Compute top K per key
      .topByKey(topK, Ordering.by(_.score))
      // Drop user key
      .values
      // Flatten result `Iterable[Rating]`
      .flatten
  }

  // ## Spark Naive Approach
  def spark(input: RDD[Rating]): RDD[Rating] = {
    input
      // `groupBy` shuffles all data, inefficient
      .groupBy(_.user)
      // Drop user key
      .values
      // Convert grouped values to a `List[Rating]` and sort on a single node, inefficient
      .flatMap(_.toList.sortBy(-_.score).take(topK))
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird(input: RDD[Rating]): RDD[Rating] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    import com.twitter.algebird.spark._
    val aggregator = sortedReverseTake[Rating](topK)(Ordering.by(_.score))
    input
      .keyBy(_.user)
      .algebird
      // Aggregate per key into a `Seq[Rating]`
      .aggregateByKey(aggregator)
      // Flatten result `Seq[Rating]`
      .flatMap(_._2)
  }

  // ## Spark with MLLib
  def sparkWithMllib(input: RDD[Rating]): RDD[Rating] = {
    import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
    input
      .keyBy(_.user)
      // From `spark-mllib`, compute top K per key with a priority queue
      .topByKey(topK)(Ordering.by(_.score))
      // Flatten result `Seq[Rating]`
      .flatMap(_._2)
  }

}
