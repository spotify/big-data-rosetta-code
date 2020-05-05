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

// Example: Compute One Item with Max Score per User
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object MaxItemPerUser {

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[Rating] = {
    input
      .groupBy(_.user)
      // Reduce items per key by picking the side with higher score for each pair of input
      .reduce((x, y) => if (x.score > y.score) x else y)
      .values
  }

  // ## Scalding with Algebird `Aggregator`
  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[Rating] = {
    import com.twitter.algebird.Aggregator.maxBy
    input
      .groupBy(_.user)
      // Aggregate per key into a single `Rating` based on `Double` value via `_.score`
      .aggregate(maxBy(_.score))
      .values
  }

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[Rating] = {
    input
      .keyBy(_.user)
      // Compute top one item per key as an `Iterable[Rating]`
      .topByKey(1, Ordering.by(_.score))
      // Drop user key
      .values
      // Flatten result `Iterable[Rating]`
      .flatten
  }

  // ## Scio with Algebird `Aggregator`
  def scioWithAlgebird(input: SCollection[Rating]): SCollection[Rating] = {
    import com.twitter.algebird.Aggregator.maxBy
    input
      .keyBy(_.user)
      // Aggregate per key into a single `Rating` based on `Double` value via `_.score`. Explicit
      // type due to type inference limitation.
      .aggregateByKey(maxBy { x: Rating => x.score })
      .values
  }

  // ## Spark
  def spark(input: RDD[Rating]): RDD[Rating] = {
    input
      .keyBy(_.user)
      // Reduce items per key by picking the side with higher score for each pair of input
      .reduceByKey((x, y) => if (x.score > y.score) x else y)
      .values
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird(input: RDD[Rating]): RDD[Rating] = {
    import com.twitter.algebird.Aggregator.maxBy
    import com.twitter.algebird.spark._
    input
      .keyBy(_.user)
      .algebird
      // Aggregate per key into a single `Rating` based on `Double` value via `_.score`. Explicit
      // type due to type inference limitation.
      .aggregateByKey(maxBy { x: Rating => x.score })
      .values
  }

  // ## Spark with MLLib
  def sparkWithMllib(input: RDD[Rating]): RDD[Rating] = {
    import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
    input
      .keyBy(_.user)
      // From `spark-mllib`, compute top K per key with a priority queue
      .topByKey(1)(Ordering.by(_.score))
      .flatMap(_._2)
  }

}
