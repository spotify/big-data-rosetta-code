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

object TopItems {

  val topK = 100

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .group
      // Sum values with an implicit `Semigroup[Double]`
      .sum
      // Group all elements with a single key `Unit`
      .groupAll
      // Take top K with a priority queue
      .sortedReverseTake(topK)(Ordering.by(_._2))
      // Drop `Unit` key
      .values
      // Flatten result `Seq[(String, Double)]`
      .flatten
  }

  // ## Scalding with Algebird `Aggregator`
  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    val aggregator = sortedReverseTake[(String, Double)](topK)(Ordering.by(_._2))
    input
      .map(x => (x.item, x.score))
      .group
      // Sum values with an implicit `Semigroup[Double]`
      .sum
      .toTypedPipe
      // Aggregate globally into a single `Seq[(String, Double)]`
      .aggregate(aggregator)
      // Flatten result `Seq[(String, Double)]`
      .flatten
  }

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      // Sum values with an implicit `Semigroup[Double]`
      .sumByKey
      // Compute top K as an `Iterable[(String, Double)]`
      .top(topK)(Ordering.by(_._2))
      // Flatten result `Iterable[(String, Double)]`
      .flatten
  }

  // ## Scio with Algebird `Aggregator`
  def scioWithAlgebird(input: SCollection[Rating]): SCollection[(String, Double)] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    val aggregator = sortedReverseTake[(String, Double)](topK)(Ordering.by(_._2))
    input
      .map(x => (x.item, x.score))
      // Sum values with an implicit `Semigroup[Double]`
      .sumByKey
      // Aggregate globally into a single `Seq[(String, Double)]`
      .aggregate(aggregator)
      // Flatten result `Seq[(String, Double)]`
      .flatten
  }

  // ## Spark
  def spark(input: RDD[Rating]): Seq[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      // Sum values with addition
      .reduceByKey(_ + _)
      // `top` is an action and collects data back to the driver node
      .top(topK)(Ordering.by(_._2))
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird(input: RDD[Rating]): Seq[(String, Double)] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    import com.twitter.algebird.spark._
    val aggregator = sortedReverseTake[(String, Double)](topK)(Ordering.by(_._2))
    input
      .map(x => (x.item, x.score))
      // Sum values with addition
      .reduceByKey(_ + _)
      .algebird
      // `aggregate` is an action and collects data back to the driver node
      .aggregate(aggregator)
  }

}
