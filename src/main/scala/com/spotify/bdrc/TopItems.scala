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

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute top K items globally.
 *
 * Input is a collection of (user, item, score).
 */
object TopItems {

  val topK = 100

  def scalding(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .groupAll  // force all records to a single reducer
      .sortedReverseTake(topK)(Ordering.by(_._2))  // priority queue
      .values
      .flatten
  }

  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    val aggregator = sortedReverseTake[(String, Double)](topK)(Ordering.by(_._2))
    input
      .map(x => (x.item, x.score))
      .group
      .sum  // implicit Semigroup[Double] from Algebird
      .toTypedPipe
      .aggregate(aggregator)
      .flatten
  }

  def scio(input: SCollection[Rating]): SCollection[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .sumByKey
      .top(topK)(Ordering.by(_._2))
      .flatMap(identity)
  }

  def scioWithAlgebird(input: SCollection[Rating]): SCollection[(String, Double)] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    val aggregator = sortedReverseTake[(String, Double)](topK)(Ordering.by(_._2))
    input
      .map(x => (x.item, x.score))
      .sumByKey
      .aggregate(aggregator)
      .flatMap(identity)
  }

  def spark(input: RDD[Rating]): Seq[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .reduceByKey(_ + _)
      .top(topK)(Ordering.by(_._2))
  }

  def sparkWithAlgebird(input: RDD[Rating]): Seq[(String, Double)] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    import com.twitter.algebird.spark._
    val aggregator = sortedReverseTake[(String, Double)](topK)(Ordering.by(_._2))
    input
      .map(x => (x.item, x.score))
      .reduceByKey(_ + _)
      .algebird
      .aggregate(aggregator)
  }

}
