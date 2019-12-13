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

// Example: Compute Average Score per Item
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Semigroup
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object AverageScorePerItem {

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    input
      .groupBy(_.user)
      // Map into (sum, count)
      .mapValues(x => (x.score, 1L))
      // Sum both per key with an implicit `Semigroup[(Double, Long)]`
      .sum
      // Map (sum, count) into average
      .mapValues(p => p._1 / p._2)
      .toTypedPipe
  }

  // ## Scalding with Algebird `Aggregator`
  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    input
      .groupBy(_.user)
      // Map values into `Double`
      .mapValues(_.score)
      // Aggregate average per key
      .aggregate(AveragedValue.aggregator)
      .toTypedPipe
  }

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[(String, Double)] = {
    input
      .keyBy(_.user)
      // Map into (sum, count)
      .mapValues(x => (x.score, 1L))
      // Sum both per key with an implicit `Semigroup[(Double, Long)]`
      .sumByKey
      // Map (sum, count) into average
      .mapValues(p => p._1 / p._2)
  }

  // ## Spark
  // Summon an Algebird `Semigroup[(Double, Long)]` with implicit argument
  def spark(input: RDD[Rating])(implicit sg: Semigroup[(Double, Long)]): RDD[(String, Double)] = {
    input
      .keyBy(_.user)
      // Map into (sum, count)
      .mapValues(x => (x.score, 1L))
      // Reduce both per key with `plus = (T, T) => T` where `T` is `(Double, Long)`
      .reduceByKey(sg.plus) // plus: (T, T) => T where T is (Double, Long)
      // Map (sum, count) into average
      .mapValues(p => p._1 / p._2)
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird(input: RDD[Rating]): RDD[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    import com.twitter.algebird.spark._
    input
      .keyBy(_.user)
      .mapValues(_.score)
      // Map values into `Double`
      .algebird
      // Aggregate average per key
      .aggregateByKey(AveragedValue.aggregator)
  }

}
