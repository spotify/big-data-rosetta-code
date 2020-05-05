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

// Example: Compute the Sum of Scores per Item
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object SumPerItem {

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    input
      .groupBy(_.item)
      .mapValues(_.score)
      // Sum per key with an implicit `Semigroup[Double]`
      .sum
      .toTypedPipe
  }

  // ## Scalding with Algebird `Aggregator`
  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.Aggregator.prepareMonoid
    input
      .groupBy(_.item)
      // Aggregate per key with an aggregator that converts `UserItemData` to `Double` via
      // `_.score` before reduce
      .aggregate(prepareMonoid(_.score))
      .toTypedPipe
  }

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .sumByKey
  }

  // ## Spark
  def spark(input: RDD[Rating]): RDD[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .reduceByKey(_ + _)
  }

  // ## Spark with Algebird `Semigroup`
  def sparkWithAlgebird1(input: RDD[Rating]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    input
      .map(x => (x.item, x.score))
      .algebird
      // Sum per key with an implicit `Semigroup[Double]`
      .sumByKey
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird2(input: RDD[Rating]): RDD[(String, Double)] = {
    import com.twitter.algebird.Aggregator.prepareMonoid
    import com.twitter.algebird.spark._
    input
      .keyBy(_.item)
      .algebird
      // Aggregate per key with an aggregator that converts `UserItemData` to `Double` via
      // `_.score` before reduce. Explicit type due to type inference limitation.
      .aggregateByKey(prepareMonoid { x: Rating => x.score })
  }

}
