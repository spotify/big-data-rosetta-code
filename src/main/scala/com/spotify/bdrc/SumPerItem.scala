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

import com.spotify.bdrc.util.Records.UserItemData
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute the sum of scores per item.
 *
 * Input is a collection of (user, item, score).
 */
object SumPerItem {

  def scalding(input: TypedPipe[UserItemData]): TypedPipe[(String, Double)] = {
    input
      .groupBy(_.item)
      .mapValues(_.score)
      .sum  // implicit Semigroup[Double] from Algebird
      .toTypedPipe
  }

  def scaldingWithAlgebird(input: TypedPipe[UserItemData]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.Aggregator.prepareMonoid
    input
      .groupBy(_.item)
      // an Algebird Aggregator that converts UserItemData to Double (via _.score) before reduce
      .aggregate(prepareMonoid(_.score))
      .toTypedPipe
  }

  def scio(input: SCollection[UserItemData]): SCollection[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .sumByKey
  }

  def spark(input: RDD[UserItemData]): RDD[(String, Double)] = {
    input
      .map(x => (x.item, x.score))
      .reduceByKey(_ + _)
  }

  /** First algebird-spark approach using sumByKey on doubles. */
  def sparkWithAlgebird1(input: RDD[UserItemData]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    input
      .map(x => (x.item, x.score))
      .algebird
      .sumByKey  // implicit Semigroup[Double] from Algebird
  }

  /** Second algebird-spark approach using aggregateByKey with prepare. */
  def sparkWithAlgebird2(input: RDD[UserItemData]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    import com.twitter.algebird.Aggregator.prepareMonoid
    input
      .keyBy(_.item)
      .algebird
      // an Algebird Aggregator that converts UserItemData to Double (via _.score) before reduce
      // explicit type due to type inference limitation
      .aggregateByKey(prepareMonoid { x: UserItemData => x.score })
  }

}
