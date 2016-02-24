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
import com.twitter.algebird.Semigroup
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute average score for each item.
 *
 * Input is a collection of (user, item, score).
 */
object AverageScorePerItem {

  def scalding(input: TypedPipe[UserItemData]): TypedPipe[(String, Double)] = {
    input
      .groupBy(_.user)
      .mapValues(x => (x.score, 1L))
      .sum  // implicit Semigroup[(Double, Long)] from Algebird
      .mapValues(p => p._1 / p._2)
      .toTypedPipe
  }

  def scaldingWithAlgebird(input: TypedPipe[UserItemData]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    input
      .groupBy(_.user)
      .mapValues(_.score)
      .aggregate(AveragedValue.aggregator)
      .toTypedPipe
  }

  def scio(input: SCollection[UserItemData]): SCollection[(String, Double)] = {
    input
      .keyBy(_.user)
      .mapValues(x => (x.score, 1L))
      .sumByKey
      .mapValues(p => p._1 / p._2)
  }

  // summon an Algebird Semigroup with implicit
  def spark(input: RDD[UserItemData])(implicit sg: Semigroup[(Double, Long)]): RDD[(String, Double)] = {
    input
      .keyBy(_.user)
      .mapValues(x => (x.score, 1L))
      .reduceByKey(sg.plus)  // plus: (T, T) => T where T is (Double, Long)
      .mapValues(p => p._1 / p._2)
  }

  def sparkWithAlgebird(input: RDD[UserItemData]): RDD[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    import com.twitter.algebird.spark._
    input
      .keyBy(_.user)
      .mapValues(_.score)
      .algebird
      .aggregateByKey(AveragedValue.aggregator)
  }

}
