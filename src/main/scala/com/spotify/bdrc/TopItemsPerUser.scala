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
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute top K items for each user.
 *
 * Input is a collection of (user, item, score).
 */
object TopItemsPerUser {

  val topK = 100

  def scalding(input: TypedPipe[UserItemData]): TypedPipe[UserItemData] = {
    input
      .groupBy(_.user)
      .sortedReverseTake(topK)(Ordering.by(_.score))  // priority queue
      .values
      .flatten
  }

  def spark(input: RDD[UserItemData]): RDD[UserItemData] = {
    input
      .groupBy(_.user)
      .values
      // sort items for each user on a single node, inefficient
      .flatMap(_.toList.sortBy(-_.score).take(topK))
  }

  def sparkWithAlgebird(input: RDD[UserItemData]): RDD[UserItemData] = {
    import com.twitter.algebird.Aggregator.sortedReverseTake
    import com.twitter.algebird.spark._
    val aggregator = sortedReverseTake[UserItemData](topK)(Ordering.by(_.score))  // priority queue
    input
      .keyBy(_.user)
      .algebird
      .aggregateByKey(aggregator)
      .flatMap(_._2)
  }

  def sparkWithMllib(input: RDD[UserItemData]): RDD[UserItemData] = {
    import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
    input
      .keyBy(_.user)
      .topByKey(topK)(Ordering.by(_.score))  // from spark-mllib, priority queue
      .flatMap(_._2)
  }

}
