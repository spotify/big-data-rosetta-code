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

package com.spotify.bdrc.pipeline

import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute number of total and distinct items.
 *
 * Input is a collection of (user, item, score).
 */
object TotalAndDistinctCount {

  def aggregator = {
    import com.twitter.algebird._
    // Exact total count, approximate unique count
    val totalCount = Aggregator.size
    val uniqueCount = Aggregator.approximateUniqueCount[String]
    MultiAggregator(totalCount, uniqueCount)
  }

  def scaldingExact(input: TypedPipe[String]): TypedPipe[(Long, Long)] = {
    input
      .map((_, 1L))
      .group.sum  // (key, total count per key)
      .toTypedPipe
      .map(kv => (kv._1, (kv._2, 1L)))
      .group.sum  // (key, (total count, distinct count))
      .values
  }

  def scaldingApproximate(input: TypedPipe[String]): TypedPipe[(Long, Long)] = {
    input.aggregate(aggregator)
  }

  def scioExact(input: SCollection[String]): SCollection[(Long, Long)] = {
    input
      .map((_, 1L))
      .sumByKey  // (key, total count per key)
      .map(kv => (kv._1, (kv._2, 1L)))
      .sumByKey  // (key, (total count, distinct count))
      .values
  }

  def scioApproximate(input: SCollection[String]): SCollection[(Long, Long)] = {
    input.aggregate(aggregator)
  }

  def sparkAlgebird(input: RDD[String]): RDD[(Long, Long)] = {
    import com.twitter.algebird.spark._
    input
      .map((_, 1L))
      .algebird.sumByKey[String, Long]  // (key, total count per key)
      .map(kv => (kv._1, (kv._2, 1L)))
      .algebird.sumByKey[String, (Long, Long)]  // (key, (total count, distinct count))
      .values
  }

  def sparkInMemory(input: RDD[String]): (Long, Long) = {
    input.cache()
    (input.count(), input.distinct().count())
  }

  def sparkApproximate(input: RDD[String]): (Long, Long) = {
    input.cache()
    (input.count(), input.countApproxDistinct())
  }

}
