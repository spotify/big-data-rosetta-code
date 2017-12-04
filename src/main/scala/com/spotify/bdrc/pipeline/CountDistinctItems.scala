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

// Example: Count Number of Distinct Items
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.google.common.base.Charsets
import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object CountDistinctItems {

  // ## Scalding Exact Approach
  def scalding(input: TypedPipe[Rating]): TypedPipe[Long] = {
    input
      .map(_.item)
      // Remove duplicates, requires a shuffle
      .distinct
      .map(_ => 1L)
      // Sum with an implicit `Semigroup[Long]`
      .sum
      .toTypedPipe
  }

  // ## Scalding Approximate Approach
  def scaldingApproxWithAlgebird(input: TypedPipe[Rating]): TypedPipe[Double] = {
    import com.twitter.algebird.HyperLogLogAggregator
    val aggregator = HyperLogLogAggregator.sizeAggregator(bits = 12)
    input
      // `HyperLogLog` expects bytes input
      .map(_.item.getBytes(Charsets.UTF_8))
      // Aggregate globally into a `Double`
      .aggregate(aggregator)
      .toTypedPipe
  }

  // ## Scio Exact Approach
  def scio(input: SCollection[Rating]): SCollection[Long] = {
    input
      .map(_.item)
      .distinct
      .count
  }

  // ## Scio Approximate Approach
  def scioApprox(input: SCollection[Rating]): SCollection[Long] = {
    input
      .map(_.item)
      .countApproxDistinct()
  }

  // ## Spark Exact Approach
  def spark(input: RDD[Rating]): Long = {
    input
      .map(_.item)
      .distinct()
      .count()
  }

  // ## Spark Approximate Approach
  def sparkApprox(input: RDD[Rating]): Long = {
    input
      .map(_.item)
      .countApproxDistinct()
  }

}
