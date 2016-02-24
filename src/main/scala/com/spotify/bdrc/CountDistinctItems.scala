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

import com.google.common.base.Charsets
import com.spotify.bdrc.util.Records.UserItemData
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute number of distinct items.
 *
 * Input is a collection of (user, item, score).
 */
object CountDistinctItems {

  /** Exact approach */
  def scalding(input: TypedPipe[UserItemData]): TypedPipe[Long] = {
    input
      .map(_.item)
      .distinct
      .map(_ => 1L)
      .sum  // implicit Semigroup[Long] from Algebird
      .toTypedPipe
  }

  /** Approximate approach */
  def scaldingApproxWithAlgebird(input: TypedPipe[UserItemData]): TypedPipe[Double] = {
    import com.twitter.algebird.HyperLogLogAggregator
    val aggregator = HyperLogLogAggregator.sizeAggregator(bits = 12)
    input
      .map(_.item.getBytes(Charsets.UTF_8))  // HyperLogLog expects bytes input
      .aggregate(aggregator)
      .toTypedPipe
  }

  /** Exact approach */
  def scio(input: SCollection[UserItemData]): SCollection[Long] = {
    input
      .map(_.item)
      .distinct()
      .count()
  }

  /** Approximate approach */
  def scioApprox(input: SCollection[UserItemData]): SCollection[Long] = {
    input
      .map(_.item)
      .countApproxDistinct()
  }

  /** Exact approach */
  def spark(input: RDD[UserItemData]): Long = {
    input
      .map(_.item)
      .distinct()
      .count()
  }

  /** Approximate approach */
  def sparkApprox(input: RDD[UserItemData]): Long = {
    input
      .map(_.item)
      .countApproxDistinct()
  }

}
