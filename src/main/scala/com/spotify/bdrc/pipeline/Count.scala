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

// Example: Count Number of Items
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object Count {

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[Long] = {
    input
      .map(_ => 1L)
      // Sum with an implicit `Semigroup[Long]`
      .sum
      .toTypedPipe
  }

  // ## Scalding with Algebird `Aggregator`
  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[Long] = {
    import com.twitter.algebird.Aggregator.size
    input
      .aggregate(size)
      .toTypedPipe
  }

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[Long] =
    input.count

  // ## Scio with Algebird `Aggregator`
  def scioWithAlgebird(input: SCollection[Rating]): SCollection[Long] = {
    import com.twitter.algebird.Aggregator.size
    input
      .aggregate(size)
  }

  // # Spark
  def spark(input: RDD[Rating]): Long = {
    input
    // `count` is an action and collects data back to the driver node
    .count
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird(input: RDD[Rating]): Long = {
    import com.twitter.algebird.Aggregator.size
    import com.twitter.algebird.spark._
    input.algebird
    // `aggregate` is an action and collects data back to the driver node
      .aggregate(size)
  }

}
