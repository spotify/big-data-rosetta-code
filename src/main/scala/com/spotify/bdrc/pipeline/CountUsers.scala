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

// Example: Count the Number of Items of a Given User
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object CountUsers {

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[Long] = {
    input
      .filter(_.user == "Smith")
      .map(_ => 1L)
      // Sum with an implicit `Semigroup[Long]`
      .sum
      .toTypedPipe
  }

  // ## Sclading with Algebird `Aggregator`
  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[Long] = {
    import com.twitter.algebird.Aggregator.count
    input
    // Aggregate globally into a single `Long`
      .aggregate(count(_.user == "Smith"))
      .toTypedPipe
  }

  def scio(input: SCollection[Rating]): SCollection[Long] = {
    input
      .filter(_.user == "Smith")
      .count
  }

  // ## Scio with Algebird `Aggregator`
  def scioWithAlgebird(input: SCollection[Rating]): SCollection[Long] = {
    import com.twitter.algebird.Aggregator.count
    input
    // Aggregate globally into a single `Long`
      .aggregate(count((_: Rating).user == "Smith"))
  }

  // ## Spark
  def spark(input: RDD[Rating]): Long = {
    input
      .filter(_.user == "Smith")
      // `count` is an action and collects data back to the driver node
      .count()
  }

  // ## Spark with Algebird `Aggregator`
  def sparkWithAlgebird(input: RDD[Rating]): Long = {
    import com.twitter.algebird.Aggregator.count
    import com.twitter.algebird.spark._
    input.algebird
    // `aggregate` is an action and collects data back to the driver node
      .aggregate(count(_.user == "Smith"))
  }

}
