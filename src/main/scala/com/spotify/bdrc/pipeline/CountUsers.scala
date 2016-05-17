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

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute number of records of a given user.
 *
 * Input is a collection of (user, item, score).
 */
object CountUsers {

  def scalding(input: TypedPipe[Rating]): TypedPipe[Long] = {
    input
      .filter(_.user == "Smith")
      .map(_ => 1L)
      .sum  // implicit Semigroup[Long] from Algebird
      .toTypedPipe
  }

  def scaldingWithAlgebird(input: TypedPipe[Rating]): TypedPipe[Long] = {
    import com.twitter.algebird.Aggregator.count
    input
      .aggregate(count(_.user == "Smith"))
      .toTypedPipe
  }

  def scio(input: SCollection[Rating]): SCollection[Long] = {
    input
      .filter(_.user == "Smith")
      .count
  }

  def scioWithAlgebird(input: SCollection[Rating]): SCollection[Long] = {
    import com.twitter.algebird.Aggregator.count
    input
      .filter(_.user == "Smith")
      .aggregate(count(_.user == "Smith"))
  }

  def spark(input: RDD[Rating]): Long = {
    input
      .filter(_.user == "Smith")
      .count()
  }

  def sparkWithAlgebird(input: RDD[Rating]): Long = {
    import com.twitter.algebird.Aggregator.count
    import com.twitter.algebird.spark._
    input
      .algebird
      .aggregate(count(_.user == "Smith"))
  }

}
