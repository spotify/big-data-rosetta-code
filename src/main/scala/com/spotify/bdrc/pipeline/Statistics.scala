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

// Example: Compute Basic Descriptive Statistics
// Input is a collection of (user, item, score)
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object Statistics {

  case class Stats(max: Double, min: Double, sum: Double, count: Long, mean: Double, stddev: Double)

  // ## Algebird `Aggregator`
  def aggregator = {
    import com.twitter.algebird._

    // Create 4 `Aggregator`s with different logic

    // The first 3 are of type `Aggregator[Rating, _, Double]` which means it takes `Rating` as
    // input and generates `Double` as output. The last one is of type
    // `Aggregator[Rating, _, Moments]`, where `Moments` include count, mean, standard deviation,
    // etc. The input `Rating` is prepared with a `Rating => Double` function `_.score`.
    val maxOp = Aggregator.max[Double].composePrepare[Rating](_.score)
    val minOp = Aggregator.min[Double].composePrepare[Rating](_.score)
    val sumOp = Aggregator.prepareMonoid[Rating, Double](_.score)
    val momentsOp = Moments.aggregator.composePrepare[Rating](_.score)

    // Apply 4 `Aggregator`s on the same input, present result tuple 4 of
    // `(Double, Double, Double, Moments)` as `Stats`
    MultiAggregator(maxOp, minOp, sumOp, momentsOp)
      .andThenPresent {
        case (max, min, sum, moments) =>
          Stats(max, min, sum, moments.count, moments.mean, moments.stddev)
      }
  }

  // ## Scalding
  def scalding(input: TypedPipe[Rating]): TypedPipe[Stats] =
    input.aggregate(aggregator)

  // ## Scio
  def scio(input: SCollection[Rating]): SCollection[Stats] = {
    input
      .map(_.score)
      .stats
      .map(s => Stats(s.max, s.min, s.sum, s.count, s.mean, s.stdev))
  }

  // ## Scio with Algebird `Aggregator`
  def scioAlgebird(input: SCollection[Rating]): SCollection[Stats] =
    input.aggregate(aggregator)

  // ## Spark
  def spark(input: RDD[Rating]): Stats = {
    val s = input.map(_.score).stats()
    Stats(s.max, s.min, s.sum, s.count, s.mean, s.stdev)
  }

  // ## Spark with Algebird `Aggregator`
  def sparkAlgebird(input: RDD[Rating]): Stats = {
    import com.twitter.algebird.spark._
    input.algebird.aggregate(aggregator)
  }

}
