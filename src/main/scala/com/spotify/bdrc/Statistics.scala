/*
 * Copyright (c) 2016 Spotify AB.
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

import com.spotify.bdrc.util.Records.Rating
import com.spotify.scio.values.SCollection
import com.twitter.algebird.MultiAggregator
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute basic statistics.
 *
 * Input is a collection of (user, item, score).
 */
object Statistics {

  case class Stats(max: Double, min: Double, sum: Double, count: Long, mean: Double, stdev: Double)

  // Algebird Aggregator
  def aggregator = {
    import com.twitter.algebird.Aggregator._
    import com.twitter.algebird.Moments
    import com.twitter.algebird.Aggregator.prepareMonoid

    // 4 aggregators with different logic
    val maxOp = maxBy[Rating, Double](_.score)
    val minOp = minBy[Rating, Double](_.score)
    val sum = prepareMonoid[Rating, Double](_.score)
    val moments = Moments.aggregator.composePrepare[Rating](_.score)

    // Apply 4 aggregators on the same input, present result Tuple4 as Stats
    MultiAggregator(maxOp, minOp, sum, moments)
      .andThenPresent { case (mmax, mmin, ssum, moment) =>
        Stats(mmax.score, mmin.score, ssum, moment.count, moment.mean, moment.stddev)
      }
  }

  def scalding(input: TypedPipe[Rating]): TypedPipe[Stats] = {
    input.aggregate(aggregator)
  }

  def scio(input: SCollection[Rating]): SCollection[Stats] = {
    input
      .map(_.score)
      .stats()
      .map(s => Stats(s.max, s.min, s.sum, s.count, s.mean, s.stdev))
  }

  def scioAlgebird(input: SCollection[Rating]): SCollection[Stats] = {
    input.aggregate(aggregator)
  }

  def spark(input: RDD[Rating]): Stats = {
    val s = input.map(_.score).stats()
    Stats(s.max, s.min, s.sum, s.count, s.mean, s.stdev)
  }

  def sparkAlgebird(input: RDD[Rating]): Stats = {
    import com.twitter.algebird.spark._
    input.algebird.aggregate(aggregator)
  }

}
