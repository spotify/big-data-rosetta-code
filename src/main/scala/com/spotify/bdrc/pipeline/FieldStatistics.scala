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
 * Compute basic statistics for each field.
 *
 * Input is a collection of case classes.
 */
object FieldStatistics {

  case class User(age: Int, income: Double, score: Double)
  case class Stats(max: Double, min: Double, mean: Double, stddev: Double)
  case class UserStats(age: Stats, income: Stats, score: Stats)

  // Algebird Aggregator
  def aggregator = {
    import com.twitter.algebird._

    /*
    3 aggregators on age field with different logic

    The first 3 aggregators are of type Aggregator[User, _, Double] which means it takes User as
    input and outputs Double. The last aggregator is of type Aggregator[Rating, _, Moments].
    The input User is prepared with a (User => Double) function _.age.
     */
    val maxAgeOp = Aggregator.max[Int].composePrepare[User](_.age)
    val minAgeOp = Aggregator.min[Int].composePrepare[User](_.age)
    val momentsAgeOp = Moments.aggregator.composePrepare[User](_.age)

    // 3 aggregators on income field with different logic
    val maxIncomeOp = Aggregator.max[Double].composePrepare[User](_.income)
    val minIncomeOp = Aggregator.min[Double].composePrepare[User](_.income)
    val momentsIncomeOp = Moments.aggregator.composePrepare[User](_.income)

    // 3 aggregators on score field with different logic
    val maxScoreOp = Aggregator.max[Double].composePrepare[User](_.score)
    val minScoreOp = Aggregator.min[Double].composePrepare[User](_.score)
    val momentsScoreOp = Moments.aggregator.composePrepare[User](_.score)

    MultiAggregator(
      maxAgeOp, minAgeOp, momentsAgeOp,
      maxIncomeOp, minIncomeOp, momentsIncomeOp,
      maxScoreOp, minScoreOp, momentsScoreOp)
      .andThenPresent { t =>
        val (maxAge, minAge, mAge, maxIncome, minIncome, mIncome, maxScore, minScore, mScore) = t
        UserStats(
          age = Stats(maxAge, minAge, mAge.mean, mAge.stddev),
          income = Stats(maxIncome, minIncome, mIncome.mean, mIncome.stddev),
          score = Stats(maxScore, minScore, mScore.mean, mScore.stddev))
      }
  }

  def scalding(input: TypedPipe[User]): TypedPipe[UserStats] = {
    input.aggregate(aggregator)
  }

  def scio(input: SCollection[User]): SCollection[UserStats] = {
    input.aggregate(aggregator)
  }

  def spark(input: RDD[User]): UserStats = {
    // compute each field separately, potentially in-efficient if input is not cached
    val s1 = input.map(_.age).stats()
    val s2 = input.map(_.income).stats()
    val s3 = input.map(_.score).stats()
    UserStats(
      age = Stats(s1.max, s1.min, s1.mean, s1.stdev),
      income = Stats(s2.max, s2.min, s2.mean, s2.stdev),
      score = Stats(s3.max, s3.min, s3.mean, s3.stdev))
  }

  def sparkAlgebird(input: RDD[User]): UserStats = {
    import com.twitter.algebird.spark._
    input.algebird.aggregate(aggregator)
  }

}
