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

// Example: Compute Basic Descriptive Statistics for Each Field
// Input is a collection of case classes
package com.spotify.bdrc.pipeline

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object FieldStatistics {

  case class User(age: Int, income: Double, score: Double)
  case class Stats(max: Double, min: Double, mean: Double, stddev: Double)
  case class UserStats(age: Stats, income: Stats, score: Stats)

  import com.twitter.algebird._
  implicit val momentsCoder: Coder[Moments] = Coder.kryo[Moments]

  // ## Algebird `Aggregator`
  def aggregator = {
    // Create 3 `Aggregator`s on `age` field with different logic

    // The first 2 are of type `Aggregator[User, _, Int]` which means it takes `User` as input and
    // generates `Int` as output. The last one is of type `Aggregator[User, _, Moments]`,
    // where `Moments` include count, mean, standard deviation, etc. The input `User` is prepared
    // with a `User => Int` function `_.age`.
    val maxAgeOp = Aggregator.max[Int].composePrepare[User](_.age)
    val minAgeOp = Aggregator.min[Int].composePrepare[User](_.age)
    val momentsAgeOp = Moments.aggregator.composePrepare[User](_.age)

    // Create 3 `Aggregator`s on `income` field with different logic
    val maxIncomeOp = Aggregator.max[Double].composePrepare[User](_.income)
    val minIncomeOp = Aggregator.min[Double].composePrepare[User](_.income)
    val momentsIncomeOp = Moments.aggregator.composePrepare[User](_.income)

    // Create 3 `Aggregator`s on `score` field with different logic
    val maxScoreOp = Aggregator.max[Double].composePrepare[User](_.score)
    val minScoreOp = Aggregator.min[Double].composePrepare[User](_.score)
    val momentsScoreOp = Moments.aggregator.composePrepare[User](_.score)

    // Apply 12 `Aggregator`s on the same input, present result tuple 12 as `UserStats`.
    MultiAggregator(
      maxAgeOp,
      minAgeOp,
      momentsAgeOp,
      maxIncomeOp,
      minIncomeOp,
      momentsIncomeOp,
      maxScoreOp,
      minScoreOp,
      momentsScoreOp
    ).andThenPresent { t =>
      val (maxAge, minAge, mAge, maxIncome, minIncome, mIncome, maxScore, minScore, mScore) = t
      UserStats(
        age = Stats(maxAge, minAge, mAge.mean, mAge.stddev),
        income = Stats(maxIncome, minIncome, mIncome.mean, mIncome.stddev),
        score = Stats(maxScore, minScore, mScore.mean, mScore.stddev)
      )
    }
  }

  // ## Scalding
  def scalding(input: TypedPipe[User]): TypedPipe[UserStats] =
    input.aggregate(aggregator)

  // ## Scio
  def scio(input: SCollection[User]): SCollection[UserStats] =
    input.aggregate(aggregator)

  // ## Spark
  def spark(input: RDD[User]): UserStats = {
    // Compute each field separately, potentially in-efficient if input is not cached
    val s1 = input.map(_.age).stats()
    val s2 = input.map(_.income).stats()
    val s3 = input.map(_.score).stats()
    UserStats(
      age = Stats(s1.max, s1.min, s1.mean, s1.stdev),
      income = Stats(s2.max, s2.min, s2.mean, s2.stdev),
      score = Stats(s3.max, s3.min, s3.mean, s3.stdev)
    )
  }

  // ## Spark with Algebird `Aggregator`
  def sparkAlgebird(input: RDD[User]): UserStats = {
    import com.twitter.algebird.spark._
    input.algebird.aggregate(aggregator)
  }

}
