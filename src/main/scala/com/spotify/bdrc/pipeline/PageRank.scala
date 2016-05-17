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
 * Classic PageRank.
 *
 * Input is a collection of (source URL, destination URL).
 */
object PageRank {

  val iterations = 10
  val dampingFactor = 0.85

  def scalding(input: TypedPipe[(String, String)]): TypedPipe[(String, Double)] = {
    val links = input
      .group
      .toList  // (src URL, list of dst URL)
    var ranks = input.keys.distinct.map((_, 1.0))  // (src URL, 1.0)

    for (i <- 1 to 10) {
      val contribs = links
        .join(ranks)
        .toTypedPipe
        .values
        // re-distribute rank of src URL among collection of dst URLs
        .flatMap { case (urls, rank) =>
          val size = urls.size
          urls.map((_, rank / size))
        }
      ranks = contribs
        .group
        .sum
        .mapValues((1 - dampingFactor) + dampingFactor * _)
        .toTypedPipe
    }

    ranks
  }

  def scio(input: SCollection[(String, String)]): SCollection[(String, Double)] = {
    val links = input.groupByKey
    var ranks = links.mapValues(_ => 1.0)

    for (i <- 1 to 10) {
      val contribs = links
        .join(ranks)
        .values
        .flatMap { case (urls, rank) =>
          val size = urls.size
          urls.map((_, rank / size))
        }
      ranks = contribs
        .sumByKey
        .mapValues((1 - dampingFactor) + dampingFactor * _)
    }

    ranks
  }

  def spark(input: RDD[(String, String)]): RDD[(String, Double)] = {
    val links = input
      .groupByKey()  // (src URL, iterable of dst URL)
      .cache()  // links is reused in every iteration
    var ranks = links.mapValues(_ => 1.0)  // (src URL, 1.0)

    for (i <- 1 to 10) {
      val contribs = links
        .join(ranks)
        .values
        // re-distribute rank of src URL among collection of dst URLs
        .flatMap { case (urls, rank) =>
          val size = urls.size
          urls.map((_, rank / size))
        }
      ranks = contribs
        .reduceByKey(_ + _)
        .mapValues((1 - dampingFactor) + dampingFactor * _)
    }

    ranks
  }
}
