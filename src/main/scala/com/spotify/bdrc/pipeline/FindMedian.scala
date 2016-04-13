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
 * Compute the median of a collection of numbers.
 */
class FindMedian {

  // Computing the exact median is very expensive as it requires sorting and counting elements.
  // QTree is a compact data structure for approximate quantile and range queries.

  def scalding(input: TypedPipe[Long]): TypedPipe[(Double, Double)] = {
    import com.twitter.algebird._
    input
      .aggregate(QTreeAggregator[Long](0.5))
      .map(i => (i.lower.lower, i.upper.upper))
  }

  def scio(input: SCollection[Long]): SCollection[(Double, Double)] = {
    import com.twitter.algebird._
    input
      .aggregate(QTreeAggregator[Long](0.5))
      .map(i => (i.lower.lower, i.upper.upper))
  }

  def spark(input: RDD[Long]): (Double, Double) = {
    import com.twitter.algebird._
    import com.twitter.algebird.spark._
    val i = input.algebird.aggregate(QTreeAggregator[Long](0.5))
    (i.lower.lower, i.upper.upper)
  }

  // TODO: exact version

}
