/*
 * Copyright 2017 Spotify AB.
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
import com.twitter.algebird._
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Filter LHS by removing items that exist in the RHS using a Bloom Filter.
 *
 * Inputs are collections of strings.
 */
object BloomFilterSetDifference {

  def scalding(lhs: TypedPipe[String], rhs: TypedPipe[String]): TypedPipe[String] = {
    val width = BloomFilter.optimalWidth(1000, 0.01).get
    val numHashes = BloomFilter.optimalNumHashes(1000, width)
    lhs
      .cross(rhs.aggregate(BloomFilterAggregator(numHashes, width)))
      .filter { case (s, bf) => bf.contains(s).isTrue }
      .keys
  }

  def scio(lhs: SCollection[String], rhs: SCollection[String]): SCollection[String] = {
    val width = BloomFilter.optimalWidth(1000, 0.01).get
    val numHashes = BloomFilter.optimalNumHashes(1000, width)
    lhs
      .cross(rhs.aggregate(BloomFilterAggregator(numHashes, width)))
      .filter { case (s, bf) => bf.contains(s).isTrue }
      .keys
  }

  def spark(lhs: RDD[String], rhs: RDD[String]): RDD[String] = {
    import com.twitter.algebird.spark._
    val width = BloomFilter.optimalWidth(1000, 0.01).get
    val numHashes = BloomFilter.optimalNumHashes(1000, width)
    val bf = rhs.algebird.aggregate(BloomFilterAggregator(numHashes, width))
    lhs.filter(s => bf.contains(s).isTrue)
  }

}
