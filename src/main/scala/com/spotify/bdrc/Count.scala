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

package com.spotify.bdrc

import com.spotify.bdrc.util.Records.UserItemData
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute number of items.
 *
 * Input is a collection of (user, item, score).
 */
object Count {

  def scalding(input: TypedPipe[UserItemData]): TypedPipe[Long] = {
    input
      .map(_ => 1L)
      .sum
      .toTypedPipe
  }

  def scaldingWithAlgebird(input: TypedPipe[UserItemData]): TypedPipe[Long] = {
    import com.twitter.algebird.Aggregator.size
    input
      .aggregate(size)
      .toTypedPipe
  }

  def spark(input: RDD[UserItemData]): Long = {
    input
      .count()
  }

  def sparkWithAlgebird(input: RDD[UserItemData]): Long = {
    import com.twitter.algebird.Aggregator.size
    import com.twitter.algebird.spark._
    input
      .algebird
      .aggregate(size)
  }

}
