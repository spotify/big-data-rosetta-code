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
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute collection of distinct items.
 *
 * Input is a collection of (user, item, score).
 */
object DistinctItems {

  def scalding(input: TypedPipe[UserItemData]): TypedPipe[String] = {
    input
      .map(_.item)
      .distinct
  }

  def scio(input: SCollection[UserItemData]): SCollection[String] = {
    input
      .map(_.item)
      .distinct()
  }

  def spark(input: RDD[UserItemData]): RDD[String] = {
    input
      .map(_.item)
      .distinct()
  }

}
