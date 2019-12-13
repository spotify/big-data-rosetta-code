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

// Example: Join Log and Metadata Datasets
// Compute average age of users who listened to a track by joining log event and user metadata.
//
// - LHS input is a large collection of (user, page, timestamp).
// - RHS input is a small collection of (user, age).
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.{LogEvent, UserMeta}
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object JoinLogAndMetadata {

  // ## Scalding Naive Approach
  def scaldingNaive(
    left: TypedPipe[LogEvent],
    right: TypedPipe[UserMeta]
  ): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    left
      .groupBy(_.user)
      // Join as (user, (LogEvent, UserMeta))
      .join(right.groupBy(_.user))
      // Drop user key
      .values
      // Map into (track, age)
      .map {
        case (logEvent, userMeta) =>
          (logEvent.track, userMeta.age.toDouble)
      }
      .group
      // Aggregate average age per track
      .aggregate(AveragedValue.aggregator)
      .toTypedPipe
  }

  // ## Scalding with Hash Join
  // `hashJoin` replicates the smaller RHS to all mappers on the LHS
  def scaldingHashJoin(
    left: TypedPipe[LogEvent],
    right: TypedPipe[UserMeta]
  ): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.AveragedValue

    // Map out fields to avoid shuffing large objects
    val lhs = left.map(e => (e.user, e.track))
    // Force to disk to avoid repeating the same computation on each mapper on the LHS
    val rhs = right.map(u => (u.user, u.age.toDouble)).forceToDisk

    lhs
      .hashJoin(rhs)
      .values
      .group
      .aggregate(AveragedValue.aggregator)
      .toTypedPipe
  }

  // ## Scio Naive Approach
  def scioNaive(
    left: SCollection[LogEvent],
    right: SCollection[UserMeta]
  ): SCollection[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble))
    // Join as (user, (track, age))
    lhs
      .join(rhs)
      // Drop user key to make track as new key in (track, age)
      .values
      // Aggregate average age per track
      .aggregateByKey(AveragedValue.aggregator)
  }

  // ## Scio with Side Input
  // Side input makes RHS available on all workers
  def scioSideInput(
    left: SCollection[LogEvent],
    right: SCollection[UserMeta]
  ): SCollection[(String, Double)] = {
    import com.twitter.algebird.AveragedValue

    // Convert RHS to a side input of `Map[String, Double]`
    val rhs = right.map(u => (u.user, u.age.toDouble)).asMapSideInput

    // Replicate RHS to each worker
    left
      .withSideInputs(rhs)
      // Access side input via the context
      .map { case (e, sideContext) => (e.track, sideContext(rhs).getOrElse(e.user, 0.0)) }
      // Convert back to regular SCollection
      .toSCollection
      .aggregateByKey(AveragedValue.aggregator)
  }

  // ## Scio with Hash Join
  // `hashJoin` is a short cut to the side input approach
  def scioHashJoin(
    left: SCollection[LogEvent],
    right: SCollection[UserMeta]
  ): SCollection[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble))
    lhs
      .hashJoin(rhs)
      .values
      .aggregateByKey(AveragedValue.aggregator)
  }

  // ## Spark Naive Approach
  def sparkNaive(left: RDD[LogEvent], right: RDD[UserMeta]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    import com.twitter.algebird.AveragedValue
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble))
    // Join as (user, (track, age))
    lhs
      .join(rhs)
      // Drop user key to make track as new key in (track, age)
      .values
      .algebird
      // Aggregate average age per track
      .aggregateByKey(AveragedValue.aggregator)
  }

  // ## Spark with Broadcast Variable
  def sparkBroadcast(left: RDD[LogEvent], right: RDD[UserMeta]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    import com.twitter.algebird.AveragedValue

    // Retrieve `SparkContext` for creating broadcast variable
    val sc = left.context

    // Collect RHS to driver memory and broadcast back to workers
    val map = right.map(u => (u.user, u.age.toDouble)).collectAsMap()
    val b = sc.broadcast(map)

    left
    // In-memory lookup on each worker
      .map(e => (e.track, b.value.getOrElse(e.user, 0.0)))
      .algebird
      .aggregateByKey(AveragedValue.aggregator)
  }

}
