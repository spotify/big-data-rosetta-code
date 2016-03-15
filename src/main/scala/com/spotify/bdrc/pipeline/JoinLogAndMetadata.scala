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

import com.spotify.bdrc.util.Records.{UserMeta, LogEvent}
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute average age of users who listened to a track by joining log event and user meta.
 *
 * LHS input is a large collection of (user, page, timestamp).
 * RHS input is a small collection of (user, age).
 */
object JoinLogAndMetadata {

  /** Naive approach that joins the 2 inputs directly. */
  def scaldingNaive(left: TypedPipe[LogEvent], right: TypedPipe[UserMeta]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    left
      .groupBy(_.user)
      .join(right.groupBy(_.user))
      .values
      .map { case (logEvent, userMeta) =>
        (logEvent.track, userMeta.age.toDouble)
      }
      .group
      .aggregate(AveragedValue.aggregator)
      .toTypedPipe
  }

  /** HashJoin that replicates the smaller RHS to all mappers on the LHS. */
  def scaldingHashJoin(left: TypedPipe[LogEvent], right: TypedPipe[UserMeta]): TypedPipe[(String, Double)] = {
    import com.twitter.algebird.AveragedValue

    // map out LogEvent#page and UserMetadata#age to avoid shuffle fat objects
    // force to disk to avoid repeating the same computation on each mapper on the LHS
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble)).forceToDisk

    lhs
      .hashJoin(rhs)  // RHS is loaded into memory of each mapper on the LHS
      .values
      .group
      .aggregate(AveragedValue.aggregator)
      .toTypedPipe
  }

  /** Native approach that joins the 2 inputs directly. */
  def scioNaive(left: SCollection[LogEvent], right: SCollection[UserMeta]): SCollection[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble))
    lhs.join(rhs)
      .values
      .aggregateByKey(AveragedValue.aggregator)
  }

  /** Side input makes RHS available on all workers. */
  def scioSideInput(left: SCollection[LogEvent], right: SCollection[UserMeta]): SCollection[(String, Double)] = {
    import com.twitter.algebird.AveragedValue

    // convert RHS to a side input of Map[String, Double]
    val rhs = right.map(u => (u.user, u.age.toDouble)).asMapSideInput

    left.withSideInputs(rhs)  // replicate RHS to each worker
      // access side input via the context
      .map { case (e, sideContext) => (e.track, sideContext(rhs).getOrElse(e.user, 0.0)) }
      .toSCollection  // convert back to regular SCollection
      .aggregateByKey(AveragedValue.aggregator)
  }

  /** Scio hash-join is a short cut to the side input approach above. */
  def scioHashJoin(left: SCollection[LogEvent], right: SCollection[UserMeta]): SCollection[(String, Double)] = {
    import com.twitter.algebird.AveragedValue
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble))
    lhs.hashJoin(rhs)
      .values
      .aggregateByKey(AveragedValue.aggregator)
  }

  /** Native approach that joins the 2 inputs directly. */
  def sparkNaive(left: RDD[LogEvent], right: RDD[UserMeta]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    import com.twitter.algebird.AveragedValue
    val lhs = left.map(e => (e.user, e.track))
    val rhs = right.map(u => (u.user, u.age.toDouble))
    lhs.join(rhs)
      .values
      .algebird
      .aggregateByKey(AveragedValue.aggregator)
  }

  /** Collect and broadcast pattern. */
  def sparkBroadcast(left: RDD[LogEvent], right: RDD[UserMeta]): RDD[(String, Double)] = {
    import com.twitter.algebird.spark._
    import com.twitter.algebird.AveragedValue

    val sc = left.context // retrieve SparkContext

    // collect RHS to driver memory and broadcast back to workers
    val map = right.map(u => (u.user, u.age.toDouble)).collectAsMap()
    val b = sc.broadcast(map)

    left
      .map(e => (e.track, b.value.getOrElse(e.user, 0.0)))  // in-memory lookup on each worker
      .algebird
      .aggregateByKey(AveragedValue.aggregator)
  }

}
