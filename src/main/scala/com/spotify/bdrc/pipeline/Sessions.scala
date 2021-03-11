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

// Example: Compute Session Duration and Number of Items from Log Data
// Input is a collection of log events
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.LogEvent
import com.spotify.scio.extra.Iterators._
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD
import org.joda.time.Instant

import scala.collection.mutable

object Sessions {

  val gapDuration = 3600000

  case class Session(user: String, duration: Long, numItems: Int)

  // Wrapper for `Iterator[LogEvent]` that group items into sessions
  class SessionIterator(self: Iterator[LogEvent]) extends Iterator[Seq[LogEvent]] {
    // `BufferedIterator` allows peak ahead
    private val bi = self.buffered
    override def hasNext: Boolean = bi.hasNext
    override def next(): Seq[LogEvent] = {
      val buf = mutable.Buffer(bi.next())
      var last = buf.head.timestamp

      // Consume subsequent events until a gap is detected
      while (bi.hasNext && bi.head.timestamp - last < gapDuration) {
        val n = bi.next()
        buf.append(n)
        last = n.timestamp
      }
      buf
    }
  }

  // ## Scalding
  def scalding(input: TypedPipe[LogEvent]): TypedPipe[Session] = {
    input
      .groupBy(_.user)
      // `sortBy` uses Hadoop secondary sort to sort keys during shuffle
      .sortBy(_.timestamp)
      // Iterate over values lazily and group items into sessions
      .mapValueStream(new SessionIterator(_))
      .toTypedPipe
      // Map over each (user, session items)
      .map { case (user, items) =>
        Session(user, items.last.timestamp - items.head.timestamp, items.size)
      }
  }

  // ## Scio
  def scio(input: SCollection[LogEvent]): SCollection[Session] = {
    input
      // Values in `groupBy` are sorted by timestamp
      .timestampBy(e => new Instant(e.timestamp))
      // No secondary sort in Scio, shuffle all items
      .groupBy(_.user)
      .flatMapValues {
        _.iterator
          // Generic version of `SessionIterator` from `scio-extra`
          .timeSeries(_.timestamp)
          .session(gapDuration)
      }
      // Map over each (user, session items)
      .map { case (user, items) =>
        Session(user, items.last.timestamp - items.head.timestamp, items.size)
      }
  }

  // ## Spark
  def spark(input: RDD[LogEvent]): RDD[Session] = {
    input
      // No secondary sort in Spark, shuffle all items
      .groupBy(_.user)
      .flatMapValues {
        _
          // Order of values after shuffle is not guaranteed
          .toList
          .sortBy(_.timestamp)
          .iterator
          // Generic version of `SessionIterator` from `scio-extra`
          .timeSeries(_.timestamp)
          .session(gapDuration)
      }
      // Map over each (user, session items)
      .map { case (user, items) =>
        Session(user, items.last.timestamp - items.head.timestamp, items.size)
      }
  }

}
