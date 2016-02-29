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

import com.spotify.bdrc.util.Records.LogEvent
import com.spotify.scio.extra.Iterators._
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * Detect sessions from log and compute duration and number of items.
 *
 * Input is a collection of log events.
 */
object Sessions {

  val gapDuration = 3600000

  case class Session(user: String, duration: Long, numItems: Int)

  /** Wrapper for Iterator[LogEvent] that group items into sessions. */
  class SessionIterator(self: Iterator[LogEvent]) extends Iterator[Seq[LogEvent]] {
    private val bi = self.buffered  // BufferedIterator allows peak ahead
    override def hasNext: Boolean = bi.hasNext
    override def next(): Seq[LogEvent] = {
      val buf = mutable.Buffer(bi.next())
      var last = buf.head.timestamp

      // consume subsequent events until a gap is detected
      while (bi.hasNext && bi.head.timestamp - last < gapDuration) {
        val n = bi.next()
        buf.append(n)
        last = n.timestamp
      }
      buf
    }
  }

  def scalding(input: TypedPipe[LogEvent]): TypedPipe[Session] = {
    input
      .groupBy(_.user)
      .sortBy(_.timestamp)  // secondary sort
      // iterate over values lazily and group items into sessions
      .mapValueStream(new SessionIterator(_))
      .toTypedPipe
      .map { case (user, items) =>
        Session(user, items.last.timestamp - items.head.timestamp, items.size)
      }
  }

  def scio(input: SCollection[LogEvent]): SCollection[Session] = {
    input
      .groupBy(_.user)  // no secondary sort in Scio
      .flatMapValues { _
        .iterator
        .timeSeries(_.timestamp)  // generic version of SessionIterator from scio-extra
        .session(gapDuration)
      }
      .map {case (user, items) =>
        Session(user, items.last.timestamp - items.head.timestamp, items.size)
      }
  }

  def spark(input: RDD[LogEvent]): RDD[Session] = {
    input
      .groupBy(_.user)  // no secondary sort in Spark
      .flatMapValues { _
        .iterator
        .timeSeries(_.timestamp)  // generic version of SessionIterator from scio-extra
        .session(gapDuration)
      }
      .map {case (user, items) =>
        Session(user, items.last.timestamp - items.head.timestamp, items.size)
      }
  }

}
