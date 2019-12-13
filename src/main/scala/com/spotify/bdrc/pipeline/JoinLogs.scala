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

// Example: Join Two Log Datasets and Compute Action Sequence
// Given two log datasets of play track and save track events, compute tracks that a user saved
// after playing in a session.

// Inputs are collections of (user, item, timestamp).
package com.spotify.bdrc.pipeline

import com.spotify.bdrc.util.Records.LogEvent
import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object JoinLogs {

  val gapDuration = 3600000

  // Detect if a pair of (event type, LogEvent) tuples match a play and save sequence
  def detectPlaySaveSequence(pair: Seq[(String, LogEvent)]): Option[String] = {
    val Seq(first, second) = pair
    if (first._1 == "play" && second._1 == "save" && first._2.track == second._2.track &&
        second._2.timestamp - first._2.timestamp <= gapDuration) {
      Some(first._2.track)
    } else {
      None
    }
  }

  // ## Scalding
  def scalding(
    playEvents: TypedPipe[LogEvent],
    saveEvents: TypedPipe[LogEvent]
  ): TypedPipe[(String, String)] = {
    // Map inputs to key-values and add event type information
    val plays = playEvents.map(e => (e.user, ("play", e))).group
    val saves = saveEvents.map(e => (e.user, ("save", e))).group

    plays
      .cogroup(saves) { (user, p, s) =>
        // `Iterable`s of play and save events for the user
        (p ++ s).toList
          .sortBy(_._2.timestamp)
          // Neighboring pairs
          .sliding(2)
          .flatMap(detectPlaySaveSequence)
      }
      .toTypedPipe
  }

  // ## Scio
  def scio(
    playEvents: SCollection[LogEvent],
    saveEvents: SCollection[LogEvent]
  ): SCollection[(String, String)] = {
    // Map inputs to key-values and add event type information
    val plays = playEvents.map(e => (e.user, ("play", e)))
    val saves = saveEvents.map(e => (e.user, ("save", e)))

    plays
      .cogroup(saves)
      // `Iterable`s of play and save events for the user
      .flatMapValues {
        case (p, s) =>
          (p ++ s).toList
            .sortBy(_._2.timestamp)
            // Neighboring pairs
            .sliding(2)
            .flatMap(detectPlaySaveSequence)
      }
  }

  // ## Spark
  def spark(playEvents: RDD[LogEvent], saveEvents: RDD[LogEvent]): RDD[(String, String)] = {
    // Map inputs to key-values and add event type information
    val plays = playEvents.map(e => (e.user, ("play", e)))
    val saves = saveEvents.map(e => (e.user, ("save", e)))

    plays
      .cogroup(saves)
      .flatMapValues {
        case (p, s) =>
          // `Iterable`s of play and save events for the user
          (p ++ s).toList
            .sortBy(_._2.timestamp)
            // Neighboring pairs
            .sliding(2)
            .flatMap(detectPlaySaveSequence)
      }
  }

}
