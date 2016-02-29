/*
 * Copyright (c) 2016 Spotify AB.
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

/**
 * Handling data with multiple Option[T]s more gracefully.
 *
 * Input is a collection of case classes with nested Option[T].
 */
object HandlingOptions {

  case class Metadata(track: Option[Track], audio: Option[Audio])
  case class Track(id: String, name: String, artist: Option[Artist])
  case class Artist(id: String, name: String)
  case class Audio(tempo: Int, key: String)

  /** Naive approach that checks every field accessed is defined. */
  def naive(input: Seq[Metadata]): Seq[(String, Int)] = {
    input
      .filter { m =>
        m.track.isDefined && m.track.get.artist.isDefined && m.audio.isDefined
      }
      .map { m =>
        // Option[T].get is safe since we already checked with Option[T].isDefined
        (m.track.get.artist.get.id, m.audio.get.tempo)
      }
  }

  /**
   * Smart approach that uses for comprehension.
   *
   * For-comprehension extracts values from Options and yields Some if all Options are defined.
   * It yields None if any of the Options is None.
   */
  def withFlatMap(input: Seq[Metadata]): Seq[(String, Int)] = {
    input.flatMap { md =>
      for {
        tr <- md.track // extract Track from Option[Track]
        ar <- tr.artist // extract Artist from Option[Artist]
        au <- md.audio // extract Audio from Option[Audio]
      } yield (ar.id, au.tempo)
    }
  }

  /** The for-comprehension above translates to nested flatMaps. */
  def withNestedFlatMap(input: Seq[Metadata]): Seq[(String, Int)] = {
    input.flatMap { md =>
      md.track.flatMap { tr =>
        tr.artist.flatMap { ar =>
          md.audio.map { au =>
            (ar.id, au.tempo)
          }
        }
      }
    }
  }

}
