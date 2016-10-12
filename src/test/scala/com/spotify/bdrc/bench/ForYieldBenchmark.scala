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

package com.spotify.bdrc.bench

import java.lang.{Iterable => JIterable}

import com.google.common.collect.Lists
import org.scalameter.api._
import org.scalameter.picklers.noPickler._

import scala.collection.JavaConverters._

/** Micro-benchmark for for/yield pattern. */
object ForYieldBenchmark extends Bench.LocalTime {

  val lSizes = Gen.enumeration("lSize")(1, 10, 100, 1000)
  val rSizes = Gen.enumeration("rSize")(1, 10, 100, 1000)

  def jIterable(i: Int): JIterable[String] =
    Lists.newArrayList((0 until i).map("v%05d".format(_)): _*).asInstanceOf[JIterable[String]]

  val inputs = for {
    l <- lSizes
    r <- rSizes
  } yield (jIterable(l), jIterable(r))

  performance of "Join" in {
    measure method "forIterable" in {
      using(inputs) in { p =>
        for {
          a <- p._1.asScala
          b <- p._2.asScala
        } yield ("key", (a, b))
      }
    }

    // Iterator version is lazy and more efficient
    measure method "forIterator" in {
      using(inputs) in { p =>
        val r = for {
          a <- p._1.asScala.iterator
          b <- p._2.asScala.iterator
        } yield ("key", (a, b))
        r.toIterable
      }
    }
  }

}
