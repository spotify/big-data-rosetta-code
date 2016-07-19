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

package com.spotify.bdrc.testing

import com.google.common.collect.MinMaxPriorityQueue
import org.scalacheck.Prop.{BooleanOperators, all, forAll}
import org.scalacheck.{Gen, Properties}

import scala.collection.JavaConverters._

object Utils {

  def top[T: Ordering](xs: Seq[T], num: Int): Seq[T] = {
    if (xs.isEmpty) {
      Seq.empty[T]
    } else {
      val size = math.min(num, xs.size)
      val ord = implicitly[Ordering[T]]
      MinMaxPriorityQueue
        .orderedBy(ord.reverse)
        .expectedSize(size)
        .maximumSize(size)
        .create[T](xs.asJava)
        .asScala
        .toSeq
        .sorted(ord.reverse)
    }
  }

  def split(input: String): Seq[String] =
    input
      .split("[^a-zA-Z']+")
      .filter(_.nonEmpty)
      .map(_.toLowerCase)

  def cosineSim(v1: Seq[Double], v2: Seq[Double]): Double = {
    require(v1.length == v2.length)
    var s1 = 0.0
    var s2 = 0.0
    var dp = 0.0
    var i = 0
    while (i < v1.length) {
      s1 += v1(i) * v1(i)
      s2 += v2(i) * v2(i)
      dp += v1(i) * v2(i)
      i += 1
    }
    dp / math.sqrt(s1 * s2)
  }

}

/**
 * Property-based testing using ScalaCheck
 *
 * http://scalacheck.org/
 *
 * Pros:
 * - No need to handcraft input data
 * - May reveal rare edge cases, e.g. null input, extreme values, empty lists
 *
 * Cons:
 * - Hard to test business logic
 * - Some properties may be hard to verify
 * - Can be slow for expensive computations
 *
 * Supported in: any framework
 *
 * Recommendation:
 * This is useful for functions simple input and output types, especially those of heavy
 * mathematically computation, e.g. linear algebra, hash functions, set operations.
 *
 * However, since input data are randomly generated based on type signature, it might produce edge
 * cases irrelevant to the business logic, e.g. Double.MinValue, strings with Unicode characters.
 * You might also have to construct your own generator if certain distribution of input data is
 * expected, e.g. positive integers, strings from a finite set.
 *
 * See AlgebirdSpec.scala for more examples of testing Algebird features using ScalaCheck
 * https://github.com/spotify/scio/blob/master/scio-examples/src/test/scala/com/spotify/scio/examples/extra/AlgebirdSpec.scala
 */
object PropertyBasedTest extends Properties("Utils") {

  property("top") = forAll { xs: Seq[Long] =>
    Utils.top(xs, 5) == xs.sorted.reverse.take(5)
  }

  property("split") = forAll { line: String =>
    Utils.split(line).forall(_.matches("[a-z']+"))
  }

  // Generator for List[Double] of 100 doubles between -100.0 and 100.0
  val genVector = Gen.listOfN(100, Gen.choose(-100.0, 100.0))

  property("cosineSim") = forAll(genVector, genVector) { (v1, v2) =>
    val s1 = Utils.cosineSim(v1, v2)
    val s2 = Utils.cosineSim(v2, v1)
    all (
      (s1 >= -1.0 && s1 <= 1.0) :| "is between [-1.0, 1.0]",
      (s1 == s2) :| "is symmetrical",
      (Utils.cosineSim(v1, v1) == 1.0) :| "is 1.0 for (v, v)",
      (Utils.cosineSim(v1, v1.map(-_)) == -1.0) :| "is -1.0 for (v, -v)"
    )
  }

}
