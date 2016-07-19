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

import com.spotify.scio._
import com.spotify.scio.testing._
import com.spotify.scio.values.SCollection

object WordCount4 {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val input = sc.textFile(args("input"))
    val wc = countWords(input)
    val output = formatOutput(wc)
    output.saveAsTextFile(args("output"))
  }

  // transforms
  def countWords(input: SCollection[String]): SCollection[(String, Long)] =
    input.flatMap(split).countByValue
  def formatOutput(input: SCollection[(String, Long)]): SCollection[String] =
    input.map(format)

  // functions
  def split(input: String): Seq[String] = input.split("[^a-zA-Z']+").filter(_.nonEmpty)
  def format(kv: (String, Long)): String = kv._1 + ": " + kv._2
}

/**
 * Mixed function, transform and end-to-end tests
 *
 * Property-based tests require an object that extends Properties and therefore are not included.
 */
class MixedTest extends PipelineSpec {

  val input = Seq("a b c d e", "a b a b")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")
  val intermediate = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))

  // Function tests

  "split" should "work" in {
    WordCount3.split("a b,c d\te\n\nf") should equal (Seq("a", "b", "c", "d", "e", "f"))
  }

  "format" should "work" in {
    WordCount3.format(("a", 10L)) should equal ("a: 10")
  }

  // Transform tests

  "countWords" should "work" in {
    runWithContext { sc =>
      val in = sc.parallelize(input)
      WordCount4.countWords(in) should containInAnyOrder (intermediate)
    }
  }

  "formatOutput" should "work" in {
    runWithContext { sc =>
      val in = sc.parallelize(intermediate)
      WordCount4.formatOutput(in) should containInAnyOrder (expected)
    }
  }

  // End-to-end test

  "WordCount1" should "work" in {
    JobTest[com.spotify.bdrc.testing.WordCount4.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), input)
      .output(TextIO("out.txt")) { output =>
        output should containInAnyOrder (expected)
      }
      .run()
  }

}
