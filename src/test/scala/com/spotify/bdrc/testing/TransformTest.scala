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

object WordCount2 {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val input = sc.textFile(args("input"))
    val wc = countWords(input)
    val output = formatOutput(wc)
    output.saveAsTextFile(args("output"))
  }

  def countWords(input: SCollection[String]): SCollection[(String, Long)] =
    input
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue

  def formatOutput(input: SCollection[(String, Long)]): SCollection[String] =
    input
      .map(kv => kv._1 + ": " + kv._2)
}

/**
 * Test pipeline transforms
 *
 * Pros:
 * - Break down complex pipelines into smaller reusable pieces
 * - Easier to handcraft input and expected data than end-to-end test
 *
 * Cons:
 * - Does not cover argument parsing and IO handling
 * - May disrupt pipeline logic flow if overused
 *
 * Supported in: Scio, Spark
 *
 * Recommendation:
 * Complex pipelines can be broken into logical blocks and tested using this approach. Individual
 * transforms should have clear roles in the pipeline, e.g. parsing input, formatting output,
 * aggregating data, training model, predicting labels, etc. It should also be easy to craft input
 * and expected data for these transforms and cover all code bases and edge cases.
 *
 * The level of granularity of each transform is also important. A transform should be small enough
 * for readability but big enough to avoid disruption to the main pipeline flow. Things to
 * consider are: number of inputs and outputs, group or join operations, etc.
 */
class TransformTest extends PipelineSpec {

  val input = Seq("a b c d e", "a b a b")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")
  val intermediate = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))

  "countWords" should "work" in {
    runWithContext { sc =>
      val in = sc.parallelize(input)
      WordCount2.countWords(in) should containInAnyOrder (intermediate)
    }
  }

  "formatOutput" should "work" in {
    runWithContext { sc =>
      val in = sc.parallelize(intermediate)
      WordCount2.formatOutput(in) should containInAnyOrder (expected)
    }
  }

}
