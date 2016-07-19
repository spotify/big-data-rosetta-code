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

object WordCount1 {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(kv => kv._1 + ": " + kv._2)
      .saveAsTextFile(args("output"))
  }
}

/**
 * Test an entire pipeline end-to-end
 *
 * Pros:
 * - Complete test of the entire pipeline
 * - Covers argument parsing and I/O handling
 * - May also reveal serialization issues
 *
 * Cons:
 * - Hard to handcraft input and expected data
 * - Hard to cover edge cases for complex pipelines
 * - Can be slow in some frameworks
 *
 * Supported in: Scalding, Scio
 *
 * Recommendation:
 * This is a good approach to test small and simple pipelines since it offers the best code
 * coverage. It can also be used for pipelines with complex argument parsing and I/O handling,
 * e.g. ones with dynamic I/O based on arguments.
 *
 * Very complex pipelines with lots of steps may be broken down into smaller logical blocks and
 * tested separately using the transform test approach.
 */
class T01EndToEndTest extends PipelineSpec {

  val input = Seq("a b c d e", "a b a b")
  val expected = Seq("a: 3", "b: 3", "c: 1", "d: 1", "e: 1")

  "WordCount1" should "work" in {
    JobTest[com.spotify.bdrc.testing.WordCount1.type]
      .args("--input=in.txt", "--output=out.txt")
      .input(TextIO("in.txt"), input)
      .output(TextIO("out.txt")) { output =>
        output should containInAnyOrder (expected)
      }
      .run()
  }

}
