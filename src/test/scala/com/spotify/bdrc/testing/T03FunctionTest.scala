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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object WordCount3 {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.textFile(args("input"))
      .flatMap(split)
      .countByValue
      .map(format)
      .saveAsTextFile(args("output"))
  }

  def split(input: String): Seq[String] = input.split("[^a-zA-Z']+").filter(_.nonEmpty)
  def format(kv: (String, Long)): String = kv._1 + ": " + kv._2
}

/**
 * Test individual functions used in a pipeline
 *
 * Pros:
 * - Fastest to test
 * - Easy to cover edge cases
 *
 * Cons:
 * - Limited scope of coverage
 * - May disrupt pipeline logic flow if overused
 *
 * Supported in: any framework
 *
 * Recommendation:
 * This is recommended for commonly reused functions or those with complex business logic, e.g.
 * numerical computation, log clean up and filtering, value group operations after groupByKey.
 *
 * The level of granularity of each function is also important. Typical candidates are multi-line
 * functions that are used more than once. Functions with complex logic and hard to test at a
 * higher level (transform or end-to-end), e.g. user session analysis after grouping by user key,
 * can also be tested with this approach.
 */
class FunctionTest extends AnyFlatSpec with Matchers {

  "split" should "work" in {
    WordCount3.split("a b,c d\te\n\nf") should equal(Seq("a", "b", "c", "d", "e", "f"))
  }

  "format" should "work" in {
    WordCount3.format(("a", 10L)) should equal("a: 10")
  }

}
