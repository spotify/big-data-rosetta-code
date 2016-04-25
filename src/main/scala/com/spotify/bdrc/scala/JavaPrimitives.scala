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

package com.spotify.bdrc.scala

/**
 * Examples for working with Java primitives.
 */
object JavaPrimitives {

  // java.lang.Double is a boxed type (object) while double in Java is a primitive type.
  // scala.Double can be either boxed or primitive depending on the context, e.g. it's boxed when
  // used as a type parameter in a generic class but primitive when used in an array or on the
  // stack.
  // Due to type system limitations, M[java.lang.Double] and M[scala.Double] are incompatible types
  // but they can be casted safely back and forth since both are implemented as Java boxed types.
  import java.lang.{Double => JDouble}
  import java.util.{List => JList}

  import scala.collection.JavaConverters._

  /**
   * `xs.asScala` returns `mutable.Buffer[JDouble]` where `Buffer` is a sub-type of `Seq` but
   * `JDouble` is not the same type as `Double` (`scala.Double`). Casting is safe because `JDouble`
   * and `Double` are equivalent when used as type parameters (boxed objects). It's also cheaper
   * than `.map(_.toDouble)` which creates a copy of the `Buffer`.
   */
  def jDoubleListToSeq(xs: JList[JDouble]): Seq[Double] = xs.asScala.asInstanceOf[Seq[Double]]

  /**
   * Array[Double] is more efficient since it's implemented as a Java primitive array. Arrays are
   * also mutable so it'scheaper to pre-allocate and mutate elements. Java iterator and while loop
   * are faster than `xs.asScala.asInstanceOf[Seq[Double]].toArray`.
   */
  def jDoubleListToArray(xs: JList[JDouble]): Array[Double] = {
    val a = new Array[Double](xs.size())
    var i = 0
    val iterator = xs.iterator()
    while (iterator.hasNext) {
      a(i) = iterator.next()
      i += 1
    }
    a
  }

}
