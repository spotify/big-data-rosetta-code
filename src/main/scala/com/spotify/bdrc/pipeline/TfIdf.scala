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

import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

/**
 * Compute TF-IDF for a set of documents.
 *
 * Input is a Seq of (doc, text).
 */
object TfIdf {

  case class Score(term: String, doc: String, score: Double)

  def scalding(input: Seq[(String, TypedPipe[String])]): TypedPipe[Score] = {
    val numDocs = input.size

    val docToTerms = input
      .map { case (doc, pipe) =>
        pipe
          .flatMap(_.split("\\W+").filter(_.nonEmpty))
          .map(t => (doc, t.toLowerCase))
      }
      // union input collections
      .reduce(_ ++ _)  // (d, t)

    val docToTermAndFreq = docToTerms
      .groupBy(identity)
      .size
      .toTypedPipe
      .map { case ((d, t), tf) => (d, (t, tf)) }

    val termToDfN = docToTerms
      .distinct
      .values
      .groupBy(identity)
      .size  // (t, df)
      .mapValues(_.toDouble / numDocs)  // (t, df/N)

    docToTerms
      .keys
      .groupBy(identity)
      .size  // (d, |d|)
      .join(docToTermAndFreq)
      .toTypedPipe
      .map { case (d, (dLen, (t, tf))) => (t, (d, tf.toDouble / dLen)) }  // (t, (d, tf/|d|))
      .join(termToDfN)
      .toTypedPipe
      .map { case (t, ((d, tfd), dfN)) => Score(t, d, tfd * math.log(1 / dfN)) }
  }

  def scio(input: Seq[(String, SCollection[String])]): SCollection[Score] = {
    val numDocs = input.size

    val docToTerms = input
      .map { case (doc, pipe) =>
        pipe
          .flatMap(_.split("\\W+").filter(_.nonEmpty))
          .map(t => (doc, t.toLowerCase))
      }
      // union input collections
      .reduce(_ ++ _)  // (d, t)

    val docToTermAndCFreq = docToTerms
      // equivalent to .countByValue but returns RDD instead of Map
      .map((_, 1L))
      .reduceByKey(_ + _)
      .map{ case ((d, t), tf) => (d, (t, tf)) }

    val termToDfN = docToTerms
      .distinct
      .values
      // equivalent to .countByValue but returns RDD instead of Map
      .map((_, 1L))
      .reduceByKey(_ + _)  // (t, df)
      .mapValues(_.toDouble / numDocs)  // (t, df/N)

    docToTerms
      .keys
      // equivalent to .countByValue but returns RDD instead of Map
      .map((_, 1L))
      .reduceByKey(_ + _)  // (d, |d|)
      .join(docToTermAndCFreq)
      .map { case (d, (dLen, (t, tf))) => (t, (d, tf.toDouble / dLen)) }  // (t, (d, tf/|d|))
      .join(termToDfN)
      .map { case (t, ((d, tfd), dfN)) => Score(t, d, tfd * math.log(1 / dfN)) }
  }

  /** Spark implementation using transformations to keep computation distributed. */
  def sparkTransformations(input: Seq[(String, RDD[String])]): RDD[Score] = {
    val numDocs = input.size

    val docToTerms = input
      .map { case (doc, pipe) =>
        pipe
          .flatMap(_.split("\\W+").filter(_.nonEmpty))
          .map(t => (doc, t.toLowerCase))
      }
      // union input collections
      .reduce(_ ++ _)  // (d, t)
      .cache()  // docToTerms is reused 3 times

    val docToTermAndCFreq = docToTerms
      // equivalent to .countByValue but returns RDD instead of Map
      .map((_, 1L))
      .reduceByKey(_ + _)
      .map{ case ((d, t), tf) => (d, (t, tf)) }

    val termToDfN = docToTerms
      .distinct()
      .values
      // equivalent to .countByValue but returns RDD instead of Map
      .map((_, 1L))
      .reduceByKey(_ + _)  // (t, df)
      .mapValues(_.toDouble / numDocs)  // (t, df/N)

    docToTerms
      .keys
      // equivalent to .countByValue but returns RDD instead of Map
      .map((_, 1L))
      .reduceByKey(_ + _)  // (d, |d|)
      .join(docToTermAndCFreq)
      .map { case (d, (dLen, (t, tf))) => (t, (d, tf.toDouble / dLen)) }  // (t, (d, tf/|d|))
      .join(termToDfN)
      .map { case (t, ((d, tfd), dfN)) => Score(t, d, tfd * math.log(1 / dfN)) }
  }

  /** Spark implementation using actions to compute some steps on the driver node. */
  def sparkActions(input: Seq[(String, RDD[String])]): Seq[Score] = {
    val numDocs = input.size

    val docToTerms = input
      .map { case (doc, pipe) =>
        pipe
          .flatMap(_.split("\\W+").filter(_.nonEmpty))
          .map(t => (doc, t.toLowerCase))
      }
      .reduce(_ ++ _)  // (d, t)
      .cache()  // docToTerms is reused 3 times

    val docToTermAndCFreq = docToTerms
      .countByValue()
      // performed on driver node
      .map{ case ((d, t), tf) => (d, (t, tf)) }

    val termToDfN = docToTerms
      .distinct()
      .values
      .countByValue()  // (t, df)
      // performed on driver node
      .mapValues(_.toDouble / numDocs)  // (t, df/N)

    docToTerms
      .keys
      .countByValue()  // (d, |d|)
      // performed on driver node
      .toSeq
      .map { case (d, dLen) =>
        val (t, tf) = docToTermAndCFreq(d)
        //(t, (d, tf.toDouble / dLen))  // (t, (d, tf/|d|))
        val tfd = tf.toDouble / dLen
        val dfN = termToDfN(t)
        Score(t, d, tfd * math.log(1 / dfN))
      }
  }

}
