/*
 * Copyright 2017 Spotify AB.
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

// Example: Build Inverted Index
// Build inverted index from a corpus of text documents

// Input is a collection of (id, text)
package com.spotify.bdrc.pipeline

import com.spotify.scio.values.SCollection
import com.twitter.scalding.TypedPipe
import org.apache.spark.rdd.RDD

object InvertedIndex {

  case class Document(id: Int, text: String)
  case class Posting(word: String, ids: Seq[Int])

  // ## Scalding
  def scalding(input: TypedPipe[Document]): TypedPipe[Posting] = {
    input
    // Split text and output (word, document ID)
      .flatMap(d => d.text.split("[^a-zA-Z']+").map(w => (w, d.id)))
      // Group and convert document IDs per key to `List[Int]`
      .group
      .toList
      .map(Posting.tupled)
  }

  // ## Scio
  def scio(input: SCollection[Document]): SCollection[Posting] = {
    input
    // Split text and output (word, document ID)
      .flatMap(d => d.text.split("[^a-zA-Z']+").map(w => (w, d.id)))
      // Group document IDs per key into `Iterable[Int]`
      .groupByKey
      .map(kv => Posting(kv._1, kv._2.toSeq))
  }

  // ## Spark
  def spark(input: RDD[Document]): RDD[Posting] = {
    input
    // Split text and output (word, document ID)
      .flatMap(d => d.text.split("[^a-zA-Z']+").map(w => (w, d.id)))
      // Group document IDs per key into `Iterable[Int]`
      .groupByKey()
      .map(kv => Posting(kv._1, kv._2.toSeq))
  }

}
