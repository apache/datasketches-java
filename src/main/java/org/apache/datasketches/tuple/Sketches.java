/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple;

import org.apache.datasketches.memory.Memory;

/**
 * Convenient static methods to instantiate generic tuple sketches.
 */
public final class Sketches {

  /**
   * @param <S> Type of Summary
   * @return an empty instance of Sketch
   */
  public static <S extends Summary> Sketch<S> createEmptySketch() {
    return new CompactSketch<S>(null, null, Long.MAX_VALUE, true);
  }

  /**
   * Instantiate Sketch from a given Memory
   * @param <S> Type of Summary
   * @param mem Memory object representing a Sketch
   * @param deserializer instance of SummaryDeserializer
   * @return Sketch created from its Memory representation
   */
  public static <S extends Summary> Sketch<S> heapifySketch(final Memory mem,
      final SummaryDeserializer<S> deserializer) {
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.QuickSelectSketch) {
      return new QuickSelectSketch<S>(mem, deserializer, null);
    }
    return new CompactSketch<S>(mem, deserializer);
  }

  /**
   * Instantiate UpdatableSketch from a given Memory
   * @param <U> Type of update value
   * @param <S> Type of Summary
   * @param mem Memory object representing a Sketch
   * @param deserializer instance of SummaryDeserializer
   * @param summaryFactory instance of SummaryFactory
   * @return Sketch created from its Memory representation
   */
  public static <U, S extends 
      UpdatableSummary<U>> UpdatableSketch<U, S> heapifyUpdatableSketch(final Memory mem,
          final SummaryDeserializer<S> deserializer, final SummaryFactory<S> summaryFactory) {
    return new UpdatableSketch<U, S>(mem, deserializer, summaryFactory);
  }

}
