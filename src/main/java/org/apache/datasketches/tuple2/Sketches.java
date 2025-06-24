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

package org.apache.datasketches.tuple2;

import java.lang.foreign.MemorySegment;

/**
 * Convenient static methods to instantiate generic tuple sketches.
 */
@SuppressWarnings("deprecation")
public final class Sketches {

  /**
   * Creates an empty sketch.
   * @param <S> Type of Summary
   * @return an empty instance of Sketch
   */
  public static <S extends Summary> Sketch<S> createEmptySketch() {
    return new CompactSketch<>(null, null, Long.MAX_VALUE, true);
  }

  /**
   * Instantiate a Sketch from a given MemorySegment.
   * @param <S> Type of Summary
   * @param seg MemorySegment object representing a Sketch
   * @param deserializer instance of SummaryDeserializer
   * @return Sketch created from its MemorySegment representation
   */
  public static <S extends Summary> Sketch<S> heapifySketch(
      final MemorySegment seg,
      final SummaryDeserializer<S> deserializer) {
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(seg);
    if (sketchType == SerializerDeserializer.SketchType.QuickSelectSketch) {
      return new QuickSelectSketch<>(seg, deserializer, null);
    }
    return new CompactSketch<>(seg, deserializer);
  }

  /**
   * Instantiate UpdatableSketch from a given MemorySegment
   * @param <U> Type of update value
   * @param <S> Type of Summary
   * @param seg MemorySegment object representing a Sketch
   * @param deserializer instance of SummaryDeserializer
   * @param summaryFactory instance of SummaryFactory
   * @return Sketch created from its MemorySegment representation
   */
  public static <U, S extends UpdatableSummary<U>> UpdatableSketch<U, S> heapifyUpdatableSketch(
      final MemorySegment seg,
      final SummaryDeserializer<S> deserializer,
      final SummaryFactory<S> summaryFactory) {
    return new UpdatableSketch<>(seg, deserializer, summaryFactory);
  }

}
