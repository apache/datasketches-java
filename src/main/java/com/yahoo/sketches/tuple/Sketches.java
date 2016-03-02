/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import com.yahoo.sketches.memory.Memory;

/**
 * Static methods to instantiate sketches
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
   * @return Sketch created from its Memory representation
   */
  public static <S extends Summary> Sketch<S> heapifySketch(final Memory mem) {
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.QuickSelectSketch) {
      return new QuickSelectSketch<S>(mem);
    }
    return new CompactSketch<S>(mem);
  }

  /**
   * Instantiate UpdatableSketch from a given Memory
   * @param <U> Type of update value
   * @param <S> Type of Summary
   * @param mem Memory object representing a Sketch
   * @return Sketch created from its Memory representation
   */
  public static <U, S extends UpdatableSummary<U>> UpdatableSketch<U, S> heapifyUpdatableSketch(final Memory mem) {
    return new UpdatableSketch<U, S>(mem);
  }

}
