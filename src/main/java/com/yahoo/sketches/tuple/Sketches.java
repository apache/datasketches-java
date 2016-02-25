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
   * @return an empty instance of Sketch
   */
  public static <S extends Summary> Sketch<S> createEmpty() {
    return new CompactSketch<S>(null, null, Long.MAX_VALUE, true);
  }

  /**
   * Instantiate Sketch from a given Memory
   * @param mem Memory object representing a Sketch
   * @return Sketch created from its Memory representation
   */
  public static <S extends Summary> Sketch<S> heapify(Memory mem) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.QuickSelectSketch) {
      return new QuickSelectSketch<S>(mem);
    }
    return new CompactSketch<S>(mem);
  }

}
