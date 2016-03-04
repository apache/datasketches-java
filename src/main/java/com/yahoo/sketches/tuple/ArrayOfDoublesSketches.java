/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.memory.Memory;

public final class ArrayOfDoublesSketches {

  public static ArrayOfDoublesSketch heapifySketch(final Memory mem) {
    return heapifySketch(mem, DEFAULT_UPDATE_SEED);
  }

  public static ArrayOfDoublesSketch heapifySketch(final Memory mem, final long seed) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new HeapArrayOfDoublesQuickSelectSketch(mem, seed);
    }
    return new HeapArrayOfDoublesCompactSketch(mem, seed);
  }

  public static ArrayOfDoublesSketch wrapSketch(final Memory mem) {
    return wrapSketch(mem, DEFAULT_UPDATE_SEED);
  }

  public static ArrayOfDoublesSketch wrapSketch(final Memory mem, final long seed) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new DirectArrayOfDoublesQuickSelectSketch(mem, seed);
    }
    return new DirectArrayOfDoublesCompactSketch(mem, seed);
  }

  public static ArrayOfDoublesUnion heapifyUnion(final Memory mem) {
    return heapifyUnion(mem, DEFAULT_UPDATE_SEED);
  }

  public static ArrayOfDoublesUnion heapifyUnion(final Memory mem, final long seed) {
    return new HeapArrayOfDoublesUnion(mem, seed);
  }

  public static ArrayOfDoublesUnion wrapUnion(final Memory mem) {
    return wrapUnion(mem, DEFAULT_UPDATE_SEED);
  }

  public static ArrayOfDoublesUnion wrapUnion(final Memory mem, final long seed) {
    return new DirectArrayOfDoublesUnion(mem, seed);
  }

}
