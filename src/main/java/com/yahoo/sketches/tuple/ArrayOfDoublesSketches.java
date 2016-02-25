/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.memory.Memory;

public final class ArrayOfDoublesSketches {

  public static ArrayOfDoublesSketch heapify(Memory mem) {
    return heapify(mem, DEFAULT_UPDATE_SEED);
  }

  public static ArrayOfDoublesSketch heapify(Memory mem, long seed) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new HeapArrayOfDoublesQuickSelectSketch(mem, seed);
    }
    return new HeapArrayOfDoublesCompactSketch(mem, seed);
  }

  public static ArrayOfDoublesSketch wrap(Memory mem) {
    return wrap(mem, DEFAULT_UPDATE_SEED);
  }

  public static ArrayOfDoublesSketch wrap(Memory mem, long seed) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new DirectArrayOfDoublesQuickSelectSketch(mem, seed);
    }
    return new DirectArrayOfDoublesCompactSketch(mem, seed);
  }

}
