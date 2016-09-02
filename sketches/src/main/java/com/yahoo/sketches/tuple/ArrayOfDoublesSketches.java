/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

/**
 * Convenient static methods to instantiate tuple sketches of type ArrayOfDoubles.
 */
public final class ArrayOfDoublesSketches {

  /**
   * Heapify the given Memory as an ArrayOfDoublesSketch
   * @param mem the given Memory
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapifySketch(final Memory mem) {
    return heapifySketch(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as a ArrayOfDoublesSketch
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapifySketch(final Memory mem, final long seed) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new HeapArrayOfDoublesQuickSelectSketch(mem, seed);
    }
    return new HeapArrayOfDoublesCompactSketch(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesSketch
   * @param mem the given Memory
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrapSketch(final Memory mem) {
    return wrapSketch(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as a ArrayOfDoublesSketch
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrapSketch(final Memory mem, final long seed) {
    SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new DirectArrayOfDoublesQuickSelectSketch(mem, seed);
    }
    return new DirectArrayOfDoublesCompactSketch(mem, seed);
  }

  /**
   * Heapify the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapifyUnion(final Memory mem) {
    return heapifyUnion(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapifyUnion(final Memory mem, final long seed) {
    return new HeapArrayOfDoublesUnion(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final Memory mem) {
    return wrapUnion(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final Memory mem, final long seed) {
    return new DirectArrayOfDoublesUnion(mem, seed);
  }

}
