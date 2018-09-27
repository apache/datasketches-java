/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * The on-heap implementation of the Union set operation for tuple sketches of type
 * ArrayOfDoubles.
 */
final class HeapArrayOfDoublesUnion extends ArrayOfDoublesUnion {

  /**
   * Creates an instance of HeapArrayOfDoublesUnion with a custom seed
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesUnion(final int nomEntries, final int numValues, final long seed) {
    super(new HeapArrayOfDoublesQuickSelectSketch(nomEntries, 3, 1f, numValues, seed));
  }

  HeapArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch) {
    super(sketch);
  }

  /**
   * This is to create an instance given a serialized form and a custom seed
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a ArrayOfDoublesUnion on the Java heap
   */
  static ArrayOfDoublesUnion heapifyUnion(final Memory mem, final long seed) {
    final SerializerDeserializer.SketchType type = SerializerDeserializer.getSketchType(mem);

    // compatibility with version 0.9.1 and lower
    if (type == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      final ArrayOfDoublesQuickSelectSketch sketch = new HeapArrayOfDoublesQuickSelectSketch(mem, seed);
      return new HeapArrayOfDoublesUnion(sketch);
    }

    final byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesUnion);

    final long unionTheta = mem.getLong(THETA_LONG);
    final Memory sketchMem = mem.region(PREAMBLE_SIZE_BYTES, mem.getCapacity() - PREAMBLE_SIZE_BYTES);
    final ArrayOfDoublesQuickSelectSketch sketch = new HeapArrayOfDoublesQuickSelectSketch(sketchMem, seed);
    final ArrayOfDoublesUnion union = new HeapArrayOfDoublesUnion(sketch);
    union.theta_ = unionTheta;
    return union;
  }

  @Override
  public void reset() {
    sketch_ = new HeapArrayOfDoublesQuickSelectSketch(nomEntries_, 3, 1f, numValues_, seed_);
    theta_ = sketch_.getThetaLong();
  }

}
