/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;

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
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
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
    final SerializerDeserializer.SketchType sketchType = SerializerDeserializer.getSketchType(mem);
    if (sketchType == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      return new DirectArrayOfDoublesQuickSelectSketchR((WritableMemory) mem, seed);
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
    return HeapArrayOfDoublesUnion.heapifyUnion(mem, seed);
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
    return wrapUnionImpl((WritableMemory) mem, seed, false);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final WritableMemory mem) {
    return wrapUnion(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final WritableMemory mem, final long seed) {
    return wrapUnionImpl(mem, seed, true);
  }

  static ArrayOfDoublesUnion wrapUnionImpl(final WritableMemory mem, final long seed, final boolean isWritable) {
    final SerializerDeserializer.SketchType type = SerializerDeserializer.getSketchType(mem);

    // compatibility with version 0.9.1 and lower
    if (type == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      final ArrayOfDoublesQuickSelectSketch sketch = new DirectArrayOfDoublesQuickSelectSketch(mem, seed);
      return new DirectArrayOfDoublesUnion(sketch, mem);
    }

    final byte version = mem.getByte(ArrayOfDoublesUnion.SERIAL_VERSION_BYTE);
    if (version != ArrayOfDoublesUnion.serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + ArrayOfDoublesUnion.serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateFamily(mem.getByte(ArrayOfDoublesUnion.FAMILY_ID_BYTE), mem.getByte(ArrayOfDoublesUnion.PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(ArrayOfDoublesUnion.SKETCH_TYPE_BYTE), SerializerDeserializer.SketchType.ArrayOfDoublesUnion);

    final WritableMemory sketchMem = mem.writableRegion(ArrayOfDoublesUnion.PREAMBLE_SIZE_BYTES, mem.getCapacity() - ArrayOfDoublesUnion.PREAMBLE_SIZE_BYTES);
    final ArrayOfDoublesQuickSelectSketch sketch = isWritable ? new DirectArrayOfDoublesQuickSelectSketch(sketchMem, seed) : new DirectArrayOfDoublesQuickSelectSketchR(sketchMem, seed);
    final ArrayOfDoublesUnion union = isWritable ? new DirectArrayOfDoublesUnion(sketch, mem) : new DirectArrayOfDoublesUnionR(sketch, mem);
    final long unionTheta = mem.getLong(ArrayOfDoublesUnion.THETA_LONG);
    union.setThetaLong(unionTheta);
    return union;
  }

}
