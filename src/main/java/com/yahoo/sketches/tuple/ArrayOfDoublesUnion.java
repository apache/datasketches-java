/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * The base class for unions of tuple sketches of type ArrayOfDoubles.
 */
public abstract class ArrayOfDoublesUnion {

  static final byte serialVersionUID = 1;

  static final int PREAMBLE_SIZE_BYTES = 16;
  // Layout of first 16 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||    Seed Hash    | #Dbls  |  Flags | SkType | FamID  | SerVer |  Preamble_Longs    |
  //      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
  //  1   ||------------------------------Theta Long-------------------------------------------|

  static final int PREAMBLE_LONGS_BYTE = 0; // not used, always 1
  static final int SERIAL_VERSION_BYTE = 1;
  static final int FAMILY_ID_BYTE = 2;
  static final int SKETCH_TYPE_BYTE = 3;
  static final int FLAGS_BYTE = 4;
  static final int NUM_VALUES_BYTE = 5;
  static final int SEED_HASH_SHORT = 6;
  static final int THETA_LONG = 8;

  final int nomEntries_;
  final int numValues_;
  final long seed_;
  final short seedHash_;
  ArrayOfDoublesQuickSelectSketch sketch_;
  long theta_;

  ArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch) {
    nomEntries_ = sketch.getNominalEntries();
    numValues_ = sketch.getNumValues();
    seed_ = sketch.getSeed();
    seedHash_ = Util.computeSeedHash(seed_);
    sketch_ = sketch;
    theta_ = sketch.getThetaLong();
  }

  /**
   * Heapify the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final Memory mem) {
    return heapify(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final Memory mem, final long seed) {
    return HeapArrayOfDoublesUnion.heapifyUnion(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final Memory mem) {
    return wrap(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final Memory mem, final long seed) {
    return wrapUnionImpl((WritableMemory) mem, seed, false);
  }

  /**
   * Wrap the given WritableMemory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final WritableMemory mem) {
    return wrap(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given WritableMemory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final WritableMemory mem, final long seed) {
    return wrapUnionImpl(mem, seed, true);
  }

  /**
   * Updates the union by adding a set of entries from a given sketch
   * @param sketchIn sketch to add to the union
   */
  public void update(final ArrayOfDoublesSketch sketchIn) {
    if (sketchIn == null) { return; }
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    if (sketch_.getNumValues() != sketchIn.getNumValues()) {
      throw new SketchesArgumentException("Incompatible sketches: number of values mismatch "
          + sketch_.getNumValues() + " and " + sketchIn.getNumValues());
    }
    if (sketchIn.isEmpty()) { return; }
    if (sketchIn.getThetaLong() < theta_) { theta_ = sketchIn.getThetaLong(); }
    final ArrayOfDoublesSketchIterator it = sketchIn.iterator();
    while (it.next()) {
      sketch_.merge(it.getKey(), it.getValues());
    }
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @param dstMem memory for the result (can be null)
   * @return compact sketch representing the union (off-heap if memory is provided)
   */
  public ArrayOfDoublesCompactSketch getResult(final WritableMemory dstMem) {
    if (sketch_.getRetainedEntries() > sketch_.getNominalEntries()) {
      theta_ = Math.min(theta_, sketch_.getNewTheta());
    }
    if (dstMem == null) {
      return new HeapArrayOfDoublesCompactSketch(sketch_, theta_);
    }
    return new DirectArrayOfDoublesCompactSketch(sketch_, theta_, dstMem);
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @return on-heap compact sketch representing the union
   */
  public ArrayOfDoublesCompactSketch getResult() {
    return getResult(null);
  }

  /**
   * Resets the union to an empty state
   */
  public abstract void reset();

  /**
   * @return a byte array representation of this object
   */
  public byte[] toByteArray() {
    final int sizeBytes = PREAMBLE_SIZE_BYTES + sketch_.getSerializedSizeBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1); // unused, always 1
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesUnion.ordinal());
    mem.putLong(THETA_LONG, theta_);
    sketch_.serializeInto(mem.writableRegion(PREAMBLE_SIZE_BYTES, mem.getCapacity() - PREAMBLE_SIZE_BYTES));
    return byteArray;
  }

  /**
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param numValues Number of double values to keep for each key
   * @return maximum required storage bytes given nomEntries and numValues
   */
  public static int getMaxBytes(final int nomEntries, final int numValues) {
    return ArrayOfDoublesQuickSelectSketch.getMaxBytes(nomEntries, numValues);
  }

  void setThetaLong(final long theta) {
    theta_ = theta;
  }

  static ArrayOfDoublesUnion wrapUnionImpl(final WritableMemory mem, final long seed,
      final boolean isWritable) {
    final SerializerDeserializer.SketchType type = SerializerDeserializer.getSketchType(mem);
    final ArrayOfDoublesQuickSelectSketch sketch;
    final ArrayOfDoublesUnion union;

    // compatibility with version 0.9.1 and lower
    if (type == SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch) {
      if (isWritable) {
        sketch = new DirectArrayOfDoublesQuickSelectSketch(mem, seed);
        union = new DirectArrayOfDoublesUnion(sketch, mem);
      } else {
        sketch = new DirectArrayOfDoublesQuickSelectSketchR(mem, seed);
        union = new DirectArrayOfDoublesUnionR(sketch, mem);
      }
      return union; //Do not need to set theta_
    }
    //versions > 0.9.1

    //sanity checks
    final byte version = mem.getByte(ArrayOfDoublesUnion.SERIAL_VERSION_BYTE);
    if (version != ArrayOfDoublesUnion.serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + ArrayOfDoublesUnion.serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateFamily(mem.getByte(ArrayOfDoublesUnion.FAMILY_ID_BYTE),
        mem.getByte(ArrayOfDoublesUnion.PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(ArrayOfDoublesUnion.SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesUnion);

    if (isWritable) {
      final WritableMemory sketchMem = mem.writableRegion(ArrayOfDoublesUnion.PREAMBLE_SIZE_BYTES,
          mem.getCapacity() - ArrayOfDoublesUnion.PREAMBLE_SIZE_BYTES);
      sketch = new DirectArrayOfDoublesQuickSelectSketch(sketchMem, seed);
      union = new DirectArrayOfDoublesUnion(sketch, mem);

    } else {
      final Memory sketchMem = mem.region(ArrayOfDoublesUnion.PREAMBLE_SIZE_BYTES,
          mem.getCapacity() - ArrayOfDoublesUnion.PREAMBLE_SIZE_BYTES);
      sketch = new DirectArrayOfDoublesQuickSelectSketchR(sketchMem, seed);
      union = new DirectArrayOfDoublesUnionR(sketch, mem);
    }
    union.theta_ = mem.getLong(ArrayOfDoublesUnion.THETA_LONG);
    return union;
  }

}
