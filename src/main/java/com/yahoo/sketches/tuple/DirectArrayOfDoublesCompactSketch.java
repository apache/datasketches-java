/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import java.nio.ByteOrder;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This implementation keeps the data in a given memory, which is owned by the user.
 * The purpose is to avoid garbage collection.
 */
class DirectArrayOfDoublesCompactSketch extends ArrayOfDoublesCompactSketch {

  // this value exists only on heap, never serialized
  private Memory mem_;

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form.
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param dstMem the given destination Memory.
   */
  DirectArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch, final Memory dstMem) {
    super(sketch.getNumValues());
    mem_ = dstMem;
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem_.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem_.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem_.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    isEmpty_ = sketch.isEmpty();
    int count = sketch.getRetainedEntries();
    mem_.putByte(FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0) |
      (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0) |
      (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal(): 0)
    ));
    mem_.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    mem_.putShort(SEED_HASH_SHORT, Util.computeSeedHash(sketch.getSeed()));
    theta_ = sketch.getThetaLong();
    mem_.putLong(THETA_LONG, theta_);
    if (count > 0) {
      mem_.putInt(RETAINED_ENTRIES_INT, sketch.getRetainedEntries());
      int keyOffset = ENTRIES_START;
      int valuesOffset = keyOffset + SIZE_OF_KEY_BYTES * sketch.getRetainedEntries();
      ArrayOfDoublesSketchIterator it = sketch.iterator();
      while (it.next()) {
        mem_.putLong(keyOffset, it.getKey());
        mem_.putDoubleArray(valuesOffset, it.getValues(), 0, numValues_);
        keyOffset += SIZE_OF_KEY_BYTES;
        valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
  }

  /*
   * Creates an instance from components
   */
  DirectArrayOfDoublesCompactSketch(final long[] keys, final double[] values, final long theta, final boolean isEmpty, final int numValues, final short seedHash, final Memory dstMem) {
    super(numValues);
    mem_ = dstMem;
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem_.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem_.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem_.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    isEmpty_ = isEmpty;
    int count = keys.length;
    mem_.putByte(FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0) |
      (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0) |
      (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal(): 0)
    ));
    mem_.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    mem_.putShort(SEED_HASH_SHORT, seedHash);
    theta_ = theta;
    mem_.putLong(THETA_LONG, theta_);
    if (count > 0) {
      mem_.putInt(RETAINED_ENTRIES_INT, count);
      mem_.putLongArray(ENTRIES_START, keys, 0, count);
      mem_.putDoubleArray(ENTRIES_START + SIZE_OF_KEY_BYTES * count, values, 0, values.length);
    }
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectArrayOfDoublesCompactSketch(final Memory mem) {
    this(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  DirectArrayOfDoublesCompactSketch(final Memory mem, final long seed) {
    super(mem.getByte(NUM_VALUES_BYTE));
    mem_ = mem;
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem_.getByte(SKETCH_TYPE_BYTE), SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    byte version = mem_.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) throw new RuntimeException("Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    boolean isBigEndian = mem.isAllBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_BIG_ENDIAN.ordinal()));
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) throw new RuntimeException("Byte order mismatch");
    Util.checkSeedHashes(mem.getShort(SEED_HASH_SHORT), Util.computeSeedHash(seed));
    isEmpty_ = mem_.isAnyBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    theta_ = mem_.getLong(THETA_LONG);
  }

  @Override
  public int getRetainedEntries() {
    boolean hasEntries = mem_.isAnyBitsSet(FLAGS_BYTE, (byte) (1 << Flags.HAS_ENTRIES.ordinal()));
    return (hasEntries ? mem_.getInt(RETAINED_ENTRIES_INT) : 0);
  }

  @Override
  public double[][] getValues() {
    int count = getRetainedEntries();
    double[][] values = new double[count][];
    if (count > 0) {
      int valuesOffset = ENTRIES_START + SIZE_OF_KEY_BYTES * count;
      for (int i = 0; i < count; i++) {
        double[] array = new double[numValues_];
        mem_.getDoubleArray(valuesOffset, array, 0, numValues_);
        values[i] = array;
        valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
    return values;
  }

  @Override
  public byte[] toByteArray() {
    int count = getRetainedEntries();
    int sizeBytes = EMPTY_SIZE;
    if (count > 0) {
      sizeBytes = ENTRIES_START + SIZE_OF_KEY_BYTES * count + SIZE_OF_VALUE_BYTES * count * numValues_;
    }
    byte[] byteArray = new byte[sizeBytes];
    Memory mem = new NativeMemory(byteArray);
    NativeMemory.copy(mem_, 0, mem, 0, sizeBytes);
    return byteArray;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new DirectArrayOfDoublesSketchIterator(mem_, ENTRIES_START, getRetainedEntries(), numValues_);
  }

  @Override
  short getSeedHash() {
    return mem_.getShort(SEED_HASH_SHORT);
  }

}
