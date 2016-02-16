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
 * This is on-heap implementation.
 */
public class HeapArrayOfDoublesCompactSketch extends ArrayOfDoublesCompactSketch {

  private final short seedHash_;
  private long[] keys_;
  private double[][] values_;

  /**
   * This is to create an instance of empty compact sketch 
   * @param numValues number of double values associated with a key (not really needed except for consistency) 
   */
  public HeapArrayOfDoublesCompactSketch(int numValues) {
    super(numValues);
    theta_ = Long.MAX_VALUE;
    seedHash_ = Util.computeSeedHash(DEFAULT_UPDATE_SEED);
  }

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form.
   * @param sketch the given UpdatableArrayOfDoublesSketch
   */
  HeapArrayOfDoublesCompactSketch(UpdatableArrayOfDoublesSketch sketch) {
    super(sketch.getNumValues());
    isEmpty_ = sketch.isEmpty();
    theta_ = sketch.getThetaLong();
    seedHash_ = Util.computeSeedHash(sketch.getSeed());
    int count = sketch.getRetainedEntries();
    if (count > 0) {
      keys_ = new long[count];
      values_ = new double[count][];
      ArrayOfDoublesSketchIterator it = sketch.iterator();
      int i = 0;
      while (it.next()) {
        keys_[i] = it.getKey();
        values_[i] = it.getValues();
        i++;
      }
    }
  }

  /**
   * This is to create an instance given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public HeapArrayOfDoublesCompactSketch(Memory mem) {
    super(mem.getByte(NUM_VALUES_BYTE));
    seedHash_ = mem.getShort(SEED_HASH_SHORT);
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE), SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) throw new RuntimeException("Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    boolean isBigEndian = mem.isAllBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_BIG_ENDIAN.ordinal()));
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) throw new RuntimeException("Byte order mismatch");
    isEmpty_ = mem.isAllBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    theta_ = mem.getLong(THETA_LONG);
    boolean hasEntries = mem.isAllBitsSet(FLAGS_BYTE, (byte) (1 << Flags.HAS_ENTRIES.ordinal()));
    if (hasEntries) {
      int count = mem.getInt(RETAINED_ENTRIES_INT);
      keys_ = new long[count];
      values_ = new double[count][numValues_];
      mem.getLongArray(ENTRIES_START, keys_, 0, count);
      int offset = ENTRIES_START + SIZE_OF_KEY_BYTES * count;
      for (int i = 0; i < count; i++) {
        mem.getDoubleArray(offset, values_[i], 0, numValues_);
        offset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
  }

  @Override
  public int getRetainedEntries() {
    return keys_ == null ? 0 : keys_.length;
  }

  @Override
  public byte[] toByteArray() {
    int count = getRetainedEntries();
    int sizeBytes = EMPTY_SIZE;
    if (count > 0) {
      sizeBytes = ENTRIES_START + SIZE_OF_KEY_BYTES * count + SIZE_OF_VALUE_BYTES * numValues_ * count;
    }
    byte[] bytes = new byte[sizeBytes];
    Memory mem = new NativeMemory(bytes);
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID()); 
    mem.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem.putByte(FLAGS_BYTE, (byte) (
      ((isBigEndian ? 1 : 0) << Flags.IS_BIG_ENDIAN.ordinal()) |
      ((isEmpty() ? 1 : 0) << Flags.IS_EMPTY.ordinal()) |
      ((count > 0 ? 1 : 0) << Flags.HAS_ENTRIES.ordinal())
    ));
    mem.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    mem.putShort(SEED_HASH_SHORT, seedHash_);
    mem.putLong(THETA_LONG, theta_);
    if (count > 0) {
      mem.putInt(RETAINED_ENTRIES_INT, count);
      mem.putLongArray(ENTRIES_START, keys_, 0, count);
      int offset = ENTRIES_START + SIZE_OF_KEY_BYTES * count;
      for (int i = 0; i < count; i++) {
        mem.putDoubleArray(offset, values_[i], 0, numValues_);
        offset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
    return bytes;
  }

  @Override
  public double[][] getValues() {
    int count = getRetainedEntries();
    double[][] values = new double[count][];
    if (count > 0) {
      int i = 0;
      for (int j = 0; j < values_.length; j++) {
        values[i++] = values_[j].clone();
      }
    }
    return values;
  }

  @Override
  ArrayOfDoublesSketchIterator iterator() {
    return new HeapArrayOfDoublesSketchIterator(keys_, values_);
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

}
