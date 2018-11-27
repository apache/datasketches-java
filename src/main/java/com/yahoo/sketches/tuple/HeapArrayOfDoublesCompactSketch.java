/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.nio.ByteOrder;
import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * The on-heap implementation of tuple Compact Sketch of type ArrayOfDoubles.
 */
final class HeapArrayOfDoublesCompactSketch extends ArrayOfDoublesCompactSketch {

  private final short seedHash_;
  private long[] keys_;
  private double[] values_;

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form.
   * @param sketch the given UpdatableArrayOfDoublesSketch
   */
  HeapArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch) {
    this(sketch, sketch.getThetaLong());
  }

  /**
   * Converts the given UpdatableArrayOfDoublesSketch to this compact form
   * trimming if necessary according to given theta
   * @param sketch the given UpdatableArrayOfDoublesSketch
   * @param theta new value of theta
   */
  HeapArrayOfDoublesCompactSketch(final ArrayOfDoublesUpdatableSketch sketch, final long theta) {
    super(sketch.getNumValues());
    isEmpty_ = sketch.isEmpty();
    theta_ = Math.min(sketch.getThetaLong(), theta);
    seedHash_ = Util.computeSeedHash(sketch.getSeed());
    final int count = sketch.getRetainedEntries();
    if (count > 0) {
      keys_ = new long[count];
      values_ = new double[count * numValues_];
      final ArrayOfDoublesSketchIterator it = sketch.iterator();
      int i = 0;
      while (it.next()) {
        final long key = it.getKey();
        if (key < theta_) {
          keys_[i] = key;
          System.arraycopy(it.getValues(), 0, values_, i * numValues_, numValues_);
          i++;
        }
      }
      // trim if necessary
      if (i < count) {
        if (i == 0) {
          keys_ = null;
          values_ = null;
        } else {
          keys_ = Arrays.copyOf(keys_, i);
          values_ = Arrays.copyOf(values_, i * numValues_);
        }
      }
    }
  }

  /*
   * Creates an instance from components
   */
  HeapArrayOfDoublesCompactSketch(final long[] keys, final double[] values, final long theta,
      final boolean isEmpty, final int numValues, final short seedHash) {
    super(numValues);
    keys_ = keys;
    values_ = values;
    theta_ = theta;
    isEmpty_ = isEmpty;
    seedHash_ = seedHash;
  }

  /**
   * This is to create an instance given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  HeapArrayOfDoublesCompactSketch(final Memory mem) {
    this(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * This is to create an instance given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesCompactSketch(final Memory mem, final long seed) {
    super(mem.getByte(NUM_VALUES_BYTE));
    seedHash_ = mem.getShort(SEED_HASH_SHORT);
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE),
        mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch);
    final byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException(
          "Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    }
    final boolean isBigEndian =
        (mem.getByte(FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    Util.checkSeedHashes(seedHash_, Util.computeSeedHash(seed));
    isEmpty_ = (mem.getByte(FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    theta_ = mem.getLong(THETA_LONG);
    final boolean hasEntries =
        (mem.getByte(FLAGS_BYTE) & (1 << Flags.HAS_ENTRIES.ordinal())) != 0;
    if (hasEntries) {
      final int count = mem.getInt(RETAINED_ENTRIES_INT);
      keys_ = new long[count];
      values_ = new double[count * numValues_];
      mem.getLongArray(ENTRIES_START, keys_, 0, count);
      mem.getDoubleArray(ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * count), values_, 0, values_.length);
    }
  }

  @Override
  public int getRetainedEntries() {
    return keys_ == null ? 0 : keys_.length;
  }

  @Override
  public byte[] toByteArray() {
    final int count = getRetainedEntries();
    int sizeBytes = EMPTY_SIZE;
    if (count > 0) {
      sizeBytes =
          ENTRIES_START + (SIZE_OF_KEY_BYTES * count) + (SIZE_OF_VALUE_BYTES * numValues_ * count);
    }
    final byte[] bytes = new byte[sizeBytes];
    final WritableMemory mem = WritableMemory.wrap(bytes);
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem.putByte(SKETCH_TYPE_BYTE,
        (byte) SerializerDeserializer.SketchType.ArrayOfDoublesCompactSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem.putByte(FLAGS_BYTE, (byte) (
      ((isBigEndian ? 1 : 0) << Flags.IS_BIG_ENDIAN.ordinal())
      | ((isEmpty() ? 1 : 0) << Flags.IS_EMPTY.ordinal())
      | ((count > 0 ? 1 : 0) << Flags.HAS_ENTRIES.ordinal())
    ));
    mem.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    mem.putShort(SEED_HASH_SHORT, seedHash_);
    mem.putLong(THETA_LONG, theta_);
    if (count > 0) {
      mem.putInt(RETAINED_ENTRIES_INT, count);
      mem.putLongArray(ENTRIES_START, keys_, 0, count);
      mem.putDoubleArray(ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * count), values_, 0, values_.length);
    }
    return bytes;
  }

  @Override
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      int i = 0;
      for (int j = 0; j < count; j++) {
        values[i++] = Arrays.copyOfRange(values_, j * numValues_, (j + 1) * numValues_);
      }
    }
    return values;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new HeapArrayOfDoublesSketchIterator(keys_, values_, numValues_);
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

}
