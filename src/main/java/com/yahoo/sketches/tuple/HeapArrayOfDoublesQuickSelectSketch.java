/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

/**
 * This is on-heap implementation
 */

import java.nio.ByteOrder;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

public class HeapArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesQuickSelectSketch {

  private final int nomEntries_;
  private final int lgResizeFactor_;
  private final float samplingProbability_;

  private int count_;
  protected long[] keys_;
  protected double[][] values_;

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param numValues number of double values to keep for each key
   */
  public HeapArrayOfDoublesQuickSelectSketch(int nomEntries, int numValues) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, numValues);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor and a given sampling probability.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param numValues number of double values to keep for each key
   */
  public HeapArrayOfDoublesQuickSelectSketch(int nomEntries, float samplingProbability, int numValues) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, samplingProbability, numValues, DEFAULT_UPDATE_SEED);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resize factor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param numValues number of double values to keep for each key
   */
  public HeapArrayOfDoublesQuickSelectSketch(int nomEntries, int lgResizeFactor, int numValues) {
    this(nomEntries, lgResizeFactor, 1f, numValues, DEFAULT_UPDATE_SEED);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling probability
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resize factor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param numValues number of double values to keep for each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  public HeapArrayOfDoublesQuickSelectSketch(int nomEntries, int lgResizeFactor, float samplingProbability, int numValues, long seed) {
    super(numValues, seed);
    nomEntries_ = ceilingPowerOf2(nomEntries);
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    int startingCapacity = 1 << Util.startingSubMultiple(
      Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries) * 2), // target table size is twice the number of nominal entries
      lgResizeFactor,
      Integer.numberOfTrailingZeros(MIN_NOM_ENTRIES)
    );
    keys_ = new long[startingCapacity];
    values_ = new double[startingCapacity][];
    mask_ = startingCapacity - 1;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  /**
   * This is to create an instance with the default update seed given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public HeapArrayOfDoublesQuickSelectSketch(Memory mem) {
    this(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * This is to create an instance given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  public HeapArrayOfDoublesQuickSelectSketch(Memory mem, long seed) {
    super(mem.getByte(NUM_VALUES_BYTE), seed);
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE), SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch);
    byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) throw new RuntimeException("Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    byte flags = mem.getByte(FLAGS_BYTE);
    boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) throw new RuntimeException("Byte order mismatch");
    Util.checkSeedHashes(mem.getShort(SEED_HASH_SHORT), Util.computeSeedHash(seed));
    isEmpty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    nomEntries_ = 1 << mem.getByte(LG_NOM_ENTRIES_BYTE);
    theta_ = mem.getLong(THETA_LONG);
    int currentCapacity = 1 << mem.getByte(LG_CUR_CAPACITY_BYTE);
    lgResizeFactor_ = mem.getByte(LG_RESIZE_FACTOR_BYTE);
    samplingProbability_ = mem.getFloat(SAMPLING_P_FLOAT);
    keys_ = new long[currentCapacity];
    values_ = new double[currentCapacity][];
    mask_ = currentCapacity - 1;
    boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    count_ = hasEntries ? mem.getInt(RETAINED_ENTRIES_INT) : 0;
    if (count_ > 0) {
      mem.getLongArray(ENTRIES_START, keys_, 0, currentCapacity);
      int offset = ENTRIES_START + SIZE_OF_KEY_BYTES * currentCapacity;
      int sizeOfValues = SIZE_OF_VALUE_BYTES * numValues_;
      for (int i = 0; i < currentCapacity; i++) {
        if (keys_[i] != 0) {
          double[] values = new double[numValues_];
          mem.getDoubleArray(offset, values, 0, numValues_);
          values_[i] = values;
        }
        offset += sizeOfValues;
      }
    }
    setRebuildThreshold();
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(currentCapacity);
  }

  @Override
  public double[][] getValues() {
    int count = getRetainedEntries();
    double[][] values = new double[count][];
    if (count > 0) {
      int i = 0;
      for (int j = 0; j < values_.length; j++) {
        if (values_[j] != null) values[i++] = values_[j].clone();
      }
    }
    return values;
  }

  @Override
  public int getRetainedEntries() {
    return count_;
  }

  @Override
  public int getNominalEntries() {
    return nomEntries_;
  }

  @Override
  public byte[] toByteArray() {
    int sizeBytes = ENTRIES_START + (SIZE_OF_KEY_BYTES + SIZE_OF_VALUE_BYTES * numValues_) * getCurrentCapacity();
    byte[] byteArray = new byte[sizeBytes];
    Memory mem = new NativeMemory(byteArray); // wrap the byte array to use the putX methods
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem.putByte(SKETCH_TYPE_BYTE, (byte)SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch.ordinal());
    boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem.putByte(FLAGS_BYTE, (byte)(
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0) |
      (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0) |
      (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0) |
      (count_ > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
    ));
    mem.putByte(NUM_VALUES_BYTE, (byte) numValues_);
    mem.putShort(SEED_HASH_SHORT, Util.computeSeedHash(seed_));
    mem.putLong(THETA_LONG, theta_);
    mem.putByte(LG_NOM_ENTRIES_BYTE, (byte) Integer.numberOfTrailingZeros(nomEntries_));
    mem.putByte(LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(keys_.length));
    mem.putByte(LG_RESIZE_FACTOR_BYTE, (byte) lgResizeFactor_);
    mem.putFloat(SAMPLING_P_FLOAT, samplingProbability_);
    mem.putInt(RETAINED_ENTRIES_INT, count_);
    if (count_ > 0) {
      mem.putLongArray(ENTRIES_START, keys_, 0, keys_.length);
      int offset = ENTRIES_START + SIZE_OF_KEY_BYTES * keys_.length;
      int sizeOfValues = SIZE_OF_VALUE_BYTES * numValues_;
      for (int i = 0; i < values_.length; i++) {
        if (values_[i] != null) {
          mem.putDoubleArray(offset, values_[i], 0, numValues_);
        }
        offset += sizeOfValues;
      }
    }
    return byteArray;
  }

  @Override
  protected long getKey(int index) {
    return keys_[index];
  }

  @Override
  protected void setKey(int index, long key) {
    keys_[index] = key;
  }

  @Override
  protected void incrementCount() {
    count_++;
  }

  @Override
  protected void setValues(int index, double[] values, boolean isCopyRequired) {
    if (isCopyRequired) {
      values_[index] = values.clone();
    } else {
      values_[index] = values;
    }
  }

  @Override
  protected void updateValues(int index, double[] values) {
    for (int i = 0; i < numValues_; i++) values_[index][i] += values[i];
  }

  @Override
  protected void setNotEmpty() {
    isEmpty_ = false;
  }

  @Override
  protected void setIsEmpty(boolean isEmpty) {
    isEmpty_ = isEmpty;
  }

  @Override
  protected boolean isInSamplingMode() {
    return samplingProbability_ < 1f;
  }

  @Override
  protected void setThetaLong(long theta) {
    theta_ = theta;
  }

  @Override
  protected int getResizeFactor() {
    return 1 << lgResizeFactor_;
  }

  @Override
  protected int getCurrentCapacity() {
    return keys_.length;
  }

  @Override
  protected void rebuild(int newCapacity) {
    long[] oldKeys = keys_;
    double[][] oldValues = values_;
    keys_ = new long[newCapacity];
    values_ = new double[newCapacity][];
    count_ = 0;
    mask_ = newCapacity - 1;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newCapacity);
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldKeys[i] != 0 && oldKeys[i] < theta_) insert(oldKeys[i], oldValues[i]);
    }
    setRebuildThreshold();
  }

  @Override
  protected int insertKey(long key) {
    return HashOperations.hashInsertOnly(keys_, lgCurrentCapacity_, key);
  }

  @Override
  protected int findOrInsertKey(long key) {
    return HashOperations.hashSearchOrInsert(keys_, lgCurrentCapacity_, key);
  }

  @Override
  protected double[] find(long key) {
    int index = HashOperations.hashSearch(keys_, lgCurrentCapacity_, key);
    if (index == -1) return null;
    return values_[index].clone();
  }

  @Override
  ArrayOfDoublesSketchIterator iterator() {
    return new HeapArrayOfDoublesSketchIterator(keys_, values_);
  }

}
