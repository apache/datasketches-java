/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import java.nio.ByteOrder;
import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryUtil;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This implementation keeps the data in a given memory.
 * It is generally slower than on-heap implementation, but allows to avoid garbage collection
 */
public class DirectArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesQuickSelectSketch {

  // these values exist only on heap, never serialized
  private Memory mem_;
  // these can be derived from the mem_ contents, but are kept here for performance
  private int keysOffset_;
  private int valuesOffset_;
  
  /**
   * Construct a new sketch using the given Memory as its backing store.
   * 
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param numValues Number of double values to keep for each key.
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesQuickSelectSketch(int nomEntries, int numValues, Memory dstMem) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, 1f, numValues, DEFAULT_UPDATE_SEED, dstMem);
  }

  /**
   * Construct a new sketch using the given Memory as its backing store.
   * 
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param numValues Number of double values to keep for each key.
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesQuickSelectSketch(int nomEntries, float samplingProbability, int numValues, Memory dstMem) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, samplingProbability, numValues, DEFAULT_UPDATE_SEED, dstMem);
  }

  /**
   * Construct a new sketch using the given Memory as its backing store.
   * 
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resize factor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param numValues Number of double values to keep for each key.
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesQuickSelectSketch(int nomEntries, int lgResizeFactor, int numValues, Memory dstMem) {
    this(nomEntries, lgResizeFactor, 1f, numValues, DEFAULT_UPDATE_SEED, dstMem);
  }

  /**
   * Construct a new sketch using the given Memory as its backing store.
   * 
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resize factor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesQuickSelectSketch(int nomEntries, int lgResizeFactor, float samplingProbability, int numValues, long seed, Memory dstMem) {
    super(numValues, seed);
    mem_ = dstMem;
    int startingCapacity = 1 << Util.startingSubMultiple(
      Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries) * 2), // target table size is twice the number of nominal entries
      lgResizeFactor,
      Integer.numberOfTrailingZeros(MIN_NOM_ENTRIES)
    );
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem_.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem_.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem_.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch.ordinal());
    boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem_.putByte(FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0) |
      (samplingProbability < 1f ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0) |
      (1 << Flags.IS_EMPTY.ordinal())
    ));
    mem_.putByte(NUM_VALUES_BYTE, (byte) numValues);
    mem_.putShort(SEED_HASH_SHORT, Util.computeSeedHash(seed));
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    mem_.putLong(THETA_LONG, theta_);
    mem_.putByte(LG_NOM_ENTRIES_BYTE, (byte) Integer.numberOfTrailingZeros(nomEntries));
    mem_.putByte(LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(startingCapacity));
    mem_.putByte(LG_RESIZE_FACTOR_BYTE, (byte) lgResizeFactor);
    mem_.putFloat(SAMPLING_P_FLOAT, samplingProbability);
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + SIZE_OF_KEY_BYTES * startingCapacity;
    mem_.clear(keysOffset_, SIZE_OF_KEY_BYTES * startingCapacity); // clear keys only
    mask_ = startingCapacity - 1;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  /**
   * Wraps the given Memory assuming the default update seed
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public DirectArrayOfDoublesQuickSelectSketch(Memory mem) {
    this(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed update seed
   */
  public DirectArrayOfDoublesQuickSelectSketch(Memory mem, long seed) {
    super(mem.getByte(NUM_VALUES_BYTE), seed);
    mem_ = mem;
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem_.getByte(SKETCH_TYPE_BYTE), SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch);
    byte version = mem_.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) throw new RuntimeException("Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    boolean isBigEndian = mem.isAllBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_BIG_ENDIAN.ordinal()));
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) throw new RuntimeException("Byte order mismatch");
    Util.checkSeedHashes(mem.getShort(SEED_HASH_SHORT), Util.computeSeedHash(seed));
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + SIZE_OF_KEY_BYTES * getCurrentCapacity();
    // to do: make parent take care of its own parts
    mask_ = getCurrentCapacity() - 1;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(getCurrentCapacity());
    theta_ = mem_.getLong(THETA_LONG);
    isEmpty_ = mem_.isAllBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    setRebuildThreshold();
  }

  @Override
  public double[][] getValues() {
    int count = getRetainedEntries();
    double[][] values = new double[count][];
    if (count > 0) {
      long keyOffset = keysOffset_;
      long valuesOffset = valuesOffset_;
      int i = 0;
      for (int j = 0; j < getCurrentCapacity(); j++) {
        if (mem_.getLong(keyOffset) != 0) {
          double[] array = new double[numValues_];
          mem_.getDoubleArray(valuesOffset, array, 0, numValues_);
          values[i++] = array;
        }
        keyOffset += SIZE_OF_KEY_BYTES;
        valuesOffset += SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
    return values;
  }

  @Override
  public int getRetainedEntries() {
    return mem_.getInt(RETAINED_ENTRIES_INT);
  }

  @Override
  public int getNominalEntries() {
    return 1 << mem_.getByte(LG_NOM_ENTRIES_BYTE);
  }

  @Override
  public byte[] toByteArray() {
    int sizeBytes = valuesOffset_ + SIZE_OF_VALUE_BYTES * numValues_ * getCurrentCapacity();
    byte[] byteArray = new byte[sizeBytes];
    Memory mem = new NativeMemory(byteArray);
    MemoryUtil.copy(mem_, 0, mem, 0, sizeBytes);
    return byteArray;
  }

  @Override
  protected long getKey(int index) {
    return mem_.getLong(keysOffset_ + SIZE_OF_KEY_BYTES * index);
  }

  @Override
  protected void setKey(int index, long key) {
    mem_.putLong(keysOffset_ + SIZE_OF_KEY_BYTES * index, key);
  }

  @Override
  protected void incrementCount() {
    int count = mem_.getInt(RETAINED_ENTRIES_INT);
    if (count == 0) mem_.setBits(FLAGS_BYTE, (byte) (1 << Flags.HAS_ENTRIES.ordinal()));
    mem_.putInt(RETAINED_ENTRIES_INT, count + 1);
  }

  @Override
  protected int getCurrentCapacity() {
    return 1 << mem_.getByte(LG_CUR_CAPACITY_BYTE);
  }

  @Override
  protected void setThetaLong(long theta) {
    theta_ = theta;
    mem_.putLong(THETA_LONG, theta_);
  }

  @Override
  protected int getResizeFactor() {
    return 1 << mem_.getByte(LG_RESIZE_FACTOR_BYTE);
  }

  @Override
  // this method copies values regardless of isCopyRequired
  protected void setValues(int index, double[] values, boolean isCopyRequired) {
    long offset = valuesOffset_ + SIZE_OF_VALUE_BYTES * numValues_ * index;
    for (int i = 0; i < numValues_; i++) {
      mem_.putDouble(offset, values[i]);
      offset += SIZE_OF_VALUE_BYTES;
    }
  }

  @Override
  protected void updateValues(int index, double[] values) {
    long offset = valuesOffset_ + SIZE_OF_VALUE_BYTES * numValues_ * index;
    for (int i = 0; i < numValues_; i++) {
      mem_.putDouble(offset, mem_.getDouble(offset) + values[i]);
      offset += SIZE_OF_VALUE_BYTES;
    }
  }

  @Override
  protected void setNotEmpty() {
    if (isEmpty_) {
      isEmpty_ = false;
      mem_.clearBits(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    }
  }

  @Override
  protected void setIsEmpty(boolean isEmpty) {
    if (isEmpty_ && !isEmpty) {
      isEmpty_ = false;
      mem_.clearBits(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    } else if (!isEmpty_ && isEmpty) {
      isEmpty_ = true;
      mem_.setBits(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    }
  }

  @Override
  protected boolean isInSamplingMode() {
    return mem_.isAnyBitsSet(FLAGS_BYTE, (byte) (1 << Flags.IS_IN_SAMPLING_MODE.ordinal()));
  }

  // rebuild in the same memory assuming enough space
  @Override
  protected void rebuild(int newCapacity) {
    int currCapacity = getCurrentCapacity();
    int numValues = getNumValues();
    long[] keys = new long[currCapacity];
    double[] values = new double[currCapacity * numValues];
    mem_.getLongArray(keysOffset_, keys, 0, currCapacity);
    mem_.getDoubleArray(valuesOffset_, values, 0, currCapacity * numValues);
    mem_.clear(keysOffset_, SIZE_OF_KEY_BYTES * newCapacity + SIZE_OF_VALUE_BYTES * newCapacity * numValues);
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    mem_.putByte(LG_CUR_CAPACITY_BYTE, (byte)Integer.numberOfTrailingZeros(newCapacity));
    valuesOffset_ = keysOffset_ + SIZE_OF_KEY_BYTES * newCapacity;
    mask_ = newCapacity - 1;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newCapacity);
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] != 0 && keys[i] < theta_) {
        insert(keys[i], Arrays.copyOfRange(values, i * numValues, (i + 1) * numValues));
      }
    }
    setRebuildThreshold();
  }

  @Override
  protected int insertKey(long key) {
    return HashOperations.hashInsertOnly(mem_, lgCurrentCapacity_, key, ENTRIES_START);
  }

  @Override
  protected int findOrInsertKey(long key) {
    return HashOperations.hashSearchOrInsert(mem_, lgCurrentCapacity_, key, ENTRIES_START);
  }

  @Override
  protected double[] find(long key) {
    int index = HashOperations.hashSearch(mem_, lgCurrentCapacity_, key, ENTRIES_START);
    if (index == -1) return null;
    double[] array = new double[numValues_];
    mem_.getDoubleArray(valuesOffset_ + SIZE_OF_VALUE_BYTES * numValues_ * index, array, 0, numValues_);
    return array;
  }

  @Override
  ArrayOfDoublesSketchIterator iterator() {
    return new DirectArrayOfDoublesSketchIterator(mem_, keysOffset_, getCurrentCapacity(), numValues_);
  }

}
