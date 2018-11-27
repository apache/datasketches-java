/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.ceilingPowerOf2;

import java.nio.ByteOrder;
import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * The on-heap implementation of the tuple QuickSelect sketch of type ArrayOfDoubles.
 */

final class HeapArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesQuickSelectSketch {

  private final int nomEntries_;
  private final int lgResizeFactor_;
  private final float samplingProbability_;

  private int count_;
  private long[] keys_;
  private double[] values_;

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling
   * probability
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param lgResizeFactor log2(resize factor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param samplingProbability
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param numValues number of double values to keep for each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesQuickSelectSketch(final int nomEntries, final int lgResizeFactor,
      final float samplingProbability, final int numValues, final long seed) {
    super(numValues, seed);
    nomEntries_ = ceilingPowerOf2(nomEntries);
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    final int startingCapacity = Util.getStartingCapacity(nomEntries, lgResizeFactor);
    keys_ = new long[startingCapacity];
    values_ = new double[startingCapacity * numValues];
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  /**
   * This is to create an instance given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesQuickSelectSketch(final Memory mem, final long seed) {
    super(mem.getByte(NUM_VALUES_BYTE), seed);
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE),
        mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch);
    final byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + serialVersionUID + ", actual: " + version);
    }
    final byte flags = mem.getByte(FLAGS_BYTE);
    final boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    Util.checkSeedHashes(mem.getShort(SEED_HASH_SHORT), Util.computeSeedHash(seed));
    isEmpty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    nomEntries_ = 1 << mem.getByte(LG_NOM_ENTRIES_BYTE);
    theta_ = mem.getLong(THETA_LONG);
    final int currentCapacity = 1 << mem.getByte(LG_CUR_CAPACITY_BYTE);
    lgResizeFactor_ = mem.getByte(LG_RESIZE_FACTOR_BYTE);
    samplingProbability_ = mem.getFloat(SAMPLING_P_FLOAT);
    keys_ = new long[currentCapacity];
    values_ = new double[currentCapacity * numValues_];
    final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    count_ = hasEntries ? mem.getInt(RETAINED_ENTRIES_INT) : 0;
    if (count_ > 0) {
      mem.getLongArray(ENTRIES_START, keys_, 0, currentCapacity);
      mem.getDoubleArray(ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * currentCapacity), values_, 0,
          currentCapacity * numValues_);
    }
    setRebuildThreshold();
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(currentCapacity);
  }

  @Override
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      int i = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (keys_[j] != 0) {
          values[i++] = Arrays.copyOfRange(values_, j * numValues_, (j + 1) * numValues_);
        }
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
  public float getSamplingProbability() {
    return samplingProbability_;
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(lgResizeFactor_);
  }

  @Override
  public byte[] toByteArray() {
    final byte[] byteArray = new byte[getSerializedSizeBytes()];
    final WritableMemory mem = WritableMemory.wrap(byteArray); // wrap the byte array to use the putX methods
    serializeInto(mem);
    return byteArray;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new HeapArrayOfDoublesSketchIterator(keys_, values_, numValues_);
  }

  @Override
  int getSerializedSizeBytes() {
    return ENTRIES_START + ((SIZE_OF_KEY_BYTES + (SIZE_OF_VALUE_BYTES * numValues_)) * getCurrentCapacity());
  }

  @Override
  void serializeInto(final WritableMemory mem) {
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem.putByte(SKETCH_TYPE_BYTE,
        (byte) SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem.putByte(FLAGS_BYTE, (byte)(
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count_ > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
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
      mem.putDoubleArray(ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * keys_.length), values_, 0,
          values_.length);
    }
  }

  @Override
  public void reset() {
    isEmpty_ = true;
    count_ = 0;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    final int startingCapacity = Util.getStartingCapacity(nomEntries_, lgResizeFactor_);
    keys_ = new long[startingCapacity];
    values_ = new double[startingCapacity * numValues_];
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  @Override
  protected long getKey(final int index) {
    return keys_[index];
  }

  @Override
  protected void incrementCount() {
    count_++;
  }

  @Override
  protected void setValues(final int index, final double[] values) {
    if (numValues_ == 1) {
      values_[index] = values[0];
    } else {
      System.arraycopy(values, 0, values_, index * numValues_, numValues_);
    }
  }

  @Override
  protected void updateValues(final int index, final double[] values) {
    if (numValues_ == 1) {
      values_[index] += values[0];
    } else {
      final int offset = index * numValues_;
      for (int i = 0; i < numValues_; i++) {
        values_[offset + i] += values[i];
      }
    }
  }

  @Override
  protected void setNotEmpty() {
    isEmpty_ = false;
  }

  @Override
  protected boolean isInSamplingMode() {
    return samplingProbability_ < 1f;
  }

  @Override
  protected void setThetaLong(final long theta) {
    theta_ = theta;
  }

  @Override
  protected int getCurrentCapacity() {
    return keys_.length;
  }

  @Override
  protected void rebuild(final int newCapacity) {
    final long[] oldKeys = keys_;
    final double[] oldValues = values_;
    keys_ = new long[newCapacity];
    values_ = new double[newCapacity * numValues_];
    count_ = 0;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newCapacity);
    for (int i = 0; i < oldKeys.length; i++) {
      if ((oldKeys[i] != 0) && (oldKeys[i] < theta_)) {
        insert(oldKeys[i], Arrays.copyOfRange(oldValues, i * numValues_, (i + 1) * numValues_));
      }
    }
    setRebuildThreshold();
  }

  @Override
  protected int insertKey(final long key) {
    return HashOperations.hashInsertOnly(keys_, lgCurrentCapacity_, key);
  }

  @Override
  protected int findOrInsertKey(final long key) {
    return HashOperations.hashSearchOrInsert(keys_, lgCurrentCapacity_, key);
  }

  @Override
  protected double[] find(final long key) {
    final int index = HashOperations.hashSearch(keys_, lgCurrentCapacity_, key);
    if (index == -1) { return null; }
    return Arrays.copyOfRange(values_, index * numValues_, (index + 1) * numValues_);
  }

}
