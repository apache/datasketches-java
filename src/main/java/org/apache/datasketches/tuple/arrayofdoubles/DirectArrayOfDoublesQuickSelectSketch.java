/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple.arrayofdoubles;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.tuple.SerializerDeserializer;
import org.apache.datasketches.tuple.Util;

/**
 * Direct QuickSelect tuple sketch of type ArrayOfDoubles.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
class DirectArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesQuickSelectSketch {

  // these values exist only on heap, never serialized
  private WritableMemory mem_;
  // these can be derived from the mem_ contents, but are kept here for performance
  private int keysOffset_;
  private int valuesOffset_;

  /**
   * Construct a new sketch using the given Memory as its backing store.
   *
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param lgResizeFactor log2(resize factor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param samplingProbability
   *  <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectArrayOfDoublesQuickSelectSketch(
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final int numValues,
      final long seed,
      final WritableMemory dstMem) {
    this(checkMemory(nomEntries, lgResizeFactor, numValues, dstMem),
    //SpotBugs CT_CONSTRUCTOR_THROW is false positive.
    //this construction scheme is compliant with SEI CERT Oracle Coding Standard for Java / OBJ11-J
        nomEntries,
        lgResizeFactor,
        samplingProbability,
        numValues,
        seed,
        dstMem);
  }

  private DirectArrayOfDoublesQuickSelectSketch(
      final boolean secure, //required part of Finalizer Attack prevention
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final int numValues,
      final long seed,
      final WritableMemory dstMem) {
    super(numValues, seed);
    mem_ = dstMem;
    final int startingCapacity = Util.getStartingCapacity(nomEntries, lgResizeFactor);
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) 1);
    mem_.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem_.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem_.putByte(SKETCH_TYPE_BYTE, (byte)
        SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem_.putByte(FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (samplingProbability < 1f ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0)
      | (1 << Flags.IS_EMPTY.ordinal())
    ));
    mem_.putByte(NUM_VALUES_BYTE, (byte) numValues);
    mem_.putShort(SEED_HASH_SHORT, Util.computeSeedHash(seed));
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    mem_.putLong(THETA_LONG, thetaLong_);
    mem_.putByte(LG_NOM_ENTRIES_BYTE, (byte) Integer.numberOfTrailingZeros(nomEntries));
    mem_.putByte(LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(startingCapacity));
    mem_.putByte(LG_RESIZE_FACTOR_BYTE, (byte) lgResizeFactor);
    mem_.putFloat(SAMPLING_P_FLOAT, samplingProbability);
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * startingCapacity);
    mem_.clear(keysOffset_, (long) SIZE_OF_KEY_BYTES * startingCapacity); // clear keys only
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  private static final boolean checkMemory(
      final int nomEntries,
      final int lgResizeFactor,
      final int numValues,
      final WritableMemory dstMem) {
    final int startingCapacity = Util.getStartingCapacity(nomEntries, lgResizeFactor);
    checkIfEnoughMemory(dstMem, startingCapacity, numValues);
    return true;
  }

  /**
   * Wraps the given Memory.
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed update seed
   */
  DirectArrayOfDoublesQuickSelectSketch(
      final WritableMemory mem,
      final long seed) {
    this(checkSerVer_Endianness(mem), mem, seed);
    //SpotBugs CT_CONSTRUCTOR_THROW is false positive.
    //this construction scheme is compliant with SEI CERT Oracle Coding Standard for Java / OBJ11-J
  }

  private DirectArrayOfDoublesQuickSelectSketch(
      final boolean secure, //required part of Finalizer Attack prevention
      final WritableMemory mem,
      final long seed) {
    super(mem.getByte(NUM_VALUES_BYTE), seed);
    mem_ = mem;
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE),
        mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem_.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch);

    Util.checkSeedHashes(mem.getShort(SEED_HASH_SHORT), Util.computeSeedHash(seed));
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * getCurrentCapacity());
    // to do: make parent take care of its own parts
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(getCurrentCapacity());
    thetaLong_ = mem_.getLong(THETA_LONG);
    isEmpty_ = (mem_.getByte(FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    setRebuildThreshold();
  }

  private static final boolean checkSerVer_Endianness(final Memory mem) {
    final byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: " + serialVersionUID
          + ", actual: " + version);
    }
    final boolean isBigEndian =
        (mem.getByte(FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    return true;
  }

  @Override
  //converts Memory hashTable of double[] to compacted double[][]
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      long keyOffset = keysOffset_;
      long valuesOffset = valuesOffset_;
      int cnt = 0;
      for (int j = 0; j < getCurrentCapacity(); j++) {
        if (mem_.getLong(keyOffset) != 0) {
          final double[] array = new double[numValues_];
          mem_.getDoubleArray(valuesOffset, array, 0, numValues_);
          values[cnt++] = array;
        }
        keyOffset += SIZE_OF_KEY_BYTES;
        valuesOffset += (long)SIZE_OF_VALUE_BYTES * numValues_;
      }
    }
    return values;
  }

  @Override
  //converts heap hashTable of double[] to compacted double[]
  double[] getValuesAsOneDimension() {
    final int count = getRetainedEntries();
    final double[] values = new double[count * numValues_];
    final int cap = getCurrentCapacity();
    if (count > 0) {
      long keyOffsetBytes = keysOffset_;
      long valuesOffsetBytes = valuesOffset_;
      int cnt = 0;
      for (int j = 0; j < cap; j++) {
        if (mem_.getLong(keyOffsetBytes) != 0) {
          mem_.getDoubleArray(valuesOffsetBytes, values, cnt++ * numValues_, numValues_);
        }
        keyOffsetBytes += SIZE_OF_KEY_BYTES;
        valuesOffsetBytes += (long)SIZE_OF_VALUE_BYTES * numValues_;
      }
      assert cnt == count;
    }
    return values;
  }

  @Override
  //converts heap hashTable of long[] to compacted long[]
  long[] getKeys() {
    final int count = getRetainedEntries();
    final long[] keys = new long[count];
    final int cap = getCurrentCapacity();
    if (count > 0) {
      long keyOffsetBytes = keysOffset_;
      int cnt = 0;
      for (int j = 0; j < cap; j++) {
        final long key;
        if ((key = mem_.getLong(keyOffsetBytes)) != 0) {
          keys[cnt++] = key;
        }
        keyOffsetBytes += SIZE_OF_KEY_BYTES;
      }
      assert cnt == count;
    }
    return keys;
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
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(mem_.getByte(LG_RESIZE_FACTOR_BYTE));
  }

  @Override
  public float getSamplingProbability() {
    return mem_.getFloat(SAMPLING_P_FLOAT);
  }

  @Override
  public byte[] toByteArray() {
    final int sizeBytes = getSerializedSizeBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final WritableMemory mem = WritableMemory.writableWrap(byteArray);
    serializeInto(mem);
    return byteArray;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new DirectArrayOfDoublesSketchIterator(mem_, keysOffset_, getCurrentCapacity(),
        numValues_);
  }

  @Override
  public boolean hasMemory() { return true; }

  @Override
  WritableMemory getMemory() { return mem_; }

  @Override
  int getSerializedSizeBytes() {
    return valuesOffset_ + (SIZE_OF_VALUE_BYTES * numValues_ * getCurrentCapacity());
  }

  @Override
  void serializeInto(final WritableMemory mem) {
    mem_.copyTo(0, mem, 0, mem.getCapacity());
  }

  @Override
  public void reset() {
    if (!isEmpty_) {
      isEmpty_ = true;
      mem_.setBits(FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    }
    final int lgResizeFactor = mem_.getByte(LG_RESIZE_FACTOR_BYTE);
    final float samplingProbability = mem_.getFloat(SAMPLING_P_FLOAT);
    final int startingCapacity = Util.getStartingCapacity(getNominalEntries(), lgResizeFactor);
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    mem_.putLong(THETA_LONG, thetaLong_);
    mem_.putByte(LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(startingCapacity));
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * startingCapacity);
    mem_.clear(keysOffset_, (long) SIZE_OF_KEY_BYTES * startingCapacity); // clear keys only
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  @Override
  protected long getKey(final int index) {
    return mem_.getLong(keysOffset_ + ((long) SIZE_OF_KEY_BYTES * index));
  }

  @Override
  protected void incrementCount() {
    final int count = mem_.getInt(RETAINED_ENTRIES_INT);
    if (count == 0) {
      mem_.setBits(FLAGS_BYTE, (byte) (1 << Flags.HAS_ENTRIES.ordinal()));
    }
    mem_.putInt(RETAINED_ENTRIES_INT, count + 1);
  }

  @Override
  protected final int getCurrentCapacity() {
    return 1 << mem_.getByte(LG_CUR_CAPACITY_BYTE);
  }

  @Override
  protected void setThetaLong(final long thetaLong) {
    thetaLong_ = thetaLong;
    mem_.putLong(THETA_LONG, thetaLong_);
  }

  @Override
  protected void setValues(final int index, final double[] values) {
    long offset = valuesOffset_ + ((long) SIZE_OF_VALUE_BYTES * numValues_ * index);
    for (int i = 0; i < numValues_; i++) {
      mem_.putDouble(offset, values[i]);
      offset += SIZE_OF_VALUE_BYTES;
    }
  }

  @Override
  protected void updateValues(final int index, final double[] values) {
    long offset = valuesOffset_ + ((long) SIZE_OF_VALUE_BYTES * numValues_ * index);
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
  protected boolean isInSamplingMode() {
    return (mem_.getByte(FLAGS_BYTE) & (1 << Flags.IS_IN_SAMPLING_MODE.ordinal())) != 0;
  }

  // rebuild in the same memory
  @Override
  protected void rebuild(final int newCapacity) {
    final int numValues = getNumValues();
    checkIfEnoughMemory(mem_, newCapacity, numValues);
    final int currCapacity = getCurrentCapacity();
    final long[] keys = new long[currCapacity];
    final double[] values = new double[currCapacity * numValues];
    mem_.getLongArray(keysOffset_, keys, 0, currCapacity);
    mem_.getDoubleArray(valuesOffset_, values, 0, currCapacity * numValues);
    mem_.clear(keysOffset_,
        ((long) SIZE_OF_KEY_BYTES * newCapacity) + ((long) SIZE_OF_VALUE_BYTES * newCapacity * numValues));
    mem_.putInt(RETAINED_ENTRIES_INT, 0);
    mem_.putByte(LG_CUR_CAPACITY_BYTE, (byte)Integer.numberOfTrailingZeros(newCapacity));
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * newCapacity);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newCapacity);
    for (int i = 0; i < keys.length; i++) {
      if ((keys[i] != 0) && (keys[i] < thetaLong_)) {
        insert(keys[i], Arrays.copyOfRange(values, i * numValues, (i + 1) * numValues));
      }
    }
    setRebuildThreshold();
  }

  @Override
  protected int insertKey(final long key) {
    return HashOperations.hashInsertOnlyMemory(mem_, lgCurrentCapacity_, key, ENTRIES_START);
  }

  @Override
  protected int findOrInsertKey(final long key) {
    return HashOperations.hashSearchOrInsertMemory(mem_, lgCurrentCapacity_, key, ENTRIES_START);
  }

  @Override
  protected double[] find(final long key) {
    final int index = HashOperations.hashSearchMemory(mem_, lgCurrentCapacity_, key, ENTRIES_START);
    if (index == -1) { return null; }
    final double[] array = new double[numValues_];
    mem_.getDoubleArray(valuesOffset_ + ((long) SIZE_OF_VALUE_BYTES * numValues_ * index),
        array, 0, numValues_);
    return array;
  }

  private static void checkIfEnoughMemory(final Memory mem, final int numEntries, final int numValues) {
    final int sizeNeeded =
        ENTRIES_START + ((SIZE_OF_KEY_BYTES + (SIZE_OF_VALUE_BYTES * numValues)) * numEntries);
    if (sizeNeeded > mem.getCapacity()) {
      throw new SketchesArgumentException("Not enough memory: need "
          + sizeNeeded + " bytes, got " + mem.getCapacity() + " bytes");
    }
  }

}
