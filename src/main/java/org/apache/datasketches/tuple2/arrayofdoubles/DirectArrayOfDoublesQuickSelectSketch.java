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

package org.apache.datasketches.tuple2.arrayofdoubles;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.thetacommon2.ThetaUtil.checkSeedHashes;
import static org.apache.datasketches.thetacommon2.ThetaUtil.computeSeedHash;
import static org.apache.datasketches.common.Util.clear;
import static org.apache.datasketches.common.Util.clearBits;
import static org.apache.datasketches.common.Util.setBits;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon2.HashOperations;
import org.apache.datasketches.tuple2.SerializerDeserializer;
import org.apache.datasketches.tuple2.Util;

/**
 * Direct QuickSelect tuple sketch of type ArrayOfDoubles.
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
class DirectArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesQuickSelectSketch {

  // these values exist only on heap, never serialized
  private MemorySegment seg_;
  // these can be derived from the seg_ contents, but are kept here for performance
  private int keysOffset_;
  private int valuesOffset_;

  /**
   * Construct a new sketch using the given MemorySegment as its backing store.
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
   * @param dstSeg the destination MemorySegment.
   */
  DirectArrayOfDoublesQuickSelectSketch(
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final int numValues,
      final long seed,
      final MemorySegment dstSeg) {
    this(checkMemorySegment(nomEntries, lgResizeFactor, numValues, dstSeg),
    //SpotBugs CT_CONSTRUCTOR_THROW is false positive.
    //this construction scheme is compliant with SEI CERT Oracle Coding Standard for Java / OBJ11-J
        nomEntries,
        lgResizeFactor,
        samplingProbability,
        numValues,
        seed,
        dstSeg);
  }

  private DirectArrayOfDoublesQuickSelectSketch(
      final boolean secure, //required part of Finalizer Attack prevention
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final int numValues,
      final long seed,
      final MemorySegment dstSeg) {
    super(numValues, seed);
    seg_ = dstSeg;
    final int startingCapacity = Util.getStartingCapacity(nomEntries, lgResizeFactor);
    seg_.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1);
    seg_.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    seg_.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    seg_.set(JAVA_BYTE, SKETCH_TYPE_BYTE, (byte)
        SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    seg_.set(JAVA_BYTE, FLAGS_BYTE, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (samplingProbability < 1f ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0)
      | (1 << Flags.IS_EMPTY.ordinal())
    ));
    seg_.set(JAVA_BYTE, NUM_VALUES_BYTE, (byte) numValues);
    seg_.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, computeSeedHash(seed));
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    seg_.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
    seg_.set(JAVA_BYTE, LG_NOM_ENTRIES_BYTE, (byte) Integer.numberOfTrailingZeros(nomEntries));
    seg_.set(JAVA_BYTE, LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(startingCapacity));
    seg_.set(JAVA_BYTE, LG_RESIZE_FACTOR_BYTE, (byte) lgResizeFactor);
    seg_.set(JAVA_FLOAT_UNALIGNED, SAMPLING_P_FLOAT, samplingProbability);
    seg_.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, 0);
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * startingCapacity);
    clear(seg_, keysOffset_, (long) SIZE_OF_KEY_BYTES * startingCapacity); //clear keys only
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  private static final boolean checkMemorySegment(
      final int nomEntries,
      final int lgResizeFactor,
      final int numValues,
      final MemorySegment dstSeg) {
    final int startingCapacity = Util.getStartingCapacity(nomEntries, lgResizeFactor);
    checkMemorySegmentSize(dstSeg, startingCapacity, numValues);
    return true;
  }

  /**
   * Wraps the given MemorySegment.
   * @param seg the given MemorySegment
   * @param seed update seed
   */
  DirectArrayOfDoublesQuickSelectSketch(
      final MemorySegment seg,
      final long seed) {
    this(checkSerVer_Endianness(seg), seg, seed);
    //SpotBugs CT_CONSTRUCTOR_THROW is false positive.
    //this construction scheme is compliant with SEI CERT Oracle Coding Standard for Java / OBJ11-J
  }

  private DirectArrayOfDoublesQuickSelectSketch(
      final boolean secure, //required part of Finalizer Attack prevention
      final MemorySegment seg,
      final long seed) {
    super(seg.get(JAVA_BYTE, NUM_VALUES_BYTE), seed);
    seg_ = seg;
    SerializerDeserializer.validateFamily(seg.get(JAVA_BYTE, FAMILY_ID_BYTE),
        seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(seg_.get(JAVA_BYTE, SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch);

    checkSeedHashes(seg.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT), computeSeedHash(seed));
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * getCurrentCapacity());
    // to do: make parent take care of its own parts
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(getCurrentCapacity());
    thetaLong_ = seg_.get(JAVA_LONG_UNALIGNED, THETA_LONG);
    isEmpty_ = (seg_.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_EMPTY.ordinal())) != 0;
    setRebuildThreshold();
  }

  private static final boolean checkSerVer_Endianness(final MemorySegment seg) {
    final byte version = seg.get(JAVA_BYTE, SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: " + serialVersionUID
          + ", actual: " + version);
    }
    final boolean isBigEndian =
        (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_BIG_ENDIAN.ordinal())) != 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    return true;
  }

  @Override
  //converts MemorySegment hashTable of double[] to compacted double[][]
  public double[][] getValues() {
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      long keyOffset = keysOffset_;
      long valuesOffset = valuesOffset_;
      int cnt = 0;
      for (int j = 0; j < getCurrentCapacity(); j++) {
        if (seg_.get(JAVA_LONG_UNALIGNED, keyOffset) != 0) {
          final double[] array = new double[numValues_];
          MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, valuesOffset, array, 0, numValues_);
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
        if (seg_.get(JAVA_LONG_UNALIGNED, keyOffsetBytes) != 0) {
          MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, valuesOffsetBytes, values, cnt++ * numValues_, numValues_);
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
        if ((key = seg_.get(JAVA_LONG_UNALIGNED, keyOffsetBytes)) != 0) {
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
    return seg_.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
  }

  @Override
  public int getNominalEntries() {
    return 1 << seg_.get(JAVA_BYTE, LG_NOM_ENTRIES_BYTE);
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(seg_.get(JAVA_BYTE, LG_RESIZE_FACTOR_BYTE));
  }

  @Override
  public float getSamplingProbability() {
    return seg_.get(JAVA_FLOAT_UNALIGNED, SAMPLING_P_FLOAT);
  }

  @Override
  public byte[] toByteArray() {
    final int sizeBytes = getSerializedSizeBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    serializeInto(seg);
    return byteArray;
  }

  @Override
  public ArrayOfDoublesSketchIterator iterator() {
    return new DirectArrayOfDoublesSketchIterator(seg_, keysOffset_, getCurrentCapacity(),  numValues_);
  }

  @Override
  public boolean hasMemorySegment() { return true; }

  @Override
  MemorySegment getMemorySegment() { return seg_; }

  @Override
  int getSerializedSizeBytes() {
    return valuesOffset_ + (SIZE_OF_VALUE_BYTES * numValues_ * getCurrentCapacity());
  }

  @Override
  void serializeInto(final MemorySegment seg) {
    MemorySegment.copy(seg_, 0, seg, 0, seg.byteSize());
  }

  @Override
  public void reset() {
    if (!isEmpty_) {
      isEmpty_ = true;
      setBits(seg_, FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));
    }
    final int lgResizeFactor = seg_.get(JAVA_BYTE, LG_RESIZE_FACTOR_BYTE);
    final float samplingProbability = seg_.get(JAVA_FLOAT_UNALIGNED, SAMPLING_P_FLOAT);
    final int startingCapacity = Util.getStartingCapacity(getNominalEntries(), lgResizeFactor);
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    seg_.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
    seg_.set(JAVA_BYTE, LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(startingCapacity));
    seg_.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, 0);
    keysOffset_ = ENTRIES_START;
    valuesOffset_ = keysOffset_ + (SIZE_OF_KEY_BYTES * startingCapacity);
    clear(seg_, keysOffset_, (long) SIZE_OF_KEY_BYTES * startingCapacity); //clear keys only
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  @Override
  protected long getKey(final int index) {
    return seg_.get(JAVA_LONG_UNALIGNED, keysOffset_ + ((long) SIZE_OF_KEY_BYTES * index));
  }

  @Override
  protected void incrementCount() {
    final int count = seg_.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
    if (count == 0) {
      setBits(seg_, FLAGS_BYTE, (byte) (1 << Flags.HAS_ENTRIES.ordinal()));
    }
    seg_.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, count + 1);
  }

  @Override
  protected final int getCurrentCapacity() {
    return 1 << seg_.get(JAVA_BYTE, LG_CUR_CAPACITY_BYTE);
  }

  @Override
  protected void setThetaLong(final long thetaLong) {
    thetaLong_ = thetaLong;
    seg_.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
  }

  @Override
  protected void setValues(final int index, final double[] values) {
    long offset = valuesOffset_ + ((long) SIZE_OF_VALUE_BYTES * numValues_ * index);
    for (int i = 0; i < numValues_; i++) {
      seg_.set(JAVA_DOUBLE_UNALIGNED, offset, values[i]);
      offset += SIZE_OF_VALUE_BYTES;
    }
  }

  @Override
  protected void updateValues(final int index, final double[] values) {
    long offset = valuesOffset_ + ((long) SIZE_OF_VALUE_BYTES * numValues_ * index);
    for (int i = 0; i < numValues_; i++) {
      seg_.set(JAVA_DOUBLE_UNALIGNED, offset, seg_.get(JAVA_DOUBLE_UNALIGNED, offset) + values[i]);
      offset += SIZE_OF_VALUE_BYTES;
    }
  }

  @Override
  protected void setNotEmpty() {
    if (isEmpty_) {
      isEmpty_ = false;
      clearBits(seg_, FLAGS_BYTE, (byte) (1 << Flags.IS_EMPTY.ordinal()));

    }
  }

  @Override
  protected boolean isInSamplingMode() {
    return (seg_.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.IS_IN_SAMPLING_MODE.ordinal())) != 0;
  }

  // rebuild in the same MemorySegment
  @Override
  protected void rebuild(final int newCapacity) {
    final int numValues = getNumValues();
    checkMemorySegmentSize(seg_, newCapacity, numValues);
    final int currCapacity = getCurrentCapacity();
    final long[] keys = new long[currCapacity];
    final double[] values = new double[currCapacity * numValues];
    MemorySegment.copy(seg_, JAVA_LONG_UNALIGNED, keysOffset_, keys, 0, currCapacity);
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, valuesOffset_, values, 0, currCapacity * numValues);

    clear(seg_, keysOffset_, ((long) SIZE_OF_KEY_BYTES * newCapacity) + ((long) SIZE_OF_VALUE_BYTES * newCapacity * numValues));
    seg_.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, 0);
    seg_.set(JAVA_BYTE, LG_CUR_CAPACITY_BYTE, (byte)Integer.numberOfTrailingZeros(newCapacity));
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
    return HashOperations.hashInsertOnlyMemorySegment(seg_, lgCurrentCapacity_, key, ENTRIES_START);
  }

  @Override
  protected int findOrInsertKey(final long key) {
    return HashOperations.hashSearchOrInsertMemorySegment(seg_, lgCurrentCapacity_, key, ENTRIES_START);
  }

  @Override
  protected double[] find(final long key) {
    final int index = HashOperations.hashSearchMemorySegment(seg_, lgCurrentCapacity_, key, ENTRIES_START);
    if (index == -1) { return null; }
    final double[] array = new double[numValues_];
    MemorySegment.copy(seg_, JAVA_DOUBLE_UNALIGNED, valuesOffset_
        + ((long) SIZE_OF_VALUE_BYTES * numValues_ * index), array, 0, numValues_);
    return array;
  }

  private static void checkMemorySegmentSize(final MemorySegment seg, final int numEntries, final int numValues) {
    final int sizeNeeded =
        ENTRIES_START + ((SIZE_OF_KEY_BYTES + (SIZE_OF_VALUE_BYTES * numValues)) * numEntries);
    if (sizeNeeded > seg.byteSize()) {
      throw new SketchesArgumentException("Not enough space: need "
          + sizeNeeded + " bytes, got " + seg.byteSize() + " bytes");
    }
  }

}
