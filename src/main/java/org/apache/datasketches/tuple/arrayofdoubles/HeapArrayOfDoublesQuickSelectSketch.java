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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.checkSeedHashes;
import static org.apache.datasketches.common.Util.computeSeedHash;
import static org.apache.datasketches.common.Util.exactLog2OfLong;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.tuple.SerializerDeserializer;
import org.apache.datasketches.tuple.Util;

/**
 * The on-heap implementation of the tuple QuickSelect sketch of type ArrayOfDoubles.
 */

final class HeapArrayOfDoublesQuickSelectSketch extends ArrayOfDoublesQuickSelectSketch {

  private final int lgNomEntries_;
  private final int lgResizeFactor_;
  private final float samplingProbability_;

  private int count_;
  private long[] keys_;
  private double[] values_;

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling
   * probability
   * @param nomEntries Nominal number of entries. Forced to the smallest power of 2 greater than
   * or equal to the given value.
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
    lgNomEntries_ = exactLog2OfLong(ceilingPowerOf2(nomEntries));
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    final int startingCapacity = Util.getStartingCapacity(nomEntries, lgResizeFactor);
    keys_ = new long[startingCapacity];
    values_ = new double[startingCapacity * numValues];
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    setRebuildThreshold();
  }

  /**
   * This is to create an instance given a serialized form
   * @param seg the source MemorySegment
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesQuickSelectSketch(final MemorySegment seg, final long seed) {
    super(seg.get(JAVA_BYTE, NUM_VALUES_BYTE), seed);
    SerializerDeserializer.validateFamily(seg.get(JAVA_BYTE, FAMILY_ID_BYTE),
        seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(seg.get(JAVA_BYTE, SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch);
    final byte version = seg.get(JAVA_BYTE, SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + serialVersionUID + ", actual: " + version);
    }
    final byte flags = seg.get(JAVA_BYTE, FLAGS_BYTE);
    final boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Byte order mismatch");
    }
    checkSeedHashes(seg.get(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT), computeSeedHash(seed));
    isEmpty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    lgNomEntries_ = seg.get(JAVA_BYTE, LG_NOM_ENTRIES_BYTE);
    thetaLong_ = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
    final int currentCapacity = 1 << seg.get(JAVA_BYTE, LG_CUR_CAPACITY_BYTE);
    lgResizeFactor_ = seg.get(JAVA_BYTE, LG_RESIZE_FACTOR_BYTE);
    samplingProbability_ = seg.get(JAVA_FLOAT_UNALIGNED, SAMPLING_P_FLOAT);
    keys_ = new long[currentCapacity];
    values_ = new double[currentCapacity * numValues_];
    final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    count_ = hasEntries ? seg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT) : 0;
    if (count_ > 0) {
      MemorySegment.copy(seg, JAVA_LONG_UNALIGNED, ENTRIES_START, keys_, 0, currentCapacity);
      final long off = ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * currentCapacity);
      MemorySegment.copy(seg, JAVA_DOUBLE_UNALIGNED, off, values_, 0, currentCapacity * numValues_);

    }
    setRebuildThreshold();
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(currentCapacity);
  }

  @Override
  //converts heap hashTable of double[] to compacted double[][]
  public double[][] getValues() {
    final int numVal = numValues_;
    final int count = getRetainedEntries();
    final double[][] values = new double[count][];
    if (count > 0) {
      int cnt = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (keys_[j] == 0) { continue; }
        values[cnt++] = Arrays.copyOfRange(values_, j * numVal, (j + 1) * numVal);
      }
      assert cnt == count;
    }
    return values;
  }

  @Override
  //converts heap hashTable of double[] to compacted double[]
  double[] getValuesAsOneDimension() {
    final int numVal = numValues_;
    final int count = getRetainedEntries();
    final double[] values = new double[count * numVal];
    if (count > 0) {
      int cnt = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (keys_[j] == 0) { continue; }
        System.arraycopy(values_, j * numVal, values, cnt++ * numVal, numVal);
      }
      assert cnt == count;
    }
    return values;
  }

  @Override
  //converts heap hashTable of long[] to compacted long[]
  long[] getKeys() {
    final int count = getRetainedEntries();
    final long[] keysArr = new long[count];
    if (count > 0) {
      int cnt = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (keys_[j] == 0) { continue; }
        keysArr[cnt++] = keys_[j];
      }
      assert cnt == count;
    }
    return keysArr;
  }

  @Override
  public int getRetainedEntries() {
    return count_;
  }

  @Override
  public int getNominalEntries() {
    return 1 << lgNomEntries_;
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
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    serializeInto(seg);
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

  //  X/Y: X = Byte index for just AoDQuickSelectSketch
  //       Y = Byte index when combined with Union Preamble
  // Long || Start Byte Adr:
  // Adr:
  // First 16 bytes are preamble from AoDUnion
  //      ||  7/23  |  6/22  |  5/21  |  4/20  |  3/19   |  2/18  |   1/17   |    0/16            |
  //  0/2 ||    Seed Hash    | #Dbls  |  Flags | SkType2 | FamID  | SerVer   |  Preamble_Longs    |
  //      || 15/31  | 14/30  | 13/29  | 12/28  | 11/27   | 10/26  |   9/25   |    8/24            |
  //  1/3 ||------------------------------Theta Long----------------------------------------------|
  //      || 23/39  | 22/38  | 21/37  | 20/36  | 19/35   | 18/34  | 17/33    |   16/32            |
  //  2/4 ||        Sampling P Float           |         |  LgRF  |lgCapLongs|  LgNomEntries      |
  //      || 31/47  | 30/46  | 29/45  | 28/44  | 27/43   | 26/42  | 25/41    |   24/40            |
  //  3/5 ||                                   |         Retained Entries Int                     |
  //      ||                                                                 |   32/48            |
  //  4/6 ||                               Keys Array longs * keys[] Length                       |
  //      ||                            Values Array doubles * values[] Length                    |

  @Override
  void serializeInto(final MemorySegment seg) {
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1);
    seg.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    seg.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    seg.set(JAVA_BYTE, SKETCH_TYPE_BYTE,
        (byte) SerializerDeserializer.SketchType.ArrayOfDoublesQuickSelectSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    seg.set(JAVA_BYTE, FLAGS_BYTE, (byte)(
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0)
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count_ > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
    ));
    seg.set(JAVA_BYTE, NUM_VALUES_BYTE, (byte) numValues_);
    seg.set(JAVA_SHORT_UNALIGNED, SEED_HASH_SHORT, computeSeedHash(seed_));
    seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong_);
    seg.set(JAVA_BYTE, LG_NOM_ENTRIES_BYTE, (byte) lgNomEntries_);
    seg.set(JAVA_BYTE, LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(keys_.length));
    seg.set(JAVA_BYTE, LG_RESIZE_FACTOR_BYTE, (byte) lgResizeFactor_);
    seg.set(JAVA_FLOAT_UNALIGNED, SAMPLING_P_FLOAT, samplingProbability_);
    seg.set(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT, count_);
    if (count_ > 0) {
      MemorySegment.copy(keys_, 0, seg, JAVA_LONG_UNALIGNED, ENTRIES_START, keys_.length);
      final long off = ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * keys_.length);
      MemorySegment.copy(values_, 0, seg, JAVA_DOUBLE_UNALIGNED, off, values_.length);
    }
  }

  @Override
  public boolean hasMemorySegment() { return false; }

  @Override
  MemorySegment getMemorySegment() { return null; }

  @Override
  public void reset() {
    isEmpty_ = true;
    count_ = 0;
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    final int startingCapacity = Util.getStartingCapacity(1 << lgNomEntries_, lgResizeFactor_);
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
  protected void setThetaLong(final long thetaLong) {
    thetaLong_ = thetaLong;
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
      if ((oldKeys[i] != 0) && (oldKeys[i] < thetaLong_)) {
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
