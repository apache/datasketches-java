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

import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.Util.simpleLog2OfLong;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.HashOperations;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
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
    lgNomEntries_ = simpleLog2OfLong(ceilingPowerOf2(nomEntries));
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
    lgNomEntries_ = mem.getByte(LG_NOM_ENTRIES_BYTE);
    thetaLong_ = mem.getLong(THETA_LONG);
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
    final WritableMemory mem = WritableMemory.writableWrap(byteArray);
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
    mem.putLong(THETA_LONG, thetaLong_);
    mem.putByte(LG_NOM_ENTRIES_BYTE, (byte) lgNomEntries_);
    mem.putByte(LG_CUR_CAPACITY_BYTE, (byte) Integer.numberOfTrailingZeros(keys_.length));
    mem.putByte(LG_RESIZE_FACTOR_BYTE, (byte) lgResizeFactor_);
    mem.putFloat(SAMPLING_P_FLOAT, samplingProbability_);
    mem.putInt(RETAINED_ENTRIES_INT, count_);
    if (count_ > 0) {
      mem.putLongArray(ENTRIES_START, keys_, 0, keys_.length);
      mem.putDoubleArray(ENTRIES_START + ((long) SIZE_OF_KEY_BYTES * keys_.length), values_, 0, values_.length);
    }
  }

  @Override
  public boolean hasMemory() { return false; }

  @Override
  Memory getMemory() { return null; }

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
