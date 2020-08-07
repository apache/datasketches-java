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

package org.apache.datasketches.tuple;

import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.RESIZE_THRESHOLD;
import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.Util.simpleLog2OfLong;

import java.lang.reflect.Array;
import java.nio.ByteOrder;

import org.apache.datasketches.ByteArrayUtil;
import org.apache.datasketches.Family;
import org.apache.datasketches.HashOperations;
import org.apache.datasketches.QuickSelect;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;

/**
 * A generic tuple sketch using the QuickSelect algorithm.
 *
 * @param <S> type of Summary
 */
class QuickSelectSketch<S extends Summary> extends Sketch<S> {
  private static final byte serialVersionUID = 2;

  private enum Flags { IS_BIG_ENDIAN, IS_IN_SAMPLING_MODE, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  static final int DEFAULT_LG_RESIZE_FACTOR = ResizeFactor.X8.lg();
  private final int nomEntries_;
  private int lgCurrentCapacity_;
  private final int lgResizeFactor_;
  private int count_;
  private final SummaryFactory<S> summaryFactory_;
  private final float samplingProbability_;
  private int rebuildThreshold_;
  private long[] hashTable_;
  S[] summaryTable_;

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(
      final int nomEntries,
      final SummaryFactory<S> summaryFactory) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * <pre>
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * </pre>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(
      final int nomEntries,
      final int lgResizeFactor,
      final SummaryFactory<S> summaryFactory) {
    this(nomEntries, lgResizeFactor, 1f, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling
   * probability
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * or equal to the given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * <pre>
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * </pre>
   * @param samplingProbability the given sampling probability
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final SummaryFactory<S> summaryFactory) {
    this(
      nomEntries,
      lgResizeFactor,
      samplingProbability,
      summaryFactory,
      Util.getStartingCapacity(nomEntries, lgResizeFactor)
    );
  }

  QuickSelectSketch(
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final SummaryFactory<S> summaryFactory,
      final int startingSize) {
    nomEntries_ = ceilingPowerOf2(nomEntries);
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    summaryFactory_ = summaryFactory;
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingSize);
    hashTable_ = new long[startingSize];
    summaryTable_ = null; // wait for the first summary to call Array.newInstance()
    setRebuildThreshold();
  }

  /**
   * This is to create an instance of a QuickSelectSketch given a serialized form
   * @param mem Memory object with serialized QukckSelectSketch
   * @param deserializer the SummaryDeserializer
   * @param summaryFactory the SummaryFactory
   */
  QuickSelectSketch(
      final Memory mem,
      final SummaryDeserializer<S> deserializer,
      final SummaryFactory<S> summaryFactory) {
    summaryFactory_ = summaryFactory;
    int offset = 0;
    final byte preambleLongs = mem.getByte(offset++);
    final byte version = mem.getByte(offset++);
    final byte familyId = mem.getByte(offset++);
    SerializerDeserializer.validateFamily(familyId, preambleLongs);
    if (version > serialVersionUID) {
      throw new SketchesArgumentException(
          "Unsupported serial version. Expected: " + serialVersionUID + " or lower, actual: "
              + version);
    }
    SerializerDeserializer.validateType(mem.getByte(offset++),
        SerializerDeserializer.SketchType.QuickSelectSketch);
    final byte flags = mem.getByte(offset++);
    final boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesArgumentException("Endian byte order mismatch");
    }
    nomEntries_ = 1 << mem.getByte(offset++);
    lgCurrentCapacity_ = mem.getByte(offset++);
    lgResizeFactor_ = mem.getByte(offset++);

    final boolean isInSamplingMode = (flags & (1 << Flags.IS_IN_SAMPLING_MODE.ordinal())) > 0;
    samplingProbability_ = isInSamplingMode ? mem.getFloat(offset) : 1f;
    if (isInSamplingMode) {
      offset += Float.BYTES;
    }

    final boolean isThetaIncluded = (flags & (1 << Flags.IS_THETA_INCLUDED.ordinal())) > 0;
    if (isThetaIncluded) {
      thetaLong_ = mem.getLong(offset);
      offset += Long.BYTES;
    } else {
      thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    }

    int count = 0;
    final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    if (hasEntries) {
      count = mem.getInt(offset);
      offset += Integer.BYTES;
    }
    final int currentCapacity = 1 << lgCurrentCapacity_;
    hashTable_ = new long[currentCapacity];
    for (int i = 0; i < count; i++) {
      final long hash = mem.getLong(offset);
      offset += Long.BYTES;
      final Memory memRegion = mem.region(offset, mem.getCapacity() - offset);
      final DeserializeResult<S> summaryResult = deserializer.heapifySummary(memRegion);
      final S summary = summaryResult.getObject();
      offset += summaryResult.getSize();
      insert(hash, summary);
    }
    empty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    setRebuildThreshold();
  }

  long[] getHashTable() {
    return hashTable_;
  }

  @Override
  public int getRetainedEntries() {
    return count_;
  }

  S[] getSummaryTable() {
    return summaryTable_;
  }

  /**
   * Get configured nominal number of entries
   * @return nominal number of entries
   */
  public int getNominalEntries() {
    return nomEntries_;
  }

  /**
   * Get log_base2 of Nominal Entries
   * @return log_base2 of Nominal Entries
   */
  public int getLgK() {
    return simpleLog2OfLong(nomEntries_);
  }

  /**
   * Get configured sampling probability
   * @return sampling probability
   */
  public float getSamplingProbability() {
    return samplingProbability_;
  }

  /**
   * Get current capacity
   * @return current capacity
   */
  public int getCurrentCapacity() {
    return 1 << lgCurrentCapacity_;
  }

  /**
   * Get configured resize factor
   * @return resize factor
   */
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(lgResizeFactor_);
  }

  /**
   * Rebuilds reducing the actual number of entries to the nominal number of entries if needed
   */
  public void trim() {
    if (count_ > nomEntries_) {
      updateTheta();
      resize(hashTable_.length);
    }
  }

  /**
   * Resets this sketch an empty state.
   */
  public void reset() {
    empty_ = true;
    count_ = 0;
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    final int startingCapacity = Util.getStartingCapacity(nomEntries_, lgResizeFactor_);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    hashTable_ = new long[startingCapacity];
    summaryTable_ = null; // wait for the first summary to call Array.newInstance()
    setRebuildThreshold();
  }

  /**
   * Converts the current state of the sketch into a compact sketch
   * @return compact sketch
   */
  @Override
  @SuppressWarnings("unchecked")
  public CompactSketch<S> compact() {
    if (getRetainedEntries() == 0) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }
    final long[] hashArr = new long[getRetainedEntries()];
    final S[] summaryArr = (S[])
      Array.newInstance(summaryTable_.getClass().getComponentType(), getRetainedEntries());
    int i = 0;
    for (int j = 0; j < hashTable_.length; j++) {
      if (summaryTable_[j] != null) {
        hashArr[i] = hashTable_[j];
        summaryArr[i] = (S)summaryTable_[j].copy();
        i++;
      }
    }
    return new CompactSketch<>(hashArr, summaryArr, thetaLong_, empty_);
  }

  // Layout of first 8 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||   RF   |  lgArr | lgNom  |  Flags | SkType | FamID  | SerVer |  Preamble_Longs    |
  @SuppressWarnings("null")
  @Override
  public byte[] toByteArray() {
    byte[][] summariesBytes = null;
    int summariesBytesLength = 0;
    if (count_ > 0) {
      summariesBytes = new byte[count_][];
      int i = 0;
      for (int j = 0; j < summaryTable_.length; j++) {
        if (summaryTable_[j] != null) {
          summariesBytes[i] = summaryTable_[j].toByteArray();
          summariesBytesLength += summariesBytes[i].length;
          i++;
        }
      }
    }
    int sizeBytes =
        Byte.BYTES // preamble longs
      + Byte.BYTES // serial version
      + Byte.BYTES // family
      + Byte.BYTES // sketch type
      + Byte.BYTES // flags
      + Byte.BYTES // log2(nomEntries)
      + Byte.BYTES // log2(currentCapacity)
      + Byte.BYTES; // log2(resizeFactor)
    if (isInSamplingMode()) {
      sizeBytes += Float.BYTES; // samplingProbability
    }
    final boolean isThetaIncluded = isInSamplingMode()
        ? thetaLong_ < samplingProbability_ : thetaLong_ < Long.MAX_VALUE;
    if (isThetaIncluded) {
      sizeBytes += Long.BYTES;
    }
    if (count_ > 0) {
      sizeBytes += Integer.BYTES; // count
    }
    sizeBytes += (Long.BYTES * count_) + summariesBytesLength;
    final byte[] bytes = new byte[sizeBytes];
    int offset = 0;
    bytes[offset++] = PREAMBLE_LONGS;
    bytes[offset++] = serialVersionUID;
    bytes[offset++] = (byte) Family.TUPLE.getID();
    bytes[offset++] = (byte) SerializerDeserializer.SketchType.QuickSelectSketch.ordinal();
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    bytes[offset++] = (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0)
      | (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0)
      | (empty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (count_ > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
      | (isThetaIncluded ? 1 << Flags.IS_THETA_INCLUDED.ordinal() : 0)
    );
    bytes[offset++] = (byte) Integer.numberOfTrailingZeros(nomEntries_);
    bytes[offset++] = (byte) lgCurrentCapacity_;
    bytes[offset++] = (byte) lgResizeFactor_;
    if (samplingProbability_ < 1f) {
      ByteArrayUtil.putFloatLE(bytes, offset, samplingProbability_);
      offset += Float.BYTES;
    }
    if (isThetaIncluded) {
      ByteArrayUtil.putLongLE(bytes, offset, thetaLong_);
      offset += Long.BYTES;
    }
    if (count_ > 0) {
      ByteArrayUtil.putIntLE(bytes, offset, count_);
      offset += Integer.BYTES;
    }
    if (count_ > 0) {
      int i = 0;
      for (int j = 0; j < hashTable_.length; j++) {
        if (summaryTable_[j] != null) {
          ByteArrayUtil.putLongLE(bytes, offset, hashTable_[j]);
          offset += Long.BYTES;
          System.arraycopy(summariesBytes[i], 0, bytes, offset, summariesBytes[i].length);
          offset += summariesBytes[i].length;
          i++;
        }
      }
    }
    return bytes;
  }

  // non-public methods below

  // this is a special back door insert for merging
  // not sufficient by itself without keeping track of theta of another sketch
  @SuppressWarnings("unchecked")
  void merge(final long hash, final S summary, final SummarySetOperations<S> summarySetOps) {
    empty_ = false;
    if ((hash > 0) && (hash < thetaLong_)) {
      final int index = findOrInsert(hash);
      if (index < 0) {
        insertSummary(~index, (S)summary.copy()); //did not find, so insert
      } else {
        insertSummary(index, summarySetOps.union(summaryTable_[index], summary));
      }
      rebuildIfNeeded();
    }
  }

  boolean isInSamplingMode() {
    return samplingProbability_ < 1f;
  }

  void setThetaLong(final long theta) {
    thetaLong_ = theta;
  }

  void setEmpty(final boolean value) {
    empty_ = value;
  }

  SummaryFactory<S> getSummaryFactory() {
    return summaryFactory_;
  }

  int findOrInsert(final long hash) {
    final int index = HashOperations.hashSearchOrInsert(hashTable_, lgCurrentCapacity_, hash);
    if (index < 0) {
      count_++;
    }
    return index;
  }

  boolean rebuildIfNeeded() {
    if (count_ <= rebuildThreshold_) {
      return false;
    }
    if (hashTable_.length > nomEntries_) {
      updateTheta();
      rebuild();
    } else {
      resize(hashTable_.length * (1 << lgResizeFactor_));
    }
    return true;
  }

  void rebuild() {
    resize(hashTable_.length);
  }

  void insert(final long hash, final S summary) {
    final int index = HashOperations.hashInsertOnly(hashTable_, lgCurrentCapacity_, hash);
    insertSummary(index, summary);
    count_++;
    empty_ = false;
  }

  private void updateTheta() {
    final long[] hashArr = new long[count_];
    int i = 0;
    //Because of the association of the hashTable with the summaryTable we cannot destroy the
    // hashTable structure. So we must copy. May as well compact at the same time.
    // Might consider a whole table clone and use the selectExcludingZeros method instead.
    // Not sure if there would be any speed advantage.
    for (int j = 0; j < hashTable_.length; j++) {
      if (summaryTable_[j] != null) {
        hashArr[i++] = hashTable_[j];
      }
    }
    thetaLong_ = QuickSelect.select(hashArr, 0, count_ - 1, nomEntries_);
  }

  @SuppressWarnings({"unchecked"})
  private void resize(final int newSize) {
    final long[] oldHashTable = hashTable_;
    final S[] oldSummaryTable = summaryTable_;
    final Class<S> summaryType = (Class<S>) summaryTable_.getClass().getComponentType();
    hashTable_ = new long[newSize];
    summaryTable_ = (S[]) Array.newInstance(summaryType, newSize);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newSize);
    count_ = 0;
    for (int i = 0; i < oldHashTable.length; i++) {
      if ((oldSummaryTable[i] != null) && (oldHashTable[i] < thetaLong_)) {
        insert(oldHashTable[i], oldSummaryTable[i]);
      }
    }
    setRebuildThreshold();
  }

  private void setRebuildThreshold() {
    if (hashTable_.length > nomEntries_) {
      rebuildThreshold_ = (int) (hashTable_.length * REBUILD_THRESHOLD);
    } else {
      rebuildThreshold_ = (int) (hashTable_.length * RESIZE_THRESHOLD);
    }
  }

  @SuppressWarnings("unchecked")
  protected void insertSummary(final int index, final S summary) {
    if (summaryTable_ == null) {
      summaryTable_ = (S[]) Array.newInstance(summary.getClass(), hashTable_.length);
    }
    summaryTable_[index] = summary;
  }

  @Override
  public SketchIterator<S> iterator() {
    return new SketchIterator<>(hashTable_, summaryTable_);
  }

}
