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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.checkBounds;
import static org.apache.datasketches.common.Util.exactLog2OfLong;
import static org.apache.datasketches.thetacommon.HashOperations.count;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Array;
import java.util.Objects;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.QuickSelect;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * A generic tuple sketch using the QuickSelect algorithm.
 *
 * @param <S> type of Summary
 */
class QuickSelectSketch<S extends Summary> extends Sketch<S> {
  private static final byte serialVersionUID = 2;

  private enum Flags { IS_RESERVED, IS_IN_SAMPLING_MODE, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  private static final int DEFAULT_LG_RESIZE_FACTOR = ResizeFactor.X8.lg();
  private final int nomEntries_;
  private final int lgResizeFactor_;
  private final float samplingProbability_;
  private int lgCurrentCapacity_;
  private int retEntries_;
  private int rebuildThreshold_;
  private long[] hashTable_;
  S[] summaryTable_;

  /**
   * This is to create a new instance of a QuickSelectSketch with default resize factor.
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
   * This is to create a new instance of a QuickSelectSketch with custom resize factor
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
   * This is to create a new instance of a QuickSelectSketch with custom resize factor and sampling
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

  /**
   * Target constructor for above constructors for a new instance.
   * @param nomEntries Nominal number of entries.
   * @param lgResizeFactor log2(resizeFactor)
   * @param samplingProbability the given sampling probability
   * @param summaryFactory An instance of a SummaryFactory.
   * @param startingSize starting size of the sketch.
   */
  private QuickSelectSketch(
      final int nomEntries,
      final int lgResizeFactor,
      final float samplingProbability,
      final SummaryFactory<S> summaryFactory,
      final int startingSize) {
      final long thetaLong = (long) (Long.MAX_VALUE * (double) samplingProbability);
    super(
        thetaLong,
        true,
        summaryFactory);
    nomEntries_ = ceilingPowerOf2(nomEntries);
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingSize);
    retEntries_ = 0;
    hashTable_ = new long[startingSize]; //must be before setRebuildThreshold
    rebuildThreshold_ = setRebuildThreshold(hashTable_, nomEntries_);
    summaryTable_ = null; // wait for the first summary to call Array.newInstance()
  }

  /**
   * Copy constructor
   * @param sketch the QuickSelectSketch to be deep copied.
   */
  QuickSelectSketch(final QuickSelectSketch<S> sketch) {
    super(
        sketch.thetaLong_,
        sketch.empty_,
        sketch.summaryFactory_);
    nomEntries_ = sketch.nomEntries_;
    lgResizeFactor_ = sketch.lgResizeFactor_;
    samplingProbability_ = sketch.samplingProbability_;
    lgCurrentCapacity_ = sketch.lgCurrentCapacity_;
    retEntries_ = sketch.retEntries_;
    hashTable_ = sketch.hashTable_.clone();
    rebuildThreshold_ = sketch.rebuildThreshold_;
    summaryTable_ = Util.copySummaryArray(sketch.summaryTable_);
  }

  /**
   * This is to create an instance of a QuickSelectSketch given a serialized form
   * @param seg MemorySegment object with serialized QuickSelectSketch
   * @param deserializer the SummaryDeserializer
   * @param summaryFactory the SummaryFactory
   * @deprecated As of 3.0.0, heapifying an UpdatableSketch is deprecated.
   * This capability will be removed in a future release.
   * Heapifying a CompactSketch is not deprecated.
   */
  @Deprecated
  QuickSelectSketch(
      final MemorySegment seg,
      final SummaryDeserializer<S> deserializer,
      final SummaryFactory<S> summaryFactory) {
    //this(new Validate<>(), seg, deserializer, summaryFactory);
    final Validate<S> val = new Validate<>();
    final long thetaLong = val.validate(seg, deserializer);
    nomEntries_ = val.myNomEntries;
    lgResizeFactor_ = val.myLgResizeFactor;
    samplingProbability_ = val.mySamplingProbability;
    lgCurrentCapacity_ = val.myLgCurrentCapacity;
    retEntries_ = val.myRetEntries;
    rebuildThreshold_ = val.myRebuildThreshold;
    hashTable_ = val.myHashTable;
    summaryTable_ = val.mySummaryTable;
    super(thetaLong, val.myEmpty, summaryFactory);
  }

  private static final class Validate<S> {
    //super fields
    long myThetaLong;
    boolean myEmpty;
    //this fields
    int myNomEntries;
    int myLgResizeFactor;
    float mySamplingProbability;
    int myLgCurrentCapacity;
    int myRetEntries;
    int myRebuildThreshold;
    long[] myHashTable;
    S[] mySummaryTable;

    @SuppressWarnings("unchecked")
    long validate(
        final MemorySegment seg,
        final SummaryDeserializer<?> deserializer) {
      Objects.requireNonNull(seg, "Source MemorySegment must not be null.");
      Objects.requireNonNull(deserializer, "Deserializer must not be null.");
      checkBounds(0, 8, seg.byteSize());

      int offset = 0;
      final byte preambleLongs = seg.get(JAVA_BYTE, offset++); //byte 0 PreLongs
      final byte version = seg.get(JAVA_BYTE, offset++);       //byte 1 SerVer
      final byte familyId = seg.get(JAVA_BYTE, offset++);      //byte 2 FamID
      SerializerDeserializer.validateFamily(familyId, preambleLongs);
      if (version > serialVersionUID) {
        throw new SketchesArgumentException(
            "Unsupported serial version. Expected: " + serialVersionUID + " or lower, actual: "
                + version);
      }
      SerializerDeserializer.validateType(seg.get(JAVA_BYTE, offset++), //byte 3
          SerializerDeserializer.SketchType.QuickSelectSketch);
      final byte flags = seg.get(JAVA_BYTE, offset++); //byte 4
      myNomEntries = 1 << seg.get(JAVA_BYTE, offset++); //byte 5
      myLgCurrentCapacity = seg.get(JAVA_BYTE, offset++); //byte 6
      myLgResizeFactor = seg.get(JAVA_BYTE, offset++); //byte 7

      checkBounds(0, preambleLongs * 8L, seg.byteSize());
      final boolean isInSamplingMode = (flags & (1 << Flags.IS_IN_SAMPLING_MODE.ordinal())) > 0;
      mySamplingProbability = isInSamplingMode ? seg.get(JAVA_FLOAT_UNALIGNED, offset) : 1f; //bytes 8 - 11
      if (isInSamplingMode) {
        offset += Float.BYTES;
      }

      final boolean isThetaIncluded = (flags & (1 << Flags.IS_THETA_INCLUDED.ordinal())) > 0;
      if (isThetaIncluded) {
        myThetaLong = seg.get(JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
      } else {
        myThetaLong = (long) (Long.MAX_VALUE * (double) mySamplingProbability);
      }

      int count = 0;
      final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
      if (hasEntries) {
        count = seg.get(JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;
      }
      final int currentCapacity = 1 << myLgCurrentCapacity;
      myHashTable = new long[currentCapacity];
      for (int i = 0; i < count; i++) {
        final long hash = seg.get(JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        final MemorySegment segRegion = seg.asSlice(offset, seg.byteSize() - offset);
        final DeserializeResult<?> summaryResult = deserializer.heapifySummary(segRegion);
        final S summary = (S) summaryResult.getObject();
        offset += summaryResult.getSize();
        //in-place equivalent to insert(hash, summary):
        final int index = HashOperations.hashInsertOnly(myHashTable, myLgCurrentCapacity, hash);
        if (mySummaryTable == null) {
          mySummaryTable = (S[]) Array.newInstance(summary.getClass(), myHashTable.length);
        }
        mySummaryTable[index] = summary;
        myRetEntries++;
        myEmpty = false;
      }
      myEmpty = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
      myRebuildThreshold = setRebuildThreshold(myHashTable, myNomEntries);
      return myThetaLong;
    }

  } //end class Validate

  /**
   * @return a deep copy of this sketch
   */
  QuickSelectSketch<S> copy() {
    return new QuickSelectSketch<>(this);
  }

  long[] getHashTable() {
    return hashTable_;
  }

  @Override
  public int getRetainedEntries() {
    return retEntries_;
  }

  @Override
  public int getCountLessThanThetaLong(final long thetaLong) {
    return count(hashTable_, thetaLong);
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
    return exactLog2OfLong(nomEntries_);
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
    if (retEntries_ > nomEntries_) {
      updateTheta();
      resize(hashTable_.length);
    }
  }

  /**
   * Resets this sketch an empty state.
   */
  public void reset() {
    empty_ = true;
    retEntries_ = 0;
    thetaLong_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    final int startingCapacity = Util.getStartingCapacity(nomEntries_, lgResizeFactor_);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    hashTable_ = new long[startingCapacity];
    summaryTable_ = null; // wait for the first summary to call Array.newInstance()
    rebuildThreshold_ = setRebuildThreshold(hashTable_, nomEntries_);
  }

  /**
   * Converts the current state of the sketch into a compact sketch
   * @return compact sketch
   */
  @Override
  @SuppressWarnings("unchecked")
  public CompactSketch<S> compact() {
    if (getRetainedEntries() == 0) {
      if (empty_) { return new CompactSketch<>(null, null, Long.MAX_VALUE, true); }
      return new CompactSketch<>(null, null, thetaLong_, false);
    }
    final long[] hashArr = new long[getRetainedEntries()];
    final S[] summaryArr = Util.newSummaryArray(summaryTable_, getRetainedEntries());
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
  /**
   * This serializes an UpdatableSketch (QuickSelectSketch).
   * @return serialized representation of an UpdatableSketch (QuickSelectSketch).
   * @deprecated As of 3.0.0, serializing an UpdatableSketch is deprecated.
   * This capability will be removed in a future release.
   * Serializing a CompactSketch is not deprecated.
   */
  @Deprecated
  @Override
  public byte[] toByteArray() {
    byte[][] summariesBytes = null;
    int summariesBytesLength = 0;
    if (retEntries_ > 0) {
      summariesBytes = new byte[retEntries_][];
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
    if (retEntries_ > 0) {
      sizeBytes += Integer.BYTES; // count
    }
    sizeBytes += (Long.BYTES * retEntries_) + summariesBytesLength;
    final byte[] bytes = new byte[sizeBytes];
    int offset = 0;
    bytes[offset++] = PREAMBLE_LONGS;
    bytes[offset++] = serialVersionUID;
    bytes[offset++] = (byte) Family.TUPLE.getID();
    bytes[offset++] = (byte) SerializerDeserializer.SketchType.QuickSelectSketch.ordinal();
    bytes[offset++] = (byte) (
        (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0)
      | (empty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
      | (retEntries_ > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0)
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
    if (retEntries_ > 0) {
      ByteArrayUtil.putIntLE(bytes, offset, retEntries_);
      offset += Integer.BYTES;
    }
    if (retEntries_ > 0) {
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
        insertSummary(index, summarySetOps.union(summaryTable_[index], (S) summary.copy()));
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

  int findOrInsert(final long hash) {
    final int index = HashOperations.hashSearchOrInsert(hashTable_, lgCurrentCapacity_, hash);
    if (index < 0) {
      retEntries_++;
    }
    return index;
  }

  boolean rebuildIfNeeded() {
    if (retEntries_ <= rebuildThreshold_) {
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
    retEntries_++;
    empty_ = false;
  }

  private void updateTheta() {
    final long[] hashArr = new long[retEntries_];
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
    thetaLong_ = QuickSelect.select(hashArr, 0, retEntries_ - 1, nomEntries_);
  }

  private void resize(final int newSize) {
    final long[] oldHashTable = hashTable_;
    final S[] oldSummaryTable = summaryTable_;
    hashTable_ = new long[newSize];
    summaryTable_ = Util.newSummaryArray(summaryTable_, newSize);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newSize);
    retEntries_ = 0;
    for (int i = 0; i < oldHashTable.length; i++) {
      if ((oldSummaryTable[i] != null) && (oldHashTable[i] < thetaLong_)) {
        insert(oldHashTable[i], oldSummaryTable[i]);
      }
    }
    rebuildThreshold_ = setRebuildThreshold(hashTable_, nomEntries_);
  }

  private static int setRebuildThreshold(final long[] hashTable, final int nomEntries) {
    if (hashTable.length > nomEntries) {
      return (int) (hashTable.length * ThetaUtil.REBUILD_THRESHOLD);
    } else {
      return (int) (hashTable.length * ThetaUtil.RESIZE_THRESHOLD);
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
  public TupleSketchIterator<S> iterator() {
    return new TupleSketchIterator<>(hashTable_, summaryTable_);
  }

}
