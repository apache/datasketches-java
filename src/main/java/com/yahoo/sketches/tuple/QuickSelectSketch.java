/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.Util.RESIZE_THRESHOLD;
import static com.yahoo.sketches.Util.ceilingPowerOf2;

import java.lang.reflect.Array;
import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ByteArrayUtil;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.QuickSelect;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * A generic tuple sketch using the QuickSelect algorithm.
 *
 * @param <S> type of Summary
 */
class QuickSelectSketch<S extends Summary> extends Sketch<S> {
  private static final byte serialVersionWithSummaryFactoryUID = 1;
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

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(final int nomEntries, final SummaryFactory<S> summaryFactory) {
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
  QuickSelectSketch(final int nomEntries, final int lgResizeFactor,
      final SummaryFactory<S> summaryFactory) {
    this(nomEntries, lgResizeFactor, 1f, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling
   * probability
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
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
  QuickSelectSketch(final int nomEntries, final int lgResizeFactor, final float samplingProbability,
      final SummaryFactory<S> summaryFactory) {
    this(
      nomEntries,
      lgResizeFactor,
      samplingProbability,
      summaryFactory,
      Util.getStartingCapacity(nomEntries, lgResizeFactor)
    );
  }

  QuickSelectSketch(final int nomEntries, final int lgResizeFactor, final float samplingProbability,
      final SummaryFactory<S> summaryFactory, final int startingSize) {
    nomEntries_ = ceilingPowerOf2(nomEntries);
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    summaryFactory_ = summaryFactory;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingSize);
    keys_ = new long[startingSize];
    summaries_ = null; // wait for the first summary to call Array.newInstance()
    setRebuildThreshold();
  }

  /**
   * This is to create an instance of a QuickSelectSketch given a serialized form
   * @param mem Memory object with serialized QukckSelectSketch
   * @param deserializer the SummaryDeserializer
   * @param summaryFactory the SummaryFactory
   */
  QuickSelectSketch(final Memory mem, final SummaryDeserializer<S> deserializer,
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
      theta_ = mem.getLong(offset);
      offset += Long.BYTES;
    } else {
      theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    }

    int count = 0;
    final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    if (hasEntries) {
      count = mem.getInt(offset);
      offset += Integer.BYTES;
    }
    if (version == serialVersionWithSummaryFactoryUID) {
      final DeserializeResult<SummaryFactory<S>> factoryResult =
          SerializerDeserializer.deserializeFromMemory(mem, offset);
      offset += factoryResult.getSize();
    }
    final int currentCapacity = 1 << lgCurrentCapacity_;
    keys_ = new long[currentCapacity];
    for (int i = 0; i < count; i++) {
      final long key = mem.getLong(offset);
      offset += Long.BYTES;
      final Memory memRegion = mem.region(offset, mem.getCapacity() - offset);
      final DeserializeResult<S> summaryResult = deserializer.heapifySummary(memRegion);
      final S summary = summaryResult.getObject();
      offset += summaryResult.getSize();
      insert(key, summary);
    }
    isEmpty_ = (flags & (1 << Flags.IS_EMPTY.ordinal())) > 0;
    setRebuildThreshold();
  }

  @Override
  public int getRetainedEntries() {
    return count_;
  }

  /**
   * Get configured nominal number of entries
   * @return nominal number of entries
   */
  public int getNominalEntries() {
    return nomEntries_;
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
      rebuild(keys_.length);
    }
  }

  /**
   * Resets this sketch an empty state.
   */
  public void reset() {
    isEmpty_ = true;
    count_ = 0;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    final int startingCapacity = Util.getStartingCapacity(nomEntries_, lgResizeFactor_);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingCapacity);
    keys_ = new long[startingCapacity];
    summaries_ = null; // wait for the first summary to call Array.newInstance()
    setRebuildThreshold();
  }

  /**
   * Converts the current state of the sketch into a compact sketch
   * @return compact sketch
   */
  public CompactSketch<S> compact() {
    if (getRetainedEntries() == 0) {
      return new CompactSketch<>(null, null, theta_, isEmpty_);
    }
    final long[] keys = new long[getRetainedEntries()];
    @SuppressWarnings("unchecked")
    final S[] summaries = (S[])
      Array.newInstance(summaries_.getClass().getComponentType(), getRetainedEntries());
    int i = 0;
    for (int j = 0; j < keys_.length; j++) {
      if (summaries_[j] != null) {
        keys[i] = keys_[j];
        summaries[i] = summaries_[j].copy();
        i++;
      }
    }
    return new CompactSketch<>(keys, summaries, theta_, isEmpty_);
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
      for (int j = 0; j < summaries_.length; j++) {
        if (summaries_[j] != null) {
          summariesBytes[i] = summaries_[j].toByteArray();
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
        ? theta_ < samplingProbability_ : theta_ < Long.MAX_VALUE;
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
      | (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0)
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
      ByteArrayUtil.putLongLE(bytes, offset, theta_);
      offset += Long.BYTES;
    }
    if (count_ > 0) {
      ByteArrayUtil.putIntLE(bytes, offset, count_);
      offset += Integer.BYTES;
    }
    if (count_ > 0) {
      int i = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (summaries_[j] != null) {
          ByteArrayUtil.putLongLE(bytes, offset, keys_[j]);
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
  void merge(final long key, final S summary, final SummarySetOperations<S> summarySetOps) {
    isEmpty_ = false;
    if (key < theta_) {
      final int index = findOrInsert(key);
      if (index < 0) {
        insertSummary(~index, summary.copy());
      } else {
        insertSummary(index, summarySetOps.union(summaries_[index], summary));
      }
      rebuildIfNeeded();
    }
  }

  boolean isInSamplingMode() {
    return samplingProbability_ < 1f;
  }

  void setThetaLong(final long theta) {
    theta_ = theta;
  }

  void setNotEmpty() {
    isEmpty_ = false;
  }

  SummaryFactory<S> getSummaryFactory() {
    return summaryFactory_;
  }

  int findOrInsert(final long key) {
    final int index = HashOperations.hashSearchOrInsert(keys_, lgCurrentCapacity_, key);
    if (index < 0) {
      count_++;
    }
    return index;
  }

  S find(final long key) {
    final int index = HashOperations.hashSearch(keys_, lgCurrentCapacity_, key);
    if (index == -1) { return null; }
    return summaries_[index];
  }

  boolean rebuildIfNeeded() {
    if (count_ < rebuildThreshold_) {
      return false;
    }
    if (keys_.length > nomEntries_) {
      updateTheta();
      rebuild();
    } else {
      rebuild(keys_.length * (1 << lgResizeFactor_));
    }
    return true;
  }

  void rebuild() {
    rebuild(keys_.length);
  }

  void insert(final long key, final S summary) {
    final int index = HashOperations.hashInsertOnly(keys_, lgCurrentCapacity_, key);
    insertSummary(index, summary);
    count_++;
  }

  private void updateTheta() {
    final long[] keys = new long[count_];
    int i = 0;
    for (int j = 0; j < keys_.length; j++) {
      if (summaries_[j] != null) {
        keys[i++] = keys_[j];
      }
    }
    theta_ = QuickSelect.select(keys, 0, count_ - 1, nomEntries_);
  }

  @SuppressWarnings({"unchecked"})
  private void rebuild(final int newSize) {
    final long[] oldKeys = keys_;
    final S[] oldSummaries = summaries_;
    keys_ = new long[newSize];
    summaries_ = (S[]) Array.newInstance(oldSummaries.getClass().getComponentType(), newSize);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newSize);
    count_ = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if ((oldSummaries[i] != null) && (oldKeys[i] < theta_)) {
        insert(oldKeys[i], oldSummaries[i]);
      }
    }
    setRebuildThreshold();
  }

  private void setRebuildThreshold() {
    if (keys_.length > nomEntries_) {
      rebuildThreshold_ = (int) (keys_.length * REBUILD_THRESHOLD);
    } else {
      rebuildThreshold_ = (int) (keys_.length * RESIZE_THRESHOLD);
    }
  }

  @SuppressWarnings("unchecked")
  protected void insertSummary(final int index, final S summary) {
    if (summaries_ == null) {
      summaries_ = (S[]) Array.newInstance(summary.getClass(), keys_.length);
    }
    summaries_[index] = summary;
  }

}
