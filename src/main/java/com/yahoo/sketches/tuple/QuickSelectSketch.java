/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import java.lang.reflect.Array;
import java.nio.ByteOrder;

import static com.yahoo.sketches.Util.ceilingPowerOf2;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.QuickSelect;
import com.yahoo.sketches.SketchesException;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This is a hash table based implementation of a tuple sketch.
 *
 * @param <S> type of Summary
 */
class QuickSelectSketch<S extends Summary> extends Sketch<S> {

  // Layout of first 8 bytes:
  // <pre>
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||   RF   |  lgArr | lgNom  |  Flags | SkType | FamID  | SerVer |  Preamble_Longs    |
  private static final byte PREAMBLE_LONGS = 1;

  static final byte serialVersionUID = 1;

  static final int MIN_NOM_ENTRIES = 32;
  static final int DEFAULT_LG_RESIZE_FACTOR = 3;
  private static final double REBUILD_RATIO_AT_RESIZE = 0.5;
  static final double REBUILD_RATIO_AT_TARGET_SIZE = 15.0 / 16.0;
  private final int nomEntries;
  private int lgCurrentCapacity;
  private final int lgResizeFactor;
  private int count;
  private final SummaryFactory<S> summaryFactory;
  private final float samplingProbability;
  private int rebuildThreshold;

  private enum Flags { IS_BIG_ENDIAN, IS_IN_SAMPLING_MODE, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(final int nomEntries, final SummaryFactory<S> summaryFactory) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * <pre>
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * </pre>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(final int nomEntries, final int lgResizeFactor, final SummaryFactory<S> summaryFactory) {
    this(nomEntries, lgResizeFactor, 1f, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling probability
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
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
  QuickSelectSketch(final int nomEntries, final int lgResizeFactor, final float samplingProbability, final SummaryFactory<S> summaryFactory) {
    this(
      nomEntries,
      lgResizeFactor,
      samplingProbability,
      summaryFactory,
      1 << Util.startingSubMultiple(
        Integer.numberOfTrailingZeros(ceilingPowerOf2(nomEntries) * 2), // target table size is twice the number of nominal entries
        lgResizeFactor,
        Integer.numberOfTrailingZeros(MIN_NOM_ENTRIES)
      )
    );
  }

  @SuppressWarnings("unchecked")
  QuickSelectSketch(final int nomEntries, final int lgResizeFactor, final float samplingProbability, final SummaryFactory<S> summaryFactory, final int startingSize) {
    this.nomEntries = ceilingPowerOf2(nomEntries);
    this.lgResizeFactor = lgResizeFactor;
    this.samplingProbability = samplingProbability;
    this.summaryFactory = summaryFactory;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    lgCurrentCapacity = Integer.numberOfTrailingZeros(startingSize);
    keys_ = new long[startingSize];
    summaries_ = (S[]) Array.newInstance(this.summaryFactory.newSummary().getClass(), startingSize);
    setRebuildThreshold();
  }

  /**
   * This is to create an instance of a QuickSelectSketch given a serialized form
   * @param mem Memory object with serialized QukckSelectSketch
   */
  @SuppressWarnings("unchecked")
  QuickSelectSketch(final Memory mem) {
    int offset = 0;
    final byte preambleLongs = mem.getByte(offset++);
    final byte version = mem.getByte(offset++);
    final byte familyId = mem.getByte(offset++);
    SerializerDeserializer.validateFamily(familyId, preambleLongs);
    if (version != serialVersionUID) {
      throw new SketchesException("Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateType(mem.getByte(offset++), SerializerDeserializer.SketchType.QuickSelectSketch);
    final byte flags = mem.getByte(offset++);
    final boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) {
      throw new SketchesException("Byte order mismatch");
    }
    nomEntries = 1 << mem.getByte(offset++);
    lgCurrentCapacity = mem.getByte(offset++);
    lgResizeFactor = mem.getByte(offset++);

    final boolean isInSamplingMode = (flags & (1 << Flags.IS_IN_SAMPLING_MODE.ordinal())) > 0;
    samplingProbability = isInSamplingMode ? mem.getFloat(offset) : 1f;
    if (isInSamplingMode) {
      offset += Float.BYTES;
    }

    final boolean isThetaIncluded = (flags & (1 << Flags.IS_THETA_INCLUDED.ordinal())) > 0;
    if (isThetaIncluded) {
      theta_ = mem.getLong(offset);
      offset += Long.BYTES;
    } else {
      theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    }

    int count = 0;
    final boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    if (hasEntries) {
      count = mem.getInt(offset);
      offset += Integer.BYTES;
    }
    DeserializeResult<SummaryFactory<S>> factoryResult = SerializerDeserializer.deserializeFromMemory(mem, offset);
    summaryFactory = factoryResult.getObject();
    offset += factoryResult.getSize();
    final int currentCapacity = 1 << lgCurrentCapacity;
    keys_ = new long[currentCapacity];
    summaries_ = (S[]) Array.newInstance(summaryFactory.newSummary().getClass(), currentCapacity);

    MemoryRegion memRegion = new MemoryRegion(mem, 0, mem.getCapacity());
    for (int i = 0; i < count; i++) {
      long key = mem.getLong(offset);
      offset += Long.BYTES;
      memRegion.reassign(offset, mem.getCapacity() - offset);
      DeserializeResult<S> summaryResult = summaryFactory.summaryFromMemory(memRegion);
      S summary = summaryResult.getObject();
      offset += summaryResult.getSize();
      insert(key, summary);
    }
    setIsEmpty((flags & (1 << Flags.IS_EMPTY.ordinal())) > 0);
    setRebuildThreshold();
  }

  @Override
  public S[] getSummaries() {
    @SuppressWarnings("unchecked")
    S[] summaries = (S[]) Array.newInstance(summaryFactory.newSummary().getClass(), count);
    int i = 0;
    for (int j = 0; j < summaries_.length; j++) {
      if (summaries_[j] != null) {
        summaries[i++] = summaries_[j].copy();
      }
    }
    return summaries;
  }

  @Override
  public int getRetainedEntries() {
    return count;
  }

  /**
   * Rebuilds reducing the actual number of entries to the nominal number of entries if needed
   */
  public void trim() {
    if (count > nomEntries) {
      updateTheta();
      rebuild(keys_.length);
    }
  }

  /**
   * Converts the current state of the sketch into a compact sketch
   * @return compact sketch
   */
  public CompactSketch<S> compact() {
    final long[] keys = new long[getRetainedEntries()];
    @SuppressWarnings("unchecked")
    final S[] summaries = (S[]) Array.newInstance(summaries_.getClass().getComponentType(), getRetainedEntries());
    int i = 0;
    for (int j = 0; j < keys_.length; j++) {
      if (summaries_[j] != null) {
        keys[i] = keys_[j];
        summaries[i] = summaries_[j].copy();
        i++;
      }
    }
    return new CompactSketch<S>(keys, summaries, theta_, isEmpty_);
  }

  @SuppressWarnings("null")
  @Override
  public byte[] toByteArray() {
    final byte[] summaryFactoryBytes = SerializerDeserializer.toByteArray(summaryFactory);
    byte[][] summariesBytes = null;
    int summariesBytesLength = 0;
    if (count > 0) {
      summariesBytes = new byte[count][];
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
    final boolean isThetaIncluded = isInSamplingMode() ? theta_ < samplingProbability : theta_ < Long.MAX_VALUE;
    if (isThetaIncluded) {
      sizeBytes += Long.BYTES;
    }
    if (count > 0) {
      sizeBytes += Integer.BYTES; // count
    }
    sizeBytes += Long.BYTES * count + summaryFactoryBytes.length + summariesBytesLength;
    final byte[] bytes = new byte[sizeBytes];
    final Memory mem = new NativeMemory(bytes);
    int offset = 0;
    mem.putByte(offset++, PREAMBLE_LONGS);
    mem.putByte(offset++, serialVersionUID);
    mem.putByte(offset++, (byte) Family.TUPLE.getID());
    mem.putByte(offset++, (byte) SerializerDeserializer.SketchType.QuickSelectSketch.ordinal());
    final boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem.putByte(offset++, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0) |
      (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0) |
      (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0) |
      (count > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0) |
      (isThetaIncluded ? 1<< Flags.IS_THETA_INCLUDED.ordinal() : 0)
    ));
    mem.putByte(offset++, (byte) Integer.numberOfTrailingZeros(nomEntries));
    mem.putByte(offset++, (byte) lgCurrentCapacity);
    mem.putByte(offset++, (byte) lgResizeFactor);
    if (samplingProbability < 1f) {
      mem.putFloat(offset, samplingProbability);
      offset += Float.BYTES;
    }
    if (isThetaIncluded) {
      mem.putLong(offset, theta_);
      offset += Long.BYTES;
    }
    if (count > 0) {
      mem.putInt(offset, count);
      offset += Integer.BYTES;
    }
    mem.putByteArray(offset, summaryFactoryBytes, 0, summaryFactoryBytes.length);
    offset += summaryFactoryBytes.length;
    if (count > 0) {
      int i = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (summaries_[j] != null) {
          mem.putLong(offset, keys_[j]);
          offset += Long.BYTES;
          mem.putByteArray(offset, summariesBytes[i], 0, summariesBytes[i].length);
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
  void merge(final long key, final S summary) {
    isEmpty_ = false;
    if (key < theta_) {
      int index = findOrInsert(key);
      if (index < 0) {
        summaries_[~index] = summary.copy();
      } else {
        summaries_[index] = summaryFactory.getSummarySetOperations().union(summaries_[index], summary);
      }
      rebuildIfNeeded();
    }
  }

  boolean isInSamplingMode() {
    return samplingProbability < 1f;
  }

  void setThetaLong(final long theta) {
    this.theta_ = theta;
  }

  void setIsEmpty(final boolean isEmpty) {
    this.isEmpty_ = isEmpty;
  }

  SummaryFactory<S> getSummaryFactory() {
    return summaryFactory;
  }

  int findOrInsert(final long key) {
    final int index = HashOperations.hashSearchOrInsert(keys_, lgCurrentCapacity, key);
    if (index < 0) {
      count++;
    }
    return index;
  }

  S find(final long key) {
    final int index = HashOperations.hashSearch(keys_, lgCurrentCapacity, key);
    if (index == -1) {
      return null;
    }
    return summaries_[index];
  }

  boolean rebuildIfNeeded() {
    if (count < rebuildThreshold) {
      return false;
    }
    if (keys_.length > nomEntries) {
      updateTheta();
      rebuild();
    } else {
      rebuild(keys_.length * (1 << lgResizeFactor));
    }
    return true;
  }

  void rebuild() {
    rebuild(keys_.length);
  }

  void insert(final long key, final S summary) {
    final int index = HashOperations.hashInsertOnly(keys_, lgCurrentCapacity, key);
    summaries_[index] = summary;
    count++;
  }

  private void updateTheta() {
    final long[] keys = new long[count];
    int i = 0;
    for (int j = 0; j < keys_.length; j++) {
      if (summaries_[j] != null) {
        keys[i++] = keys_[j];
      }
    }
    theta_ = QuickSelect.select(keys, 0, count - 1, nomEntries);
  }

  @SuppressWarnings({"unchecked"})
  private void rebuild(final int newSize) {
    final long[] oldKeys = keys_;
    final S[] oldSummaries = summaries_;
    keys_ = new long[newSize];
    summaries_ = (S[]) Array.newInstance(oldSummaries.getClass().getComponentType(), newSize);
    lgCurrentCapacity = Integer.numberOfTrailingZeros(newSize);
    count = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldSummaries[i] != null && oldKeys[i] < theta_) {
        insert(oldKeys[i], oldSummaries[i]);
      }
    }
    setRebuildThreshold();
  }

  private void setRebuildThreshold() {
    if (keys_.length > nomEntries) {
      rebuildThreshold = (int) (keys_.length * REBUILD_RATIO_AT_TARGET_SIZE);
    } else {
      rebuildThreshold = (int) (keys_.length * REBUILD_RATIO_AT_RESIZE);
    }
  }

}
