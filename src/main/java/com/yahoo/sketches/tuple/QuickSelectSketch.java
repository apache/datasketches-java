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
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This is a hash table based implementation of a tuple sketch.
 *
 * @param <S> type of Summary
 */
public class QuickSelectSketch<S extends Summary> extends Sketch<S> {

  public static final byte serialVersionUID = 1;

  static final int MIN_NOM_ENTRIES = 32;
  private static final int DEFAULT_LG_RESIZE_FACTOR = 3;
  private static final double REBUILD_RATIO_AT_RESIZE = 0.5;
  static final double REBUILD_RATIO_AT_TARGET_SIZE = 15.0 / 16.0;
  private int nomEntries_;
  private int lgCurrentCapacity_;
  private int lgResizeFactor_;
  private int count_;
  private final SummaryFactory<S> summaryFactory_;
  private float samplingProbability_;
  private int rebuildThreshold_;

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(int nomEntries, SummaryFactory<S> summaryFactory) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with default resize factor and a given sampling probability.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(int nomEntries, float samplingProbability, SummaryFactory<S> summaryFactory) {
    this(nomEntries, DEFAULT_LG_RESIZE_FACTOR, samplingProbability, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default) 
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(int nomEntries, int lgResizeFactor, SummaryFactory<S> summaryFactory) {
    this(nomEntries, lgResizeFactor, 1f, summaryFactory);
  }

  /**
   * This is to create an instance of a QuickSelectSketch with custom resize factor and sampling probability
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * @param samplingProbability
   * @param summaryFactory An instance of a SummaryFactory.
   */
  QuickSelectSketch(int nomEntries, int lgResizeFactor, float samplingProbability, SummaryFactory<S> summaryFactory) {
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
  QuickSelectSketch(int nomEntries, int lgResizeFactor, float samplingProbability, SummaryFactory<S> summaryFactory, int startingSize) {
    nomEntries_ = ceilingPowerOf2(nomEntries);
    lgResizeFactor_ = lgResizeFactor;
    samplingProbability_ = samplingProbability;
    summaryFactory_ = summaryFactory;
    theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(startingSize);
    keys_ = new long[startingSize];
    summaries_ = (S[]) Array.newInstance(summaryFactory_.newSummary().getClass(), startingSize);
    setRebuildThreshold();
  }

  /**
   * This is to create an instance of a QuickSelectSketch given a serialized form
   * @param mem Memory object with serialized QukckSelectSketch
   */
  @SuppressWarnings("unchecked")
  public QuickSelectSketch(Memory mem) {
    int offset = 0;
    byte preambleLongs = mem.getByte(offset++);
    byte version = mem.getByte(offset++);
    byte familyId = mem.getByte(offset++);
    SerializerDeserializer.validateFamily(familyId, preambleLongs);
    if (version != serialVersionUID) throw new RuntimeException("Serial version mismatch. Expected: " + serialVersionUID + ", actual: " + version);
    SerializerDeserializer.validateType(mem.getByte(offset++), SerializerDeserializer.SketchType.QuickSelectSketch);
    byte flags = mem.getByte(offset++);
    boolean isBigEndian = (flags & (1 << Flags.IS_BIG_ENDIAN.ordinal())) > 0;
    if (isBigEndian ^ ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN)) throw new RuntimeException("Byte order mismatch");
    nomEntries_ = 1 << mem.getByte(offset++);
    lgCurrentCapacity_ = mem.getByte(offset++);
    lgResizeFactor_ = mem.getByte(offset++);

    boolean isInSamplingMode = (flags & (1 << Flags.IS_IN_SAMPLING_MODE.ordinal())) > 0;
    samplingProbability_ = 1f;
    if (isInSamplingMode) {
      samplingProbability_ = mem.getFloat(offset);
      offset += 4;
    }

    boolean isThetaIncluded = (flags & (1 << Flags.IS_THETA_INCLUDED.ordinal())) > 0;
    if (isThetaIncluded) {
      theta_ = mem.getLong(offset);
      offset += 8;
    } else {
      theta_ = (long) (Long.MAX_VALUE * (double) samplingProbability_);
    }

    int count = 0;
    boolean hasEntries = (flags & (1 << Flags.HAS_ENTRIES.ordinal())) > 0;
    if (hasEntries) {
      count = mem.getInt(offset);
      offset += 4;
    }
    DeserializeResult<SummaryFactory<S>> factoryResult = SerializerDeserializer.deserializeFromMemory(mem, offset);
    summaryFactory_ = factoryResult.getObject();
    offset += factoryResult.getSize();
    int currentCapacity = 1 << lgCurrentCapacity_;
    keys_ = new long[currentCapacity];
    summaries_ = (S[]) Array.newInstance(summaryFactory_.newSummary().getClass(), currentCapacity);

    MemoryRegion memRegion = new MemoryRegion(mem, 0, mem.getCapacity());
    for (int i = 0; i < count; i++) {
      long key = mem.getLong(offset);
      offset += 8;
      memRegion.reassign(offset, mem.getCapacity() - offset);
      DeserializeResult<S> summaryResult = summaryFactory_.summaryFromMemory(memRegion);
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
    S[] summaries = (S[]) Array.newInstance(summaryFactory_.newSummary().getClass(), count_);
    int i = 0;
    for (int j = 0; j < summaries_.length; j++) {
      if (summaries_[j] != null) summaries[i++] = summaries_[j].copy();
    }
    return summaries;
  }

  @Override
  public int getRetainedEntries() {
    return count_;
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
   * Converts the current state of the sketch into a compact sketch
   * @return compact sketch
   */
  public CompactSketch<S> compact() {
    long[] keys = new long[getRetainedEntries()];
    @SuppressWarnings("unchecked")
    S[] summaries = (S[]) Array.newInstance(summaries_.getClass().getComponentType(), getRetainedEntries());
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

  private enum Flags { IS_BIG_ENDIAN, IS_IN_SAMPLING_MODE, IS_EMPTY, HAS_ENTRIES, IS_THETA_INCLUDED }

  @Override
  public byte[] toByteArray() {
    byte[] summaryFactoryBytes = SerializerDeserializer.toByteArray(summaryFactory_);
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
        1 // preamble longs
      + 1 // serial version
      + 1 // family
      + 1 // sketch type
      + 1 // flags
      + 1 // log2(nomEntries)
      + 1 // log2(currentCapacity)
      + 1; // log2(resizeFactor)
    if (isInSamplingMode()) sizeBytes += 4; // samplingProbability
    boolean isThetaIncluded = isInSamplingMode() ? theta_ < samplingProbability_ : theta_ < Long.MAX_VALUE;
    if (isThetaIncluded) sizeBytes += 8;
    if (count_ > 0) sizeBytes += 4; // count
    sizeBytes += 8 * count_ + summaryFactoryBytes.length + summariesBytesLength;
    byte[] bytes = new byte[sizeBytes];
    Memory mem = new NativeMemory(bytes);
    int offset = 0;
    mem.putByte(offset++, (byte) 1);
    mem.putByte(offset++, serialVersionUID);
    mem.putByte(offset++, (byte) Family.TUPLE.getID());
    mem.putByte(offset++, (byte) SerializerDeserializer.SketchType.QuickSelectSketch.ordinal());
    boolean isBigEndian = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    mem.putByte(offset++, (byte) (
      (isBigEndian ? 1 << Flags.IS_BIG_ENDIAN.ordinal() : 0) |
      (isInSamplingMode() ? 1 << Flags.IS_IN_SAMPLING_MODE.ordinal() : 0) |
      (isEmpty_ ? 1 << Flags.IS_EMPTY.ordinal() : 0) |
      (count_ > 0 ? 1 << Flags.HAS_ENTRIES.ordinal() : 0) |
      (isThetaIncluded ? 1<< Flags.IS_THETA_INCLUDED.ordinal() : 0)
    ));
    mem.putByte(offset++, (byte) Integer.numberOfTrailingZeros(nomEntries_));
    mem.putByte(offset++, (byte) lgCurrentCapacity_);
    mem.putByte(offset++, (byte) lgResizeFactor_);
    if (samplingProbability_ < 1f) {
      mem.putFloat(offset, samplingProbability_);
      offset += 4;
    }
    if (isThetaIncluded) {
      mem.putLong(offset, theta_);
      offset += 8;
    }
    if (count_ > 0) {
      mem.putInt(offset, count_);
      offset += 4;
    }
    mem.putByteArray(offset, summaryFactoryBytes, 0, summaryFactoryBytes.length);
    offset += summaryFactoryBytes.length;
    if (count_ > 0) {
      int i = 0;
      for (int j = 0; j < keys_.length; j++) {
        if (summaries_[j] != null) {
          mem.putLong(offset, keys_[j]);
          offset += 8;
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
  void merge(long key, S summary) {
    isEmpty_ = false;
    if (key < theta_) {
      int index = findOrInsert(key);
      if (index < 0) {
        summaries_[~index] = summary.copy();
      } else {
        summaries_[index] = summaryFactory_.getSummarySetOperations().union(summaries_[index], summary);
      }
      rebuildIfNeeded();
    }
  }

  boolean isInSamplingMode() {
    return samplingProbability_ < 1f;
  }

  void setThetaLong(long theta) {
    this.theta_ = theta;
  }

  void setIsEmpty(boolean isEmpty) {
    this.isEmpty_ = isEmpty;
  }

  SummaryFactory<S> getSummaryFactory() {
    return summaryFactory_;
  }

  int findOrInsert(long key) {
    int index = HashOperations.hashSearchOrInsert(keys_, lgCurrentCapacity_, key);
    if (index < 0) count_++;
    return index;
  }

  S find(long key) {
    int index = HashOperations.hashSearch(keys_, lgCurrentCapacity_, key);
    if (index == -1) return null;
    return summaries_[index];
  }

  boolean rebuildIfNeeded() {
    if (count_ < rebuildThreshold_) return false;
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

  void insert(long key, S summary) {
    int index = HashOperations.hashInsertOnly(keys_, lgCurrentCapacity_, key);
    summaries_[index] = summary;
    count_++;
  }

  private void updateTheta() {
    long[] keys = new long[count_];
    int i = 0;
    for (int j = 0; j < keys_.length; j++) {
      if (summaries_[j] != null) keys[i++] = keys_[j];
    }
    theta_ = QuickSelect.select(keys, 0, count_ - 1, nomEntries_);
  }

  @SuppressWarnings({"unchecked"})
  private void rebuild(int newSize) {
    long[] oldKeys = keys_;
    S[] oldSummaries = summaries_;
    keys_ = new long[newSize];
    summaries_ = (S[]) Array.newInstance(oldSummaries.getClass().getComponentType(), newSize);
    lgCurrentCapacity_ = Integer.numberOfTrailingZeros(newSize);
    count_ = 0;
    for (int i = 0; i < oldKeys.length; i++) {
      if (oldSummaries[i] != null && oldKeys[i] < theta_) insert(oldKeys[i], oldSummaries[i]);
    }
    setRebuildThreshold();
  }

  private void setRebuildThreshold() {
    if (keys_.length > nomEntries_) {
      rebuildThreshold_ = (int) (keys_.length * REBUILD_RATIO_AT_TARGET_SIZE);
    } else {
      rebuildThreshold_ = (int) (keys_.length * REBUILD_RATIO_AT_RESIZE);
    }
  }

}
