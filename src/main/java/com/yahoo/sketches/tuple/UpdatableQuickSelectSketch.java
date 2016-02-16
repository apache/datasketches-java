/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.hash.MurmurHash3;
import com.yahoo.sketches.memory.Memory;

/**
 * This is an extension of QuickSelectSketch, which can be updated with many types of keys.
 * Summary objects are created using a user-defined SummaryFactory class,
 * which should allow very flexible parameterization if needed.
 * Keys are presented to a sketch along with values of a user-defined
 * update type U. When an entry is inserted into a sketch or a duplicate key is
 * presented to a sketch then summary.update(U value) method will be called. So
 * any kind of user-defined accumulation is possible. Summaries also must know
 * how to copy themselves. Also union and intersection of summaries can be
 * implemented in a sub-class of SummarySetOperations, which will be used in
 * case Union or Intersection of two instances of Tuple Sketch is needed
 * @param U type of the value, which is passed to update method of a Summary
 */
public class UpdatableQuickSelectSketch<U, S extends UpdatableSummary<U>> extends QuickSelectSketch<S> {

  /**
   * This is to create an instance of an UpdatableQuickSelectSketch with default resize factor.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param summaryFactory An instance of a SummaryFactory.
   */
  public UpdatableQuickSelectSketch(int nomEntries, SummaryFactory<S> summaryFactory) {
    super(nomEntries, summaryFactory);
  }

  /**
   * This is to create an instance of an UpdatableQuickSelectSketch with default resize factor and a given sampling probability.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  public UpdatableQuickSelectSketch(int nomEntries, float samplingProbability, SummaryFactory<S> summaryFactory) {
    super(nomEntries, samplingProbability, summaryFactory);
  }

  /**
   * This is to create an instance of an UpdatableQuickSelectSketch with custom resize factor
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeRatio log2(resizeRatio) - value from 0 to 3:
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default) 
   * @param summaryFactory An instance of a SummaryFactory.
   */
  public UpdatableQuickSelectSketch(int nomEntries, int lgResizeRatio, SummaryFactory<S> summaryFactory) {
    super(nomEntries, lgResizeRatio, 1f, summaryFactory);
  }

  /**
   * This is to create an instance of an UpdatableQuickSelectSketch with default resize factor and a given sampling probability.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  public UpdatableQuickSelectSketch(int nomEntries, int lgResizeRatio, float samplingProbability, SummaryFactory<S> summaryFactory) {
    super(nomEntries, lgResizeRatio, samplingProbability, summaryFactory);
  }

  /**
   * This is to create an instance of a sketch given a serialized form
   * @param mem Memory object with serialized UpdatableQukckSelectSketch
   */
  public UpdatableQuickSelectSketch(Memory mem) {
    super(mem);
  }

  /**
   * Updates this sketch with a long key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given long key
   * @param value The given U value
   */
  public void update(long key, U value) {
    update(Util.longToLongArray(key), value);
  }

  /**
   * Updates this sketch with a double key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given double key
   * @param value The given U value
   */
  public void update(double key, U value) {
    update(Util.doubleToLongArray(key), value);
  }

  /**
   * Updates this sketch with a String key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given String key
   * @param value The given U value
   */
  public void update(String key, U value) {
    update(Util.stringToByteArray(key), value);
  }

  /**
   * Updates this sketch with a byte[] key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given byte[] key
   * @param value The given U value
   */
  public void update(byte[] key, U value) {
    if (key == null || key.length == 0) return;
    insertOrIgnore(MurmurHash3.hash(key, DEFAULT_UPDATE_SEED)[0] >>> 1, value);
  }

  /**
   * Updates this sketch with a int[] key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given int[] key
   * @param value The given U value
   */
  public void update(int[] key, U value) {
    if (key == null || key.length == 0) return;
    insertOrIgnore(MurmurHash3.hash(key, DEFAULT_UPDATE_SEED)[0] >>> 1, value);
  }

  /**
   * Updates this sketch with a long[] key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given long[] key
   * @param value The given U value
   */
  public void update(long[] key, U value) {
    if (key == null || key.length == 0) return;
    insertOrIgnore(MurmurHash3.hash(key, DEFAULT_UPDATE_SEED)[0] >>> 1, value);
  }

  private void insertOrIgnore(long key, U value) {
    setIsEmpty(false);
    if (key >= getThetaLong()) return;
    int index = findOrInsert(key);
    if (index < 0) {
      index = ~index;
      summaries_[index] = getSummaryFactory().newSummary();
    }
    summaries_[index].update(value);
    rebuildIfNeeded();
  }

}
