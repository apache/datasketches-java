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
 * @param <U> Type of the value, which is passed to update method of a Summary
 * @param <S> Type of the UpdatableSummary&lt;U&gt;
 */
public class UpdatableSketch<U, S extends UpdatableSummary<U>> extends QuickSelectSketch<S> {

  /**
   * This is to create an instance of an UpdatableQuickSelectSketch.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * <pre>
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * </pre>
   * @param samplingProbability <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  UpdatableSketch(final int nomEntries, final int lgResizeFactor, final float samplingProbability, final SummaryFactory<S> summaryFactory) {
    super(nomEntries, lgResizeFactor, samplingProbability, summaryFactory);
  }

  /**
   * This is to create an instance of a sketch given a serialized form
   * @param mem Memory object with serialized UpdatableQukckSelectSketch
   */
  UpdatableSketch(final Memory mem) {
    super(mem);
  }

  /**
   * Updates this sketch with a long key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given long key
   * @param value The given U value
   */
  public void update(final long key, final U value) {
  update(new long[] {key}, value);
  }

  /**
   * Updates this sketch with a double key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given double key
   * @param value The given U value
   */
  public void update(final double key, final U value) {
    update(Util.doubleToLongArray(key), value);
  }

  /**
   * Updates this sketch with a String key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given String key
   * @param value The given U value
   */
  public void update(final String key, final U value) {
    update(Util.stringToByteArray(key), value);
  }

  /**
   * Updates this sketch with a byte[] key and U value.
   * The value is passed to update() method of the Summary object associated with the key 
   * 
   * @param key The given byte[] key
   * @param value The given U value
   */
  public void update(final byte[] key, final U value) {
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
  public void update(final int[] key, final U value) {
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
  public void update(final long[] key, final U value) {
    if (key == null || key.length == 0) return;
    insertOrIgnore(MurmurHash3.hash(key, DEFAULT_UPDATE_SEED)[0] >>> 1, value);
  }

  private void insertOrIgnore(final long key, final U value) {
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
