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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.memory.Memory;

/**
 * An extension of QuickSelectSketch&lt;S&gt;, which can be updated with many types of keys.
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
   * This is to create a new instance of an UpdatableQuickSelectSketch.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * or equal to the given value.
   * @param lgResizeFactor log2(resizeFactor) - value from 0 to 3:
   * <pre>
   * 0 - no resizing (max size allocated),
   * 1 - double internal hash table each time it reaches a threshold
   * 2 - grow four times
   * 3 - grow eight times (default)
   * </pre>
   * @param samplingProbability
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   * @param summaryFactory An instance of a SummaryFactory.
   */
  public UpdatableSketch(final int nomEntries, final int lgResizeFactor,
      final float samplingProbability, final SummaryFactory<S> summaryFactory) {
    super(nomEntries, lgResizeFactor, samplingProbability, summaryFactory);
  }

  /**
   * This is to create an instance of a sketch given a serialized form
   * @param srcMem Memory object with data of a serialized UpdatableSketch
   * @param deserializer instance of SummaryDeserializer
   * @param summaryFactory instance of SummaryFactory
   */
  public UpdatableSketch(final Memory srcMem, final SummaryDeserializer<S> deserializer,
      final SummaryFactory<S> summaryFactory) {
    super(srcMem, deserializer, summaryFactory);
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
    if ((key == null) || (key.length == 0)) { return; }
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
    if ((key == null) || (key.length == 0)) { return; }
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
    if ((key == null) || (key.length == 0)) { return; }
    insertOrIgnore(MurmurHash3.hash(key, DEFAULT_UPDATE_SEED)[0] >>> 1, value);
  }

  void insertOrIgnore(final long hash, final U value) {
    setEmpty(false);
    if (hash >= getThetaLong()) { return; }
    int index = findOrInsert(hash);
    if (index < 0) {
      index = ~index;
      insertSummary(index, getSummaryFactory().newSummary());
    }
    summaryTable_[index].update(value);
    rebuildIfNeeded();
  }

}
