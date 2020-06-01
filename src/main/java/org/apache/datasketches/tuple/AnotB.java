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

import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.ceilingPowerOf2;
import static org.apache.datasketches.Util.simpleIntLog2;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.datasketches.HashOperations;
import org.apache.datasketches.theta.HashIterator;

/**
 * Computes a set difference, A-AND-NOT-B, of two generic tuple sketches
 * @param <S> Type of Summary
 */
public final class AnotB<S extends Summary> {
  private boolean empty_ = true;
  private long thetaLong_ = Long.MAX_VALUE;
  private long[] keys_ = null;   //always in compact form, not necessarily sorted
  private S[] summaries_ = null; //always in compact form, not necessarily sorted
  private int count_ = 0;

  /**
   * Sets the given Tuple sketch as the first argument <i>A</i>. This overwrites the internal state of
   * this AnotB operator with the contents of the given sketch. This sets the stage for multiple
   * following <i>notB</i> operations.
   * @param skA The incoming sketch for the first argument, <i>A</i>.
   */
  public void setA(final Sketch<S> skA) {
    if ((skA == null) || skA.isEmpty()) { return; }
    //skA is not empty
    empty_ = false;
    thetaLong_ = skA.getThetaLong();
    final CompactSketch<S> cskA = (skA instanceof CompactSketch)
        ? (CompactSketch<S>)skA
        : ((QuickSelectSketch<S>)skA).compact();
    keys_ = cskA.keys_;
    summaries_ = cskA.summaries_;
    count_ = cskA.getRetainedEntries();
  }

  /**
   * Performs an <i>AND NOT</i> operation with the existing internal state of this AnoB operator.
   * @param skB The incoming Tuple sketch for the second (or following) argument <i>B</i>.
   */
  @SuppressWarnings("unchecked")
  public void notB(final Sketch<S> skB) {
    if (empty_) { return; }
    if ((skB == null) || skB.isEmpty()) { return; }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    thetaLong_ = Math.min(thetaLong_, thetaLongB);
    //Build hashtable and removes keys of skB >= theta
    final int countB = skB.getRetainedEntries();
    final long[] hashTableKeysB = convertToHashTable(skB.keys_, countB, thetaLong_);

    //build temporary arrays of skA
    final long[] tmpKeysA = new long[count_];
    final S[] tmpSummariesA =
        (S[]) Array.newInstance(summaries_.getClass().getComponentType(), count_);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < count_; i++) {
      final long key = keys_[i];
      if ((key != 0) && (key < thetaLong_)) { //skips keys of A >= theta
        final int index =
            HashOperations.hashSearch(hashTableKeysB, simpleIntLog2(hashTableKeysB.length), key);
        if (index == -1) {
          tmpKeysA[nonMatches] = key;
          tmpSummariesA[nonMatches] = summaries_[i];
          nonMatches++;
        }
      }
    }
    keys_ = Arrays.copyOfRange(tmpKeysA, 0, nonMatches);
    summaries_ = Arrays.copyOfRange(tmpSummariesA, 0, nonMatches);
    count_ = nonMatches;
  }

  /**
   * Performs an <i>AND NOT</i> operation with the existing internal state of this AnoB operator.
   * @param skB The incoming Theta sketch for the second (or following) argument <i>B</i>.
   */
  @SuppressWarnings("unchecked")
  public void notB(final org.apache.datasketches.theta.Sketch skB) {
    if (empty_) { return; }
    if ((skB == null) || skB.isEmpty()) { return; }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    thetaLong_ = Math.min(thetaLong_, thetaLongB);
    //Build hashtable and removes keys of skB >= theta
    final int countB = skB.getRetainedEntries();
    final long[] hashTableKeysB =
        convertToHashTable(extractThetaHashArray(skB, countB), countB, thetaLong_);

    //build temporary arrays of skA
    final long[] tmpKeysA = new long[count_];
    final S[] tmpSummariesA =
        (S[]) Array.newInstance(summaries_.getClass().getComponentType(), count_);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < count_; i++) {
      final long key = keys_[i];
      if ((key > 0) && (key < thetaLong_)) { //skips keys of A >= theta
        final int index =
            HashOperations.hashSearch(hashTableKeysB, simpleIntLog2(hashTableKeysB.length), key);
        if (index == -1) {
          tmpKeysA[nonMatches] = key;
          tmpSummariesA[nonMatches] = summaries_[i];
          nonMatches++;
        }
      }
    }
    keys_ = Arrays.copyOfRange(tmpKeysA, 0, nonMatches);
    summaries_ = Arrays.copyOfRange(tmpSummariesA, 0, nonMatches);
    count_ = nonMatches;
  }


  /**
   * Returns the A-and-not-B set operation on the two given sketches.
   * A null sketch argument is interpreted as an empty sketch.
   * This is not an accumulating update.
   *
   * @param skA The incoming sketch for the first argument
   * @param skB The incoming sketch for the second argument
   * @param <S> Type of Summary
   * @return the result as a compact sketch
   */
  @SuppressWarnings("unchecked")
  public static <S extends Summary>
        CompactSketch<S> aNotB(final Sketch<S> skA, final Sketch<S> skB) {
    if ((skA == null) || skA.isEmpty()) {
      return new CompactSketch<>(null, null, Long.MAX_VALUE, true);
    }
    //skA is not empty
    final boolean empty = false;
    final long thetaLongA = skA.getThetaLong();
    final CompactSketch<S> cskA = (skA instanceof CompactSketch)
        ? (CompactSketch<S>)skA
        : ((QuickSelectSketch<S>)skA).compact();
    final long[] keysA = cskA.keys_;
    final S[] summariesA = cskA.summaries_;
    final int countA = cskA.getRetainedEntries();

    if ((skB == null) || skB.isEmpty()) {
      return new CompactSketch<>(keysA, summariesA, thetaLongA, empty);
    }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    final long thetaLong = Math.min(thetaLongA, thetaLongB);
    //Build hashtable and removes keys of skB >= theta
    final int countB = skB.getRetainedEntries();
    final long[] hashTableKeysB =
        convertToHashTable(skB.keys_, countB, thetaLong);

    //build temporary arrays of skA
    final long[] tmpKeysA = new long[countA];
    final S[] tmpSummariesA =
        (S[]) Array.newInstance(summariesA.getClass().getComponentType(), countA);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long key = keysA[i];
      if ((key != 0) && (key < thetaLong)) { //skips keys of A >= theta
        final int index =
            HashOperations.hashSearch(hashTableKeysB, simpleIntLog2(hashTableKeysB.length), key);
        if (index == -1) {
          tmpKeysA[nonMatches] = key;
          tmpSummariesA[nonMatches] = summariesA[i];
          nonMatches++;
        }
      }
    }
    final long[] keys = Arrays.copyOfRange(tmpKeysA, 0, nonMatches);
    final S[] summaries = Arrays.copyOfRange(tmpSummariesA, 0, nonMatches);
    final CompactSketch<S> result =
        new CompactSketch<>(keys, summaries, thetaLong, empty);
    return result;
  }

  /**
   * Returns the A-and-not-B set operation on a Tuple sketch and a Theta sketch.
   * A null sketch argument is interpreted as an empty sketch.
   * This is not an accumulating update.
   *
   * @param skA The incoming Tuple sketch for the first argument
   * @param skB The incoming Theta sketch for the second argument
   * @param <S> Type of Summary
   * @return the result as a compact sketch
   */
  @SuppressWarnings("unchecked")
  public static <S extends Summary>
        CompactSketch<S> aNotB(final Sketch<S> skA, final org.apache.datasketches.theta.Sketch skB) {
    if ((skA == null) || skA.isEmpty()) {
      return new CompactSketch<>(null, null, Long.MAX_VALUE, true);
    }
    //skA is not empty
    final boolean empty = false;
    final long thetaLongA = skA.getThetaLong();
    final CompactSketch<S> cskA = (skA instanceof CompactSketch)
        ? (CompactSketch<S>)skA
        : ((QuickSelectSketch<S>)skA).compact();
    final long[] keysA = cskA.keys_;
    final S[] summariesA = cskA.summaries_;
    final int countA = cskA.getRetainedEntries();

    if ((skB == null) || skB.isEmpty()) {
      return new CompactSketch<>(keysA, summariesA, thetaLongA, empty);
    }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    final long thetaLong = Math.min(thetaLongA, thetaLongB);
    //Build hashtable and removes keys of skB >= theta
    final int countB = skB.getRetainedEntries();
    final long[] hashTableKeysB =
        convertToHashTable(extractThetaHashArray(skB, countB), countB, thetaLong);

    //build temporary arrays of skA
    final long[] tmpKeysA = new long[countA];
    final S[] tmpSummariesA =
        (S[]) Array.newInstance(summariesA.getClass().getComponentType(), countA);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long key = keysA[i];
      if ((key != 0) && (key < thetaLong)) { //skips keys of A >= theta
        final int index =
            HashOperations.hashSearch(hashTableKeysB, simpleIntLog2(hashTableKeysB.length), key);
        if (index == -1) {
          tmpKeysA[nonMatches] = key;
          tmpSummariesA[nonMatches] = summariesA[i];
          nonMatches++;
        }
      }
    }
    final long[] keys = Arrays.copyOfRange(tmpKeysA, 0, nonMatches);
    final S[] summaries = Arrays.copyOfRange(tmpSummariesA, 0, nonMatches);
    final CompactSketch<S> result =
        new CompactSketch<>(keys, summaries, thetaLong, empty);
    return result;
  }

  /**
   * Gets the result of this operation.
   * @param reset if true, clears this operator to the empty state after result is returned.
   * @return the result of this operation as a CompactSketch.
   */
  public CompactSketch<S> getResult(final boolean reset) {
    if (count_ == 0) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }
    final CompactSketch<S> result =
        new CompactSketch<>(Arrays.copyOfRange(keys_, 0, count_),
            Arrays.copyOfRange(summaries_, 0, count_), thetaLong_, empty_);
    if (reset) { reset(); }
    return result;
  }

  private static long[] extractThetaHashArray(
      final org.apache.datasketches.theta.Sketch sketch,
      final int count) {
    final HashIterator itr = sketch.iterator();
    final long[] hashArr = new long[count];
    int ctr = 0;
    while (itr.next()) {
      hashArr[ctr++] = itr.get();
    }
    assert ctr == count;
    return hashArr;
  }

  private static long[] convertToHashTable(final long[] keysArr, final int count, final long thetaLong) {
    final int size = Math.max(
      ceilingPowerOf2((int) Math.ceil(count / REBUILD_THRESHOLD)),
      1 << MIN_LG_NOM_LONGS
    );
    final long[] hashTable = new long[size];
    HashOperations.hashArrayInsert(
        keysArr, hashTable, Integer.numberOfTrailingZeros(size), thetaLong);
    return hashTable;
  }

  private void reset() {
    empty_ = true;
    thetaLong_ = Long.MAX_VALUE;
    keys_ = null;
    summaries_ = null;
    count_ = 0;
  }

  //Deprecated methods

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   * This is not an accumulating update. Calling this update() more than once
   * without calling getResult() will discard the result of previous update() by this method.
   * The result is obtained by calling getResult();
   *
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   * @deprecated After release 2.0.0. Instead please use {@link #aNotB(Sketch, Sketch)}
   * or a combination of {@link #setA(Sketch)} and
   * {@link #notB(Sketch)} with {@link #getResult(boolean)}.
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public void update(final Sketch<S> a, final Sketch<S> b) {
    if (a != null) { empty_ = a.isEmpty(); } //stays this way even if we end up with no result entries
    final long thetaA = a == null ? Long.MAX_VALUE : a.getThetaLong();
    final long thetaB = b == null ? Long.MAX_VALUE : b.getThetaLong();
    thetaLong_ = Math.min(thetaA, thetaB);
    if ((a == null) || (a.getRetainedEntries() == 0)) { return; }
    if ((b == null) || (b.getRetainedEntries() == 0)) {
      loadCompactedArrays(a);
    } else {
      final long[] hashTable;
      if (b instanceof CompactSketch) {
        hashTable = convertToHashTable(b.keys_, b.getRetainedEntries(), thetaLong_);
      } else {
        hashTable = b.keys_;
      }
      final int lgHashTableSize = Integer.numberOfTrailingZeros(hashTable.length);
      final int noMatchSize = a.getRetainedEntries();
      keys_ = new long[noMatchSize];
      summaries_ = (S[]) Array.newInstance(a.summaries_.getClass().getComponentType(), noMatchSize);
      for (int i = 0; i < a.keys_.length; i++) {
        if ((a.keys_[i] != 0) && (a.keys_[i] < thetaLong_)) {
          final int index = HashOperations.hashSearch(hashTable, lgHashTableSize, a.keys_[i]);
          if (index == -1) {
            keys_[count_] = a.keys_[i];
            summaries_[count_] = a.summaries_[i];
            count_++;
          }
        }
      }
    }
  }

  /**
   * Gets the result of this operation. This clears the state of this operator after the result is
   * returned.
   * @return the result of this operation as a CompactSketch
   * @deprecated Only used with deprecated {@link #update(Sketch,Sketch)}.
   * Instead use {@link #aNotB(Sketch, Sketch)} or a combination of {@link #setA(Sketch)} and
   * {@link #notB(Sketch)} with {@link #getResult(boolean)}.
   */
  @Deprecated
  public CompactSketch<S> getResult() {
    if (count_ == 0) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }
    final CompactSketch<S> result =
        new CompactSketch<>(Arrays.copyOfRange(keys_, 0, count_),
            Arrays.copyOfRange(summaries_, 0, count_), thetaLong_, empty_);
    reset();
    return result;
  }

  /**
   * Only used by deprecated {@link #update(Sketch, Sketch)}.
   * Remove at the same time other deprecated methods are removed.
   * @param sketch the given sketch to extract arrays from.
   */
  private void loadCompactedArrays(final Sketch<S> sketch) {
    if (sketch instanceof CompactSketch) {
      keys_ = sketch.keys_.clone();
      summaries_ = sketch.summaries_.clone();
    } else { // assuming only two types: CompactSketch and QuickSelectSketch
      final CompactSketch<S> compact = ((QuickSelectSketch<S>)sketch).compact();
      keys_ = compact.keys_;
      summaries_ = compact.summaries_;
    }
    count_ = sketch.getRetainedEntries();
  }

}
