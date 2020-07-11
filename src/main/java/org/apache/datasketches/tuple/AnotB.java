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

import static org.apache.datasketches.HashOperations.convertToHashTable;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.simpleIntLog2;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.theta.HashIterator;

/**
 * Computes a set difference, A-AND-NOT-B, of two generic tuple sketches.
 * This class includes both stateful and stateless operations.
 *
 * <p>The stateful operation is as follows:</p>
 * <pre><code>
 * AnotB anotb = SetOperationBuilder.buildAnotB();
 *
 * anotb.setA(Sketch skA); //The first argument.
 * anotb.notB(Sketch skB); //The second (subtraction) argument.
 * anotb.notB(Sketch skC); // ...any number of additional subtractions...
 * anotb.getResult(false); //Get an interim result.
 * anotb.notB(Sketch skD); //Additional subtractions.
 * anotb.getResult(true);  //Final result and resets the AnotB operator.
 * </code></pre>
 *
 * <p>The stateless operation is as follows:</p>
 * <pre><code>
 * AnotB anotb = SetOperationBuilder.buildAnotB();
 *
 * CompactSketch csk = anotb.aNotB(Sketch skA, Sketch skB);
 * </code></pre>
 *
 * <p>Calling the <i>setA</i> operation a second time essentially clears the internal state and loads
 * the new sketch.</p>
 *
 * <p>The stateless and stateful operations are independent of each other with the exception of
 * sharing the same update hash seed loaded as the default seed or specified by the user as an
 * argument to the builder.</p>
 *
 * @param <S> Type of Summary
 */
public final class AnotB<S extends Summary> {
  private boolean empty_ = true;
  private long thetaLong_ = Long.MAX_VALUE;
  private long[] hashArr_ = null;   //always in compact form, not necessarily sorted
  private S[] summaryArr_ = null; //always in compact form, not necessarily sorted
  private int curCount_ = 0;

  /**
   * This is part of a multistep, stateful AnotB operation and sets the given Tuple sketch as the
   * first argument <i>A</i> of <i>A-AND-NOT-B</i>. This overwrites the internal state of this
   * AnotB operator with the contents of the given sketch.
   * This sets the stage for multiple following <i>notB</i> steps.
   *
   * <p>An input argument of null will throw an exception.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistant object.
   * In most cases it is a programming error due to some object that was not properly initialized.
   * With a null as the first argument, we cannot know what the user's intent is.
   * Since it is very likely that a <i>null</i> is a programming error, we throw a an exception.</p>
   *
   * <p>An enpty input argument will set the internal state to empty.</p>
   *
   * <p>Rationale: An empty set is a mathematically legal concept. Although it makes any subsequent,
   * valid argument for B irrelvant, we must allow this and assume the user knows what they are
   * doing.</p>
   *
   * <p>Performing {@link #getResult(boolean)} just after this step will return a compact form of
   * the given argument.</p>
   *
   * @param skA The incoming sketch for the first argument, <i>A</i>.
   */
  public void setA(final Sketch<S> skA) {
    if (skA == null) {
      reset();
      throw new SketchesArgumentException("The input argument <i>A</i> may not be null");
    }
    if (skA.isEmpty()) {
      reset();
      return;
    }
    //skA is not empty
    empty_ = false;
    thetaLong_ = skA.getThetaLong();
    final CompactSketch<S> cskA = (skA instanceof CompactSketch)
        ? (CompactSketch<S>)skA
        : ((QuickSelectSketch<S>)skA).compact();
    hashArr_ = cskA.getHashArr().clone();
    summaryArr_ = cskA.getSummaryArr().clone(); //shallow
    curCount_ = cskA.getRetainedEntries();
  }

  /**
   * This is part of a multistep, stateful AnotB operation and sets the given Tuple sketch as the
   * second (or <i>n+1</i>th) argument <i>B</i> of <i>A-AND-NOT-B</i>.
   * Performs an <i>AND NOT</i> operation with the existing internal state of this AnotB operator.
   *
   * <p>An input argument of null or empty is ignored.</p>
   *
   * <p>Rationale: A <i>null</i> for the second or following arguments is more tollerable because
   * <i>A NOT null</i> is still <i>A</i> even if we don't know exactly what the null represents. It
   * clearly does not have any content that overlaps with <i>A</i>. Also, because this can be part of
   * a multistep operation with multiple <i>notB</i> steps. Other following steps can still produce
   * a valid result.</p>
   *
   * <p>Use {@link #getResult(boolean)} to obtain the result.</p>
   *
   * @param skB The incoming Tuple sketch for the second (or following) argument <i>B</i>.
   */
  @SuppressWarnings("unchecked")
  public void notB(final Sketch<S> skB) {
    if (empty_ || (skB == null) || skB.isEmpty()) { return; }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    thetaLong_ = Math.min(thetaLong_, thetaLongB);

    //Build hashtable and removes hashes of skB >= theta
    final int countB = skB.getRetainedEntries();
    CompactSketch<S> cskB = null;
    QuickSelectSketch<S> qskB = null;
    final long[] hashTableB;
    if (skB instanceof CompactSketch) {
      cskB = (CompactSketch<S>) skB;
      hashTableB =
          convertToHashTable(cskB.getHashArr(), countB, thetaLong_, REBUILD_THRESHOLD);
    } else {
      qskB = (QuickSelectSketch<S>) skB;
      hashTableB = (thetaLong_ < thetaLongB)
          ? convertToHashTable(qskB.getHashTable(), countB, thetaLong_, REBUILD_THRESHOLD)
          : qskB.getHashTable();
    }

    //build temporary arrays of skA
    final long[] tmpHashArrA = new long[curCount_];
    final Class<S> summaryType = (Class<S>) summaryArr_.getClass().getComponentType();
    final S[] tmpSummaryArrA = (S[]) Array.newInstance(summaryType, curCount_);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < curCount_; i++) {
      final long hash = hashArr_[i];
      if ((hash != 0) && (hash < thetaLong_)) { //skips hashes of A >= theta
        final int index =
            hashSearch(hashTableB, simpleIntLog2(hashTableB.length), hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          tmpSummaryArrA[nonMatches] = summaryArr_[i];
          nonMatches++;
        }
      }
    }
    hashArr_ = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    summaryArr_ = Arrays.copyOfRange(tmpSummaryArrA, 0, nonMatches);
    curCount_ = nonMatches;
  }

  /**
   * This is part of a multistep, stateful AnotB operation and sets the given Theta sketch as the
   * second (or <i>n+1</i>th) argument <i>B</i> of <i>A-AND-NOT-B</i>.
   * Performs an <i>AND NOT</i> operation with the existing internal state of this AnotB operator.
   * Calls to this method can be intermingled with calls to
   * {@link #notB(org.apache.datasketches.theta.Sketch)}.
   *
   * <p>An input argument of null or empty is ignored.</p>
   *
   * <p>Rationale: A <i>null</i> for the second or following arguments is more tollerable because
   * <i>A NOT null</i> is still <i>A</i> even if we don't know exactly what the null represents. It
   * clearly does not have any content that overlaps with <i>A</i>. Also, because this can be part of
   * a multistep operation with multiple <i>notB</i> steps. Other following steps can still produce
   * a valid result.</p>
   *
   * <p>Use {@link #getResult(boolean)} to obtain the result.</p>
   *
   * @param skB The incoming Theta sketch for the second (or following) argument <i>B</i>.
   */
  @SuppressWarnings("unchecked")
  public void notB(final org.apache.datasketches.theta.Sketch skB) {
    if (empty_ || (skB == null) || skB.isEmpty()) { return; }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    thetaLong_ = Math.min(thetaLong_, thetaLongB);
    //Build hashtable and removes hashes of skB >= theta
    final int countB = skB.getRetainedEntries(true);
    final long[] hashTableB =
        convertToHashTable(extractThetaHashArray(skB, countB), countB,
            thetaLong_, REBUILD_THRESHOLD);

    //build temporary arrays of skA
    final long[] tmpHashArrA = new long[curCount_];
    final Class<S> summaryType = (Class<S>) summaryArr_.getClass().getComponentType();
    final S[] tmpSummaryArrA = (S[]) Array.newInstance(summaryType, curCount_);

    //search for non matches and build temp arrays
    final int lgHTBLen = simpleIntLog2(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < curCount_; i++) {
      final long hash = hashArr_[i];
      if ((hash > 0) && (hash < thetaLong_)) { //skips hashes of A >= theta
        final int index = hashSearch(hashTableB, lgHTBLen, hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          tmpSummaryArrA[nonMatches] = summaryArr_[i];
          nonMatches++;
        }
      }
    }
    hashArr_ = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    summaryArr_ = Arrays.copyOfRange(tmpSummaryArrA, 0, nonMatches);
    curCount_ = nonMatches;
  }

  /**
   * Gets the result of the mutistep, stateful operation AnotB that have been executed with calls
   * to {@link #setA(Sketch)} and ({@link #notB(Sketch)} or
   * {@link #notB(org.apache.datasketches.theta.Sketch)}).
   *
   * @param reset If <i>true</i>, clears this operator to the empty state after this result is
   * returned. Set this to <i>false</i> if you wish to obtain an intermediate result.
   * @return the result of this operation as a {@link CompactSketch}.
   */
  public CompactSketch<S> getResult(final boolean reset) {
    if (curCount_ == 0) {
      return new CompactSketch<>(null, null, thetaLong_, empty_);
    }
    final CompactSketch<S> result =
        new CompactSketch<>(Arrays.copyOfRange(hashArr_, 0, curCount_),
            Arrays.copyOfRange(summaryArr_, 0, curCount_), thetaLong_, empty_);
    if (reset) { reset(); }
    return result;
  }

  /**
   * Returns the A-and-not-B set operation on the two given Tuple sketches.
   *
   * <p>This a stateless operation and has no impact on the internal state of this operator.
   * Thus, this is not an accumulating update and is independent of the {@link #setA(Sketch)},
   * {@link #notB(Sketch)}, {@link #notB(org.apache.datasketches.theta.Sketch)}, and
   * {@link #getResult(boolean)} methods.</p>
   *
   * <p>If either argument is null an exception is thrown.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistant object.
   * In most cases it is a programming error due to some object that was not properly initialized.
   * With a null as the first argument, we cannot know what the user's intent is.
   * With a null as the second argument, we can't ignore it as we must return a result and there is
   * no following possible viable arguments for the second argument.
   * Since it is very likely that a <i>null</i> is a programming error, we throw an exception.</p>
   *
   * @param skA The incoming Tuple sketch for the first argument
   * @param skB The incoming Tuple sketch for the second argument
   * @param <S> Type of Summary
   * @return the result as a compact sketch
   */
  @SuppressWarnings("unchecked")
  public static <S extends Summary>
        CompactSketch<S> aNotB(final Sketch<S> skA, final Sketch<S> skB) {
    if ((skA == null) || (skB == null)) {
      throw new SketchesArgumentException("Neither argument may be null");
    }
    if (skA.isEmpty()) { return new CompactSketch<>(null, null, Long.MAX_VALUE, true); }
    if (skB.isEmpty()) { return skA.compact(); }
    //Both skA & skB are not empty
    //Load skA into tmp registers
    boolean empty = false;
    final long thetaLongA = skA.getThetaLong();
    final CompactSketch<S> cskA = (skA instanceof CompactSketch)
        ? (CompactSketch<S>)skA
        : ((QuickSelectSketch<S>)skA).compact();
    final long[] hashArrA = cskA.getHashArr().clone();
    final S[] summaryArrA = cskA.getSummaryArr().clone();
    final int countA = cskA.getRetainedEntries();

    //Compare with skB
    final long thetaLongB = skB.getThetaLong();
    final long thetaLong = Math.min(thetaLongA, thetaLongB);
    final int countB = skB.getRetainedEntries();

    //Build/rebuild hashtable and removes hashes of skB >= thetaLong
    final long[] hashes = (skB instanceof CompactSketch)
        ? ((CompactSketch<S>) skB).getHashArr()
        : ((QuickSelectSketch<S>) skB).getHashTable();

    final long[] hashTableB = convertToHashTable(hashes, countB, thetaLong, REBUILD_THRESHOLD);

    //build temporary hash array and summary array for values from skA
    final long[] tmpHashArrA = new long[countA];
    final Class<S> summaryType = (Class<S>) summaryArrA.getClass().getComponentType();
    final S[] tmpSummaryArrA = (S[]) Array.newInstance(summaryType, countA);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if ((hash != 0) && (hash < thetaLong)) { //skips hashes of A >= theta
        final int index =
            hashSearch(hashTableB, simpleIntLog2(hashTableB.length), hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          tmpSummaryArrA[nonMatches] = summaryArrA[i];
          nonMatches++;
        }
      }
    }
    //final compaction
    empty = ((nonMatches == 0) && (thetaLong == Long.MAX_VALUE));
    final long[] hashArrOut = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    final S[] summaryArrOut = Arrays.copyOfRange(tmpSummaryArrA, 0, nonMatches);
    final CompactSketch<S> result =
        new CompactSketch<>(hashArrOut, summaryArrOut, thetaLong, empty);
    return result;
  }

  /**
   * Returns the A-and-not-B set operation on a Tuple sketch and a Theta sketch.
   *
   * <p>This a stateless operation and has no impact on the internal state of this operator.
   * Thus, this is not an accumulating update and is independent of the {@link #setA(Sketch)},
   * {@link #notB(Sketch)}, {@link #notB(org.apache.datasketches.theta.Sketch)}, and
   * {@link #getResult(boolean)} methods.</p>
   *
   * <p>If either argument is null an exception is thrown.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistant object.
   * In most cases it is a programming error due to some object that was not properly initialized.
   * With a null as the first argument, we cannot know what the user's intent is.
   * With a null as the second argument, we can't ignore it as we must return a result and there is
   * no following possible viable arguments for the second argument.
   * Since it is very likely that a <i>null</i> is a programming error for either argument
   * we throw a an exception.</p>
   *
   * @param skA The incoming Tuple sketch for the first argument
   * @param skB The incoming Theta sketch for the second argument
   * @param <S> Type of Summary
   * @return the result as a compact sketch
   */
  @SuppressWarnings("unchecked")
  public static <S extends Summary>
        CompactSketch<S> aNotB(final Sketch<S> skA, final org.apache.datasketches.theta.Sketch skB) {
    if ((skA == null) || (skB == null)) {
      throw new SketchesArgumentException("Neither argument may be null");
    }
    if (skA.isEmpty()) {
      return new CompactSketch<>(null, null, Long.MAX_VALUE, true);
    }
    //skA is not empty
    final boolean empty = false;
    final long thetaLongA = skA.getThetaLong();
    final CompactSketch<S> cskA = (skA instanceof CompactSketch)
        ? (CompactSketch<S>)skA
        : ((QuickSelectSketch<S>)skA).compact();
    final long[] hashArrA = cskA.getHashArr();
    final S[] summaryArrA = cskA.getSummaryArr();
    final int countA = cskA.getRetainedEntries();

    if (skB.isEmpty()) {
      return new CompactSketch<>(hashArrA, summaryArrA, thetaLongA, empty);
    }
    //skB is not empty
    final long thetaLongB = skB.getThetaLong();
    final long thetaLong = Math.min(thetaLongA, thetaLongB);
    final int countB = skB.getRetainedEntries(true);

    //Build/rebuild hashtable and removes hashes of skB >= thetaLong
    final long[] hashTableB = //the following convert works for all theta sketches
        convertToHashTable(extractThetaHashArray(skB, countB), countB, thetaLong, REBUILD_THRESHOLD);

    //build temporary arrays of skA for matching
    final long[] tmpHashArrA = new long[countA];
    final Class<S> summaryType = (Class<S>) summaryArrA.getClass().getComponentType();
    final S[] tmpSummaryArrA = (S[]) Array.newInstance(summaryType, countA);

    //search for non matches and build temp arrays
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if ((hash != 0) && (hash < thetaLong)) { //skips hashes of A >= theta
        final int index =
            hashSearch(hashTableB, simpleIntLog2(hashTableB.length), hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          tmpSummaryArrA[nonMatches] = summaryArrA[i];
          nonMatches++;
        }
      }
    }
    final long[] hashArr = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    final S[] summaryArr = Arrays.copyOfRange(tmpSummaryArrA, 0, nonMatches);
    final CompactSketch<S> result =
        new CompactSketch<>(hashArr, summaryArr, thetaLong, empty);
    return result;
  }

  /**
   * Resets this sketch back to the empty state.
   */
  private void reset() {
    empty_ = true;
    thetaLong_ = Long.MAX_VALUE;
    hashArr_ = null;
    summaryArr_ = null;
    curCount_ = 0;
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

  //Deprecated methods

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   * This is not an accumulating update. Calling this update() more than once
   * without calling getResult() will discard the result of previous update() by this method.
   * The result is obtained by calling getResult();
   *
   * @param skA The incoming sketch for the first argument
   * @param skB The incoming sketch for the second argument
   * @deprecated Instead please use {@link #aNotB(Sketch, Sketch)}.
   */
  @Deprecated
  public void update(final Sketch<S> skA, final Sketch<S> skB) {
    //duplicate old behavior
    reset();
    if (skA == null) { return; }
    else { setA(skA); }
    if (skB == null) { return; }
    else { notB(skB); }
  }

  /**
   * Gets the result of this operation. This clears the state of this operator after the result is
   * returned.
   * @return the result of this operation as a CompactSketch
   * @deprecated Instead use {@link #getResult(boolean)}.
   */
  @Deprecated
  public CompactSketch<S> getResult() {
    return getResult(true);
  }

}
