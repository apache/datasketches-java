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

import static java.lang.Math.min;
import static org.apache.datasketches.HashOperations.convertToHashTable;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.simpleLog2OfLong;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.datasketches.SetOperationCornerCases;
import org.apache.datasketches.SetOperationCornerCases.AnotbAction;
import org.apache.datasketches.SetOperationCornerCases.CornerCase;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;

/**
 * Computes a set difference, A-AND-NOT-B, of two generic tuple sketches.
 * This class includes both stateful and stateless operations.
 *
 * <p>The stateful operation is as follows:</p>
 * <pre><code>
 * AnotB anotb = new AnotB();
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
 * AnotB anotb = new AnotB();
 *
 * CompactSketch csk = anotb.aNotB(Sketch skA, Sketch skB);
 * </code></pre>
 *
 * <p>Calling the <i>setA</i> operation a second time essentially clears the internal state and loads
 * the new sketch.</p>
 *
 * <p>The stateless and stateful operations are independent of each other.</p>
 *
 * @param <S> Type of Summary
 *
 * @author Lee Rhodes
 */
public final class AnotB<S extends Summary> {
  private boolean empty_ = true;
  private long thetaLong_ = Long.MAX_VALUE;
  private long[] hashArr_ = null;   //always in compact form, not necessarily sorted
  private S[] summaryArr_ = null; //always in compact form, not necessarily sorted
  private int curCount_ = 0;

  private static final Method GET_CACHE;

  static {
    try {
      GET_CACHE = org.apache.datasketches.theta.Sketch.class.getDeclaredMethod("getCache");
      GET_CACHE.setAccessible(true);
    } catch (final Exception e) {
      throw new SketchesStateException("Could not reflect getCache(): " + e);
    }
  }

  /**
   * This is part of a multistep, stateful AnotB operation and sets the given Tuple sketch as the
   * first argument <i>A</i> of <i>A-AND-NOT-B</i>. This overwrites the internal state of this
   * AnotB operator with the contents of the given sketch.
   * This sets the stage for multiple following <i>notB</i> steps.
   *
   * <p>An input argument of null will throw an exception.</p>
   *
   * <p>Rationale: In mathematics a "null set" is a set with no members, which we call an empty set.
   * That is distinctly different from the java <i>null</i>, which represents a nonexistent object.
   * In most cases it is a programming error due to some object that was not properly initialized.
   * With a null as the first argument, we cannot know what the user's intent is.
   * Since it is very likely that a <i>null</i> is a programming error, we throw a an exception.</p>
   *
   * <p>An empty input argument will set the internal state to empty.</p>
   *
   * <p>Rationale: An empty set is a mathematically legal concept. Although it makes any subsequent,
   * valid argument for B irrelevant, we must allow this and assume the user knows what they are
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

    empty_ = skA.isEmpty();
    thetaLong_ = skA.getThetaLong();
    final DataArrays<S> da = getCopyOfDataArraysTuple(skA);
    summaryArr_ = da.summaryArr;  //it may be null
    hashArr_ = da.hashArr;        //it may be null
    curCount_ = (hashArr_ == null) ? 0 : hashArr_.length;
  }

  /**
   * This is part of a multistep, stateful AnotB operation and sets the given Tuple sketch as the
   * second (or <i>n+1</i>th) argument <i>B</i> of <i>A-AND-NOT-B</i>.
   * Performs an <i>AND NOT</i> operation with the existing internal state of this AnotB operator.
   *
   * <p>An input argument of null or empty is ignored.</p>
   *
   * <p>Rationale: A <i>null</i> for the second or following arguments is more tolerable because
   * <i>A NOT null</i> is still <i>A</i> even if we don't know exactly what the null represents. It
   * clearly does not have any content that overlaps with <i>A</i>. Also, because this can be part of
   * a multistep operation with multiple <i>notB</i> steps. Other following steps can still produce
   * a valid result.</p>
   *
   * <p>Use {@link #getResult(boolean)} to obtain the result.</p>
   *
   * @param skB The incoming Tuple sketch for the second (or following) argument <i>B</i>.
   */
  public void notB(final Sketch<S> skB) {
    if (skB == null) { return; } //ignore

    final long thetaLongB = skB.getThetaLong();
    final int countB = skB.getRetainedEntries();
    final boolean emptyB = skB.isEmpty();

    final int id =
        SetOperationCornerCases.createCornerCaseId(thetaLong_, curCount_, empty_, thetaLongB, countB, emptyB);
    final CornerCase cCase = CornerCase.caseIdToCornerCase(id);
    final AnotbAction anotbAction = cCase.getAnotbAction();

    switch (anotbAction) {
      case EMPTY_1_0_T: {
        reset();
        break;
      }
      case DEGEN_MIN_0_F: {
        reset();
        thetaLong_ = min(thetaLong_, thetaLongB);
        empty_ = false;
        break;
      }
      case DEGEN_THA_0_F: {
        empty_ = false;
        curCount_ = 0;
        //thetaLong_ is ok
        break;
      }
      case TRIM_A: {
        thetaLong_ = min(thetaLong_, thetaLongB);
        final DataArrays<S> da = trimAndCopyDataArrays(hashArr_, summaryArr_, thetaLong_, true);
        hashArr_ = da.hashArr;
        curCount_ = (hashArr_ == null) ? 0 : hashArr_.length;
        summaryArr_ = da.summaryArr;
        //empty_ = is whatever SkA is,
        break;
      }
      case SKETCH_A: {
        break; //result is already in A
      }
      case FULL_ANOTB: { //both A and B should have valid entries.
        thetaLong_ = min(thetaLong_, thetaLongB);
        final DataArrays<S> daR = getCopyOfResultArraysTuple(thetaLong_, curCount_, hashArr_, summaryArr_, skB);
        hashArr_ = daR.hashArr;
        curCount_ = (hashArr_ == null) ? 0 : hashArr_.length;
        summaryArr_ = daR.summaryArr;
        //empty_ = is whatever SkA is,
      }
      //default: not possible
    }
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
   * <p>Rationale: A <i>null</i> for the second or following arguments is more tolerable because
   * <i>A NOT null</i> is still <i>A</i> even if we don't know exactly what the null represents. It
   * clearly does not have any content that overlaps with <i>A</i>. Also, because this can be part of
   * a multistep operation with multiple <i>notB</i> steps. Other following steps can still produce
   * a valid result.</p>
   *
   * <p>Use {@link #getResult(boolean)} to obtain the result.</p>
   *
   * @param skB The incoming Theta sketch for the second (or following) argument <i>B</i>.
   */
  public void notB(final org.apache.datasketches.theta.Sketch skB) {
    if (skB == null) { return; } //ignore

    final long thetaLongB = skB.getThetaLong();
    final int countB = skB.getRetainedEntries();
    final boolean emptyB = skB.isEmpty();

    final int id =
        SetOperationCornerCases.createCornerCaseId(thetaLong_, curCount_, empty_, thetaLongB, countB, emptyB);
    final CornerCase cCase = CornerCase.caseIdToCornerCase(id);
    final AnotbAction anotbAction = cCase.getAnotbAction();

    switch (anotbAction) {
      case EMPTY_1_0_T: {
        reset();
        break;
      }
      case DEGEN_MIN_0_F: {
        reset();
        thetaLong_ = min(thetaLong_, thetaLongB);
        empty_ = false;
        break;
      }
      case DEGEN_THA_0_F: {
        empty_ = false;
        curCount_ = 0;
        //thetaLong_ is ok
        break;
      }
      case TRIM_A: {
        thetaLong_ = min(thetaLong_, thetaLongB);
        final DataArrays<S> da = trimAndCopyDataArrays(hashArr_, summaryArr_,thetaLong_, true);
        hashArr_ = da.hashArr;
        curCount_ = (hashArr_ == null) ? 0 : hashArr_.length;
        summaryArr_ = da.summaryArr;
        break;
      }
      case SKETCH_A: {
        break; //result is already in A
      }
      case FULL_ANOTB: { //both A and B should have valid entries.
        thetaLong_ = min(thetaLong_, thetaLongB);
        final DataArrays<S> daB = getCopyOfResultArraysTheta(thetaLong_, curCount_, hashArr_, summaryArr_, skB);
        hashArr_ = daB.hashArr;
        curCount_ = (hashArr_ == null) ? 0 : hashArr_.length;
        summaryArr_ = daB.summaryArr;
        //empty_ = is whatever SkA is,
      }
      //default: not possible
    }
  }

  /**
   * Gets the result of the multistep, stateful operation AnotB that have been executed with calls
   * to {@link #setA(Sketch)} and ({@link #notB(Sketch)} or
   * {@link #notB(org.apache.datasketches.theta.Sketch)}).
   *
   * @param reset If <i>true</i>, clears this operator to the empty state after this result is
   * returned. Set this to <i>false</i> if you wish to obtain an intermediate result.
   * @return the result of this operation as an unordered {@link CompactSketch}.
   */
  public CompactSketch<S> getResult(final boolean reset) {
    final CompactSketch<S> result;
    if (curCount_ == 0) {
      result = new CompactSketch<>(null, null, thetaLong_, thetaLong_ == Long.MAX_VALUE);
    } else {

      result = new CompactSketch<>(hashArr_, Util.copySummaryArray(summaryArr_), thetaLong_, false);
    }
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
   * That is distinctly different from the java <i>null</i>, which represents a nonexistent object.
   * In most cases it is a programming error due to some object that was not properly initialized.
   * With a null as the first argument, we cannot know what the user's intent is.
   * With a null as the second argument, we can't ignore it as we must return a result and there is
   * no following possible viable arguments for the second argument.
   * Since it is very likely that a <i>null</i> is a programming error, we throw an exception.</p>
   *
   * @param skA The incoming Tuple sketch for the first argument
   * @param skB The incoming Tuple sketch for the second argument
   * @param <S> Type of Summary
   * @return the result as an unordered {@link CompactSketch}
   */
  public static <S extends Summary> CompactSketch<S> aNotB(
      final Sketch<S> skA,
      final Sketch<S> skB) {
    if (skA == null || skB == null) {
      throw new SketchesArgumentException("Neither argument may be null for this stateless operation.");
    }

    final long thetaLongA = skA.getThetaLong();
    final int countA = skA.getRetainedEntries();
    final boolean emptyA = skA.isEmpty();

    final long thetaLongB = skB.getThetaLong();
    final int countB = skB.getRetainedEntries();
    final boolean emptyB = skB.isEmpty();

    final int id =
        SetOperationCornerCases.createCornerCaseId(thetaLongA, countA, emptyA, thetaLongB, countB, emptyB);
    final CornerCase cCase = CornerCase.caseIdToCornerCase(id);
    final AnotbAction anotbAction = cCase.getAnotbAction();

    CompactSketch<S> result = null;

    switch (anotbAction) {
      case EMPTY_1_0_T: {
        result = new CompactSketch<>(null, null, Long.MAX_VALUE, true);
        break;
      }
      case DEGEN_MIN_0_F: {
        final long thetaLong = min(thetaLongA, thetaLongB);
        result = new CompactSketch<>(null, null, thetaLong, false);
        break;
      }
      case DEGEN_THA_0_F: {
        result = new CompactSketch<>(null, null, thetaLongA, false);
        break;
      }
      case TRIM_A: {
        final DataArrays<S> daA = getCopyOfDataArraysTuple(skA);
        final long[] hashArrA = daA.hashArr;
        final S[] summaryArrA = daA.summaryArr;
        final long minThetaLong =  min(thetaLongA, thetaLongB);
        final DataArrays<S> da = trimAndCopyDataArrays(hashArrA, summaryArrA, minThetaLong, false);
        result = new CompactSketch<>(da.hashArr, da.summaryArr, minThetaLong, skA.empty_);
        break;
      }
      case SKETCH_A: {
        final DataArrays<S> daA = getCopyOfDataArraysTuple(skA);
        result = new CompactSketch<>(daA.hashArr, daA.summaryArr, thetaLongA, skA.empty_);
        break;
      }
      case FULL_ANOTB: { //both A and B should have valid entries.
        final DataArrays<S> daA = getCopyOfDataArraysTuple(skA);
        final long minThetaLong = min(thetaLongA, thetaLongB);
        final DataArrays<S> daR =
            getCopyOfResultArraysTuple(minThetaLong, daA.hashArr.length, daA.hashArr, daA.summaryArr, skB);
        final int countR = (daR.hashArr == null) ? 0 : daR.hashArr.length;
        if (countR == 0) {
          result = new CompactSketch<>(null, null, minThetaLong, minThetaLong == Long.MAX_VALUE);
        } else {
          result = new CompactSketch<>(daR.hashArr, daR.summaryArr, minThetaLong, false);
        }
      }
      //default: not possible
    }
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
   * That is distinctly different from the java <i>null</i>, which represents a nonexistent object.
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
   * @return the result as an unordered {@link CompactSketch}
   */
  public static <S extends Summary> CompactSketch<S> aNotB(
      final Sketch<S> skA,
      final org.apache.datasketches.theta.Sketch skB) {
    if (skA == null || skB == null) {
      throw new SketchesArgumentException("Neither argument may be null for this stateless operation.");
    }

    final long thetaLongA = skA.getThetaLong();
    final int countA = skA.getRetainedEntries();
    final boolean emptyA = skA.isEmpty();

    final long thetaLongB = skB.getThetaLong();
    final int countB = skB.getRetainedEntries();
    final boolean emptyB = skB.isEmpty();

    final int id =
        SetOperationCornerCases.createCornerCaseId(thetaLongA, countA, emptyA, thetaLongB, countB, emptyB);
    final CornerCase cCase = CornerCase.caseIdToCornerCase(id);
    final AnotbAction anotbAction = cCase.getAnotbAction();

    CompactSketch<S> result = null;

    switch (anotbAction) {
      case EMPTY_1_0_T: {
        result = new CompactSketch<>(null, null, Long.MAX_VALUE, true);
        break;
      }
      case DEGEN_MIN_0_F: {
        final long thetaLong = min(thetaLongA, thetaLongB);
        result = new CompactSketch<>(null, null, thetaLong, false);
        break;
      }
      case DEGEN_THA_0_F: {
        result = new CompactSketch<>(null, null, thetaLongA, false);
        break;
      }
      case TRIM_A: {
        final DataArrays<S> daA = getCopyOfDataArraysTuple(skA);
        final long[] hashArrA = daA.hashArr;
        final S[] summaryArrA = daA.summaryArr;
        final long minThetaLong = min(thetaLongA, thetaLongB);
        final DataArrays<S> da = trimAndCopyDataArrays(hashArrA, summaryArrA, minThetaLong, false);
        result = new CompactSketch<>(da.hashArr, da.summaryArr, minThetaLong, skA.empty_);
        break;
      }
      case SKETCH_A: {
        final DataArrays<S> daA = getCopyOfDataArraysTuple(skA);
        result = new CompactSketch<>(daA.hashArr, daA.summaryArr, thetaLongA, skA.empty_);
        break;
      }
      case FULL_ANOTB: { //both A and B should have valid entries.
        final DataArrays<S> daA = getCopyOfDataArraysTuple(skA);
        final long minThetaLong = min(thetaLongA, thetaLongB);
        final DataArrays<S> daR =
            getCopyOfResultArraysTheta(minThetaLong, daA.hashArr.length, daA.hashArr, daA.summaryArr, skB);
        final int countR = (daR.hashArr == null) ? 0 : daR.hashArr.length;
        if (countR == 0) {
          result = new CompactSketch<>(null, null, minThetaLong, minThetaLong == Long.MAX_VALUE);
        } else {
          result = new CompactSketch<>(daR.hashArr, daR.summaryArr, minThetaLong, false);
        }
      }
      //default: not possible
    }
    return result;
  }

  //restricted

  static class DataArrays<S extends Summary> {
    DataArrays() {}

    long[] hashArr;
    S[] summaryArr;
  }

  private static <S extends Summary> DataArrays<S> getCopyOfDataArraysTuple(
      final Sketch<S> sk) {
    final CompactSketch<S> csk;
    final DataArrays<S> da = new DataArrays<>();
    if (sk instanceof CompactSketch) {
      csk = (CompactSketch<S>) sk;
    } else {
      csk = ((QuickSelectSketch<S>)sk).compact();
    }
    final int count = csk.getRetainedEntries();
    if (count == 0) {
      da.hashArr = null;
      da.summaryArr = null;
    } else {
      da.hashArr = csk.getHashArr().clone();       //deep copy, may not be sorted
      da.summaryArr = Util.copySummaryArray(csk.getSummaryArr());
    }
    return da;
  }

  @SuppressWarnings("unchecked")
  //Both skA and skB must have entries (count > 0)
  private static <S extends Summary> DataArrays<S> getCopyOfResultArraysTuple(
      final long minThetaLong,
      final int countA,
      final long[] hashArrA,
      final S[] summaryArrA,
      final Sketch<S> skB) {
    final DataArrays<S> daR = new DataArrays<>();

    //Rebuild/get hashtable of skB
    final long[] hashTableB;

    if (skB instanceof CompactSketch) {
      final CompactSketch<S> cskB = (CompactSketch<S>) skB;
      final int countB = skB.getRetainedEntries();
      hashTableB = convertToHashTable(cskB.getHashArr(), countB, minThetaLong, REBUILD_THRESHOLD);
    } else {
      final QuickSelectSketch<S> qskB = (QuickSelectSketch<S>) skB;
      hashTableB = qskB.getHashTable();
    }

    //build temporary arrays of skA
    final long[] tmpHashArrA = new long[countA];
    final S[] tmpSummaryArrA = Util.newSummaryArray(summaryArrA, countA);

    //search for non matches and build temp arrays
    final int lgHTBLen = simpleLog2OfLong(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if (hash != 0 && hash < minThetaLong) { //skips hashes of A >= minTheta
        final int index = hashSearch(hashTableB, lgHTBLen, hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          tmpSummaryArrA[nonMatches] = (S) summaryArrA[i].copy();
          nonMatches++;
        }
      }
    }
    daR.hashArr = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    daR.summaryArr = Arrays.copyOfRange(tmpSummaryArrA, 0, nonMatches);
    return daR;
  }

  @SuppressWarnings("unchecked")
  private static <S extends Summary> DataArrays<S> getCopyOfResultArraysTheta(
      final long minThetaLong,
      final int countA,
      final long[] hashArrA,
      final S[] summaryArrA,
      final org.apache.datasketches.theta.Sketch skB) {
    final DataArrays<S> daB = new DataArrays<>();

    //Rebuild/get hashtable of skB
    final long[] hashTableB; //read only

    final long[] hashCacheB;
    try { hashCacheB = (long[])GET_CACHE.invoke(skB);
    } catch (final Exception e) { throw new SketchesStateException("Reflection Exception " + e); }

    if (skB instanceof org.apache.datasketches.theta.CompactSketch) {
      final int countB = skB.getRetainedEntries(true);
      hashTableB = convertToHashTable(hashCacheB, countB, minThetaLong, REBUILD_THRESHOLD);
    } else {
      hashTableB = hashCacheB;
    }

    //build temporary result arrays of skA
    final long[] tmpHashArrA = new long[countA];
    final S[] tmpSummaryArrA = Util.newSummaryArray(summaryArrA, countA);

    //search for non matches and build temp arrays
    final int lgHTBLen = simpleLog2OfLong(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if (hash != 0 && hash < minThetaLong) { //skips hashes of A >= minTheta
        final int index = hashSearch(hashTableB, lgHTBLen, hash);
        if (index == -1) { //not found
          tmpHashArrA[nonMatches] = hash;
          tmpSummaryArrA[nonMatches] = (S) summaryArrA[i].copy();
          nonMatches++;
        }
      }
    }
    //trim the arrays
    daB.hashArr = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    daB.summaryArr = Arrays.copyOfRange(tmpSummaryArrA, 0, nonMatches);
    return daB;
  }

  @SuppressWarnings("unchecked")
  private static <S extends Summary> DataArrays<S> trimAndCopyDataArrays(
      final long[] hashArr,
      final S[] summaryArr,
      final long minThetaLong,
      final boolean copy) {

    //build temporary arrays
    final int countIn = hashArr.length;
    final long[] tmpHashArr = new long[countIn];
    final S[] tmpSummaryArr = Util.newSummaryArray(summaryArr, countIn);
    int countResult = 0;
    for (int i = 0; i < countIn; i++) {
      final long hash = hashArr[i];
      if (hash < minThetaLong) {
        tmpHashArr[countResult] = hash;
        tmpSummaryArr[countResult] = (S) (copy ? summaryArr[i].copy() : summaryArr[i]);
        countResult++;
      } else { continue; }
    }
    //Remove empty slots
    final DataArrays<S> da = new DataArrays<>();
    da.hashArr = Arrays.copyOfRange(tmpHashArr, 0, countResult);
    da.summaryArr = Arrays.copyOfRange(tmpSummaryArr, 0, countResult);
    return da;
  }

  /**
   * Resets this operation back to the empty state.
   */
  public void reset() {
    empty_ = true;
    thetaLong_ = Long.MAX_VALUE;
    hashArr_ = null;
    summaryArr_ = null;
    curCount_ = 0;
  }

}
