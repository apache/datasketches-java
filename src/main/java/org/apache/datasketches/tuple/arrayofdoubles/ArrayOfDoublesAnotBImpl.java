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

package org.apache.datasketches.tuple.arrayofdoubles;

import static java.lang.Math.min;
import static org.apache.datasketches.HashOperations.continueCondition;
import static org.apache.datasketches.HashOperations.convertToHashTable;
import static org.apache.datasketches.HashOperations.count;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.simpleLog2OfLong;

import java.util.Arrays;

import org.apache.datasketches.SetOperationCornerCases;
import org.apache.datasketches.SetOperationCornerCases.AnotbAction;
import org.apache.datasketches.SetOperationCornerCases.CornerCase;
//import org.apache.datasketches.tuple.AnotB.DataArrays;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.Util;

/**
 * Computes a set difference, A-AND-NOT-B, of two ArrayOfDoublesSketches.
 *
 * <p>This class includes a stateless operation as follows:</p>
 *
 * <pre><code>
 * CompactSketch csk = anotb.aNotB(ArrayOfDoublesSketch skA, ArrayOfDoublesSketch skB);
 * </code></pre>
 *
 * @author Lee Rhodes
 */
public class ArrayOfDoublesAnotBImpl extends ArrayOfDoublesAnotB {
  private int numValues_;
  private short seedHash_;

  private long thetaLong_ = Long.MAX_VALUE;
  private boolean empty_ = true;
  private long[] keys_;
  private double[] values_;
  private int count_;

  ArrayOfDoublesAnotBImpl(final int numValues, final long seed) {
    numValues_ = numValues;
    seedHash_ = Util.computeSeedHash(seed);
  }

  @Override
  public void update(final ArrayOfDoublesSketch skA, final ArrayOfDoublesSketch skB) {
    if (skA == null || skB == null) {
      throw new SketchesArgumentException("Neither argument may be null.");
    }
    numValues_ = skA.getNumValues();
    seedHash_ = skA.getSeedHash();
    if (numValues_ != skB.getNumValues()) {
      throw new SketchesArgumentException("Inputs cannot have different numValues");
    }
    if (seedHash_ != skB.getSeedHash()) {
      throw new SketchesArgumentException("Inputs cannot have different seedHashes");
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

    final long minThetaLong = min(thetaLongA, thetaLongB);

    switch (anotbAction) {
      case EMPTY_1_0_T: {
        reset();
        break;
      }
      case DEGEN_MIN_0_F: {
        keys_ = null;
        values_ = null;
        thetaLong_ = minThetaLong;
        empty_ = false;
        count_ = 0;
        break;
      }
      case DEGEN_THA_0_F: {
        keys_ = null;
        values_ = null;
        thetaLong_ = thetaLongA;
        empty_ = false;
        count_ = 0;
        break;
      }
      case TRIM_A: {
        final DataArrays daA = new DataArrays(skA.getKeys(), skA.getValuesAsOneDimension(), countA);
        final DataArrays da = trimDataArrays(daA, minThetaLong, numValues_);
        keys_ = da.hashArr;
        values_ = da.valuesArr;
        thetaLong_ = minThetaLong;
        empty_ = skA.isEmpty();
        count_ = da.count;
        break;
      }
      case SKETCH_A: {
        final ArrayOfDoublesCompactSketch csk = skA.compact();
        keys_ = csk.getKeys();
        values_ = csk.getValuesAsOneDimension();
        thetaLong_ = csk.thetaLong_;
        empty_ = csk.isEmpty();
        count_ = csk.getRetainedEntries();
        break;
      }
      case FULL_ANOTB: { //both A and B should have valid entries.
        final long[] keysA = skA.getKeys();
        final double[] valuesA = skA.getValuesAsOneDimension();
        final DataArrays daR = getResultArrays(minThetaLong, countA, keysA, valuesA, skB);
        count_ = daR.count;
        keys_ = (count_ == 0) ? null : daR.hashArr;
        values_ = (count_ == 0) ? null : daR.valuesArr;
        thetaLong_ = minThetaLong;
        empty_ = (minThetaLong == Long.MAX_VALUE) && (count_ == 0);
        break;
      }
      //default: not possible
    }
  }

  @Override
  public ArrayOfDoublesCompactSketch getResult() {
    return new HeapArrayOfDoublesCompactSketch(keys_, values_, thetaLong_, empty_, numValues_, seedHash_);
  }

  @Override
  public ArrayOfDoublesCompactSketch getResult(final WritableMemory dstMem) {
    return new DirectArrayOfDoublesCompactSketch(keys_, values_, thetaLong_, empty_, numValues_, seedHash_, dstMem);
  }

  private static DataArrays getResultArrays(
      final long minThetaLong,
      final int countA,
      final long[] hashArrA,
      final double[] valuesArrA,
      final ArrayOfDoublesSketch skB) {
    final int numValues = skB.numValues_;

    //create hashtable of skB
    final long[] hashTableB = convertToHashTable(skB.getKeys(), skB.getRetainedEntries(), minThetaLong,
        REBUILD_THRESHOLD);

    //build temporary arrays of skA
    long[] tmpHashArrA = new long[countA];
    double[] tmpValuesArrA = new double[countA * numValues];

    //search for non matches and build temp arrays
    final int lgHTBLen = simpleLog2OfLong(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if (continueCondition(minThetaLong, hash)) { continue; }
      final int index = hashSearch(hashTableB, lgHTBLen, hash);
      if (index == -1) {
        tmpHashArrA[nonMatches] = hash;
        System.arraycopy(valuesArrA, i * numValues, tmpValuesArrA, nonMatches * numValues, numValues);
        nonMatches++;
      }
    }
    tmpHashArrA = Arrays.copyOf(tmpHashArrA, nonMatches);
    tmpValuesArrA = Arrays.copyOf(tmpValuesArrA, nonMatches * numValues);
    final DataArrays daR = new DataArrays(tmpHashArrA, tmpValuesArrA, nonMatches);
    return daR;
  }

  private static class DataArrays {
    long[] hashArr;
    double[] valuesArr;
    int count;

    DataArrays(final long[] hashArr, final double[] valuesArr, final int count) {
      this.hashArr = hashArr;
      this.valuesArr = valuesArr;
      this.count = count;
    }
  }

  private static DataArrays trimDataArrays(final DataArrays da, final long thetaLong, final int numValues) {
    final long[] hashArrIn = da.hashArr;
    final double[] valuesArrIn = da.valuesArr;
    final int count = count(hashArrIn, thetaLong);
    final long[] hashArrOut = new long[count];
    final double[] valuesArrOut = new double[count * numValues];
    int haInIdx;
    int vaInIdx = 0;
    int haOutIdx = 0;
    int vaOutIdx = 0;
    for (haInIdx = 0; haInIdx < count; haInIdx++, vaInIdx += numValues) {
      final long hash = hashArrIn[haInIdx];
      if (continueCondition(thetaLong, hash)) { continue; }
      hashArrOut[haOutIdx] = hashArrIn[haInIdx];
      System.arraycopy(valuesArrIn, vaInIdx, valuesArrOut, vaOutIdx, numValues);
      haOutIdx++;
      vaOutIdx += numValues;
    }
    return new DataArrays(hashArrOut, valuesArrOut, count);
  }

  private void reset() {
    empty_ = true;
    thetaLong_ = Long.MAX_VALUE;
    keys_ = null;
    values_ = null;
    count_ = 0;
  }
}

