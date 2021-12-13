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
import static org.apache.datasketches.HashOperations.convertToHashTable;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.simpleLog2OfLong;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.datasketches.HashOperations;
import org.apache.datasketches.SetOperationCornerCases;
import org.apache.datasketches.SetOperationCornerCases.AnotbAction;
import org.apache.datasketches.SetOperationCornerCases.CornerCase;
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Util;
//import org.apache.datasketches.tuple.AnotB.DataArrays;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;


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
public class HeapArrayOfDoublesAnotB2 {
  private HeapArrayOfDoublesCompactSketch cskOut = null;


  public static ArrayOfDoublesCompactSketch update(final ArrayOfDoublesSketch skA, final ArrayOfDoublesSketch skB) {
    if (skA == null || skB == null) {
      throw new SketchesArgumentException("Neither argument may be null for this stateless operation.");
    }

    final long thetaLongA = skA.getThetaLong();
    final int countA = skA.getRetainedEntries();
    final boolean emptyA = skA.isEmpty();
    final int numValues = skA.getNumValues();
    final short seedHash = skA.getSeedHash();

    final long thetaLongB = skB.getThetaLong();
    final int countB = skB.getRetainedEntries();
    final boolean emptyB = skB.isEmpty();
    assert numValues == skB.getNumValues() : "Inputs cannot have different numValues";
    assert seedHash == skB.getSeedHash() : "Inputs cannot have different seedHashes";

    final int id =
        SetOperationCornerCases.createCornerCaseId(thetaLongA, countA, emptyA, thetaLongB, countB, emptyB);
    final CornerCase cCase = CornerCase.caseIdToCornerCase(id);
    final AnotbAction anotbAction = cCase.getAnotbAction();

    ArrayOfDoublesCompactSketch result = null;
    final long thetaLong = min(thetaLongA, thetaLongB);

    switch (anotbAction) {
      case EMPTY_1_0_T: {
        result = new HeapArrayOfDoublesCompactSketch(null, null, Long.MAX_VALUE, true, numValues, seedHash);
        break;
      }
      case DEGEN_MIN_0_F: {
        result = new HeapArrayOfDoublesCompactSketch(null, null, thetaLong, false, numValues, seedHash);
        break;
      }
      case DEGEN_THA_0_F: {
        result = new HeapArrayOfDoublesCompactSketch(null, null, thetaLongA, false, numValues, seedHash);
        break;
      }
      case TRIM_A: {
//        final DataArrays daA = getCopyOfDataArraysTuple(skA);
//        final long[] hashArr = daA.hashArr;
//        final double[] valuesArr summaryArrA = daA.summaryArr;
//        final long minThetaLong =  min(thetaLongA, thetaLongB);
//        final DataArrays da = trimAndCopyDataArrays(hashArrA, summaryArrA, minThetaLong, false);
//        result = new CompactSketch<>(da.hashArr, da.summaryArr, minThetaLong, skA.empty_);
        break;
      }
      case SKETCH_A: {
        result = skA.compact();
        break;
      }
      case FULL_ANOTB: { //both A and B should have valid entries.
//        final DataArrays daA = getCopyOfDataArraysTuple(skA);
//        final long minThetaLong = min(thetaLongA, thetaLongB);
//        final DataArrays daR =
//            getCopyOfResultArraysTuple(minThetaLong, daA.hashArr.length, daA.hashArr, daA.summaryArr, skB);
//        final int countR = (daR.hashArr == null) ? 0 : daR.hashArr.length;
//        if (countR == 0) {
//          result = new CompactSketch<>(null, null, minThetaLong, minThetaLong == Long.MAX_VALUE);
//        } else {
//          result = new CompactSketch<>(daR.hashArr, daR.summaryArr, minThetaLong, false);
//        }
      }
      //default: not possible
    }
    return null; //result;
  }

  private static class DataArrays {
    DataArrays() {}

    long[] hashArr;
    double[] valuesArr;
  }

  private static DataArrays getCopyOfDataArrays(ArrayOfDoublesSketch sk) {
    final ArrayOfDoublesCompactSketch csk;
    final DataArrays da = new DataArrays();
    if (sk instanceof HeapArrayOfDoublesCompactSketch) {
      csk = (HeapArrayOfDoublesCompactSketch) sk;
    } else {
      csk = ((HeapArrayOfDoublesQuickSelectSketch) sk).compact();
    }
    final int count = csk.getRetainedEntries();
    da.hashArr = null;
    da.valuesArr = null;
    if (count > 0) {
//      final ArrayOfDoublesSketchIterator it = sk.iterator();
//      while (it.next()) {
//        if (it.getKey() < theta_) {
//          final int index = HashOperations.hashSearch(hashTable, lgHashTableSize, it.getKey());
//          if (index == -1) {
//            keys_[count_] = it.getKey();
//            System.arraycopy(it.getValues(), 0, values_, count_ * numValues_, numValues_);
//            count_++;
//          }
//        }
//      }
    }
    return da;
  }


}

