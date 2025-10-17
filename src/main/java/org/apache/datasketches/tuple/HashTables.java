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

import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.thetacommon.HashOperations.hashInsertOnly;
import static org.apache.datasketches.thetacommon.HashOperations.hashSearch;

import java.lang.reflect.Array;

import org.apache.datasketches.theta.HashIterator;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.thetacommon.ThetaUtil;

@SuppressWarnings("unchecked")
class HashTables<S extends Summary> {
  long[] hashTable = null;
  S[] summaryTable = null;
  int lgTableSize = 0;
  int numKeys = 0;

  HashTables() { }

  //must have valid entries
  void fromSketch(final TupleSketch<S> sketch) {
    numKeys = sketch.getRetainedEntries();
    lgTableSize = getLgTableSize(numKeys);

    hashTable = new long[1 << lgTableSize];
    final TupleSketchIterator<S> it = sketch.iterator();
    while (it.next()) {
      final long hash = it.getHash();
      final int index = hashInsertOnly(hashTable, lgTableSize, hash);
      final S mySummary = (S)it.getSummary().copy();
      if (summaryTable == null) {
        summaryTable = (S[]) Array.newInstance(mySummary.getClass(), 1 << lgTableSize);
      }
      summaryTable[index] = mySummary;
    }
  }

  //must have valid entries
  void fromSketch(final ThetaSketch sketch, final S summary) {
    numKeys = sketch.getRetainedEntries(true);
    lgTableSize = getLgTableSize(numKeys);

    hashTable = new long[1 << lgTableSize];
    final HashIterator it = sketch.iterator();
    while (it.next()) {
      final long hash = it.get();
      final int index = hashInsertOnly(hashTable, lgTableSize, hash);
      final S mySummary = (S)summary.copy();
      if (summaryTable == null) {
        summaryTable = (S[]) Array.newInstance(mySummary.getClass(), 1 << lgTableSize);
      }
      summaryTable[index] = mySummary;
    }
  }

  private void fromArrays(final long[] hashArr, final S[] summaryArr, final int count) {
    numKeys = count;
    lgTableSize = getLgTableSize(count);

    summaryTable = null;
    hashTable = new long[1 << lgTableSize];
    for (int i = 0; i < count; i++) {
      final long hash = hashArr[i];
      final int index = hashInsertOnly(hashTable, lgTableSize, hash);
      final S mySummary = summaryArr[i];
      if (summaryTable == null) {
        summaryTable = (S[]) Array.newInstance(mySummary.getClass(), 1 << lgTableSize);
      }
      summaryTable[index] = summaryArr[i];
    }
  }

  //For TupleSketches
  HashTables<S> getIntersectHashTables(
      final TupleSketch<S> nextTupleSketch,
      final long thetaLong,
      final SummarySetOperations<S> summarySetOps) {

    //Match nextSketch data with local instance data, filtering by theta
    final int maxMatchSize = min(numKeys, nextTupleSketch.getRetainedEntries());
    final long[] matchHashArr = new long[maxMatchSize];
    final S[] matchSummariesArr = Util.newSummaryArray(summaryTable, maxMatchSize);
    int matchCount = 0;
    final TupleSketchIterator<S> it = nextTupleSketch.iterator();

    while (it.next()) {
      final long hash = it.getHash();
      if (hash >= thetaLong) { continue; }
      final int index = hashSearch(hashTable, lgTableSize, hash);
      if (index < 0) { continue; }
      //Copy the intersecting items from local hashTables_
      // sequentially into local matchHashArr_ and matchSummaries_
      matchHashArr[matchCount] = hash;
      matchSummariesArr[matchCount] = summarySetOps.intersection(summaryTable[index], it.getSummary());
      matchCount++;
    }
    final HashTables<S> resultHT = new HashTables<>();
    resultHT.fromArrays(matchHashArr, matchSummariesArr, matchCount);
    return resultHT;
  }

  //For ThetaSketches
  HashTables<S> getIntersectHashTables(
      final ThetaSketch nextThetaSketch,
      final long thetaLong,
      final SummarySetOperations<S> summarySetOps,
      final S summary) {

    final Class<S> summaryType = (Class<S>) summary.getClass();

    //Match nextSketch data with local instance data, filtering by theta
    final int maxMatchSize = min(numKeys, nextThetaSketch.getRetainedEntries());
    final long[] matchHashArr = new long[maxMatchSize];
    final S[] matchSummariesArr = (S[]) Array.newInstance(summaryType, maxMatchSize);
    int matchCount = 0;
    final HashIterator it = nextThetaSketch.iterator();

    //scan B & search A(hashTable) for match
    while (it.next()) {
      final long hash = it.get();
      if (hash >= thetaLong) { continue; }
      final int index = hashSearch(hashTable, lgTableSize, hash);
      if (index < 0) { continue; }
      //Copy the intersecting items from local hashTables_
      // sequentially into local matchHashArr_ and matchSummaries_
      matchHashArr[matchCount] = hash;
      matchSummariesArr[matchCount] = summarySetOps.intersection(summaryTable[index], summary);
      matchCount++;
    }
    final HashTables<S> resultHT = new HashTables<>();
    resultHT.fromArrays(matchHashArr, matchSummariesArr, matchCount);
    return resultHT;
  }

  void clear() {
    hashTable = null;
    summaryTable = null;
    lgTableSize = 0;
    numKeys = 0;
  }

  static int getLgTableSize(final int count) {
    final int tableSize = max(ceilingPowerOf2((int) ceil(count / 0.75)), 1 << ThetaUtil.MIN_LG_NOM_LONGS);
    return Integer.numberOfTrailingZeros(tableSize);
  }

}
