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

import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.HashOperations.hashInsertOnly;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.MIN_LG_NOM_LONGS;
import static org.apache.datasketches.Util.ceilingPowerOf2;

class HashTables {
  private long[] hashTable = null;
  private double[][] valueTable = null;
  private int numValues = 0;
  private int lgTableSize = 0;
  private int numKeys = 0;

  //Construct from sketch
  HashTables(final ArrayOfDoublesSketch sketchIn) {
    numKeys = sketchIn.getRetainedEntries();
    numValues = sketchIn.getNumValues();

    lgTableSize = getLgTableSize(numKeys);
    final int tableSize = 1 << lgTableSize;
    hashTable = new long[tableSize];
    valueTable = new double[tableSize][];
    final ArrayOfDoublesSketchIterator it = sketchIn.iterator();

    while (it.next()) {
      final long hash = it.getKey();
      final int index = hashInsertOnly(hashTable, lgTableSize, hash);
      valueTable[index] = new double[numValues];
      System.arraycopy(it.getValues(), 0, valueTable[index], 0, numValues);
    }
  }

  //Construct: Load the hash and value tables from packed hash and value arrays
  private HashTables(final long[] hashArr, final double[][] valuesArr, final int numKeys, final int numValues) {
    this.numValues = numValues;
    this.numKeys = numKeys;
    lgTableSize = getLgTableSize(numKeys);

    final int tableSize = 1 << lgTableSize;
    hashTable = new long[tableSize];
    valueTable = new double[tableSize][];

    for (int i = 0; i < numKeys; i++) {
      final long hash = hashArr[i];
      final int index = hashInsertOnly(hashTable, lgTableSize, hash);
      valueTable[index] = new double[numValues];
      System.arraycopy(valuesArr[i], 0, valueTable[index], 0, numValues);
    }
  }

  HashTables getIntersectHashTables(
      final ArrayOfDoublesSketch nextTupleSketch,
      final long thetaLong,
      final ArrayOfDoublesCombiner combiner) {
    //Match nextSketch data with local instance data, filtering by theta
    final int maxMatchSize = min(numKeys, nextTupleSketch.getRetainedEntries());
    assert numValues == nextTupleSketch.numValues_;
    final long[] matchHashArr = new long[maxMatchSize];
    final double[][] matchValuesArr = new double[maxMatchSize][];

    //Copy the intersecting items from local hashTables_
    // sequentially into local packed matchHashArr_ and matchValuesArr
    int matchCount = 0;
    final ArrayOfDoublesSketchIterator it = nextTupleSketch.iterator();
    while (it.next()) {
      final long hash = it.getKey();
      if (hash >= thetaLong) { continue; }
      final int index = hashSearch(hashTable, lgTableSize, hash);
      if (index < 0) { continue; }
      matchHashArr[matchCount] = hash;
      matchValuesArr[matchCount] = combiner.combine(valueTable[index], it.getValues());
      matchCount++;
    }
    return new HashTables(matchHashArr, matchValuesArr, matchCount, numValues);
  }

  int getNumKeys() {
    return numKeys;
  }

  int getNumValues() {
    return numValues;
  }

  long[] getHashTable() {
    return hashTable;
  }

  double[][] getValueTable() {
    return valueTable;
  }

  void clear() {
    hashTable = null;
    valueTable = null;
    numValues = 0;
    lgTableSize = 0;
    numKeys = 0;
  }

  static int getLgTableSize(final int numKeys) {
    final int tableSize = max(ceilingPowerOf2((int) ceil(numKeys / 0.75)), 1 << MIN_LG_NOM_LONGS);
    return Integer.numberOfTrailingZeros(tableSize);
  }

}
