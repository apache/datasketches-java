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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.Util;

/**
 * Computes the intersection of two or more tuple sketches of type ArrayOfDoubles.
 * A new instance represents the Universal Set.
 * Every update() computes an intersection with the internal set
 * and can only reduce the internal set.
 */
public abstract class ArrayOfDoublesIntersection {
  //not changed by resetToEmpty() or hardReset()
  private final short seedHash_;
  private final int numValues_;
  //nulled or reset by resetToEmpty
  private HashTables hashTables_;
  private boolean empty_;
  private boolean firstCall_;
  private long thetaLong_;

  /**
   * Internal constructor, called by HeapArrayOfDoublesIntersection and DirectArrayOfDoublesIntersection
   * @param numValues the number of double values in the summary array
   * @param seed the hash function update seed.
   */
  ArrayOfDoublesIntersection(final int numValues, final long seed) {
    seedHash_ = Util.computeSeedHash(seed);
    numValues_ = numValues;
    hashTables_ = null;
    empty_ = false;
    thetaLong_ = Long.MAX_VALUE;
    firstCall_ = true;
  }

  /**
   * Performs a stateful intersection of the internal set with the given tupleSketch.
   * The given tupleSketch and the internal state must have the same <i>numValues</i>.
   * @param tupleSketch Input sketch to intersect with the internal set.
   * @param combiner Method of combining two arrays of double values
   */
  public void intersect(final ArrayOfDoublesSketch tupleSketch, final ArrayOfDoublesCombiner combiner) {
    if (tupleSketch == null) { throw new SketchesArgumentException("Sketch must not be null"); }
    Util.checkSeedHashes(seedHash_, tupleSketch.getSeedHash());
    if (tupleSketch.numValues_ != numValues_) {
      throw new SketchesArgumentException(
          "Input tupleSketch cannot have different numValues from the internal numValues.");
    }

    final boolean isFirstCall = firstCall_;
    firstCall_ = false;

    //could be first or next call

    final boolean emptyIn = tupleSketch.isEmpty();
    if (empty_ || emptyIn) { //empty rule
      //Whatever the current internal state, we make our local empty.
      resetToEmpty(); //
      return;
    }

    final long thetaLongIn = tupleSketch.getThetaLong();
    thetaLong_ = min(thetaLong_, thetaLongIn); //Theta rule

    if (tupleSketch.getRetainedEntries() == 0) {
      if (hashTables_ != null) {
        hashTables_.clear();
      }
    }
    // input sketch will have valid entries > 0

    if (isFirstCall) {
      //Copy first sketch data into local instance hashTables_
      hashTables_ = new HashTables(tupleSketch);
    }

    //Next Call
    else {
      assert hashTables_ != null;
      if (hashTables_.getNumKeys() == 0) { return; }
    //process intersect with current hashTables
      hashTables_ = hashTables_.getIntersectHashTables(tupleSketch, thetaLong_, combiner);
    }
  }

  /**
   * Gets the internal set as an on-heap compact sketch.
   * @return Result of the intersections so far as a compact sketch.
   */
  public ArrayOfDoublesCompactSketch getResult() {
    return getResult(null);
  }

  /**
   * Gets the result of stateful intersections so far.
   * @param dstMem Memory for the compact sketch (can be null).
   * @return Result of the intersections so far as a compact sketch.
   */
  public ArrayOfDoublesCompactSketch getResult(final WritableMemory dstMem) {
    if (firstCall_) {
      throw new SketchesStateException(
          "getResult() with no intervening intersections is not a legal result.");
    }
    long[] hashArrOut = new long[0];
    double[] valuesArrOut = new double[0];
    if (hashTables_ != null && hashTables_.getHashTable() != null) {
      final int numKeys = hashTables_.getNumKeys();

      if (numKeys > 0) {
        final int tableSize = hashTables_.getHashTable().length;

        hashArrOut = new long[numKeys];
        valuesArrOut = new double[numKeys * numValues_];

        // & flatten the hash tables
        int cnt = 0;
        final long[] hashTable = hashTables_.getHashTable();
        final double[][] valueTable = hashTables_.getValueTable();
        for (int i = 0; i < tableSize; i++) {
          final long hash = hashTable[i];
          if (hash == 0 || hash > thetaLong_) { continue; }
          hashArrOut[cnt] = hash;
          System.arraycopy(valueTable[i], 0, valuesArrOut, cnt * numValues_, numValues_);
          cnt++;
        }
        assert cnt == numKeys;
      }
    }

    return (dstMem == null)
        ? new HeapArrayOfDoublesCompactSketch(hashArrOut, valuesArrOut,
            thetaLong_, empty_, numValues_, seedHash_)
        : new DirectArrayOfDoublesCompactSketch(hashArrOut, valuesArrOut,
            thetaLong_, empty_, numValues_, seedHash_, dstMem);
  }

  /**
   * Resets the internal set to the initial state, which represents the Universal Set
   */
  public void reset() {
    hardReset();
  }

  private void hardReset() {
    empty_ = false;
    firstCall_ = true;
    thetaLong_ = Long.MAX_VALUE;
    if (hashTables_ != null) { hashTables_.clear(); }
  }

  private void resetToEmpty() {
    empty_ = true;
    firstCall_ = false;
    thetaLong_ = Long.MAX_VALUE;
    if (hashTables_ != null) { hashTables_.clear(); }
  }

  protected abstract ArrayOfDoublesQuickSelectSketch createSketch(int nomEntries, int numValues, long seed);

}
