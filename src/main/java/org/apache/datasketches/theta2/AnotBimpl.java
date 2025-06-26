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

package org.apache.datasketches.theta2;

import static org.apache.datasketches.common.Util.exactLog2OfLong;
import static org.apache.datasketches.thetacommon2.HashOperations.checkThetaCorruption;
import static org.apache.datasketches.thetacommon2.HashOperations.continueCondition;
import static org.apache.datasketches.thetacommon2.HashOperations.hashSearch;
import static org.apache.datasketches.thetacommon2.HashOperations.hashSearchOrInsert;
import static org.apache.datasketches.thetacommon2.HashOperations.minLgHashTableSize;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon2.ThetaUtil;

/**
 * Implements the A-and-not-B operations.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class AnotBimpl extends AnotB {
  private final short seedHash_;
  private boolean empty_;
  private long thetaLong_;
  private long[] hashArr_ = new long[0]; //compact array w curCount_ entries
  private int curCount_;

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by SetOperation.Builder.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  AnotBimpl(final long seed) {
    this(ThetaUtil.computeSeedHash(seed));
  }

  /**
   * Construct a new AnotB SetOperation on the java heap.
   *
   * @param seedHash 16 bit hash of the chosen update seed.
   */
  private AnotBimpl(final short seedHash) {
    seedHash_ = seedHash;
    reset();
  }

  @Override
  public void setA(final Sketch skA) {
    if (skA == null) {
      reset();
      throw new SketchesArgumentException("The input argument <i>A</i> must not be null");
    }
    if (skA.isEmpty()) {
      reset();
      return;
    }
    //skA is not empty
    ThetaUtil.checkSeedHashes(seedHash_, skA.getSeedHash());

    //process A
    hashArr_ = getHashArrA(skA);
    empty_ = false;
    thetaLong_ = skA.getThetaLong();
    curCount_ = hashArr_.length;
  }

  @Override
  public void notB(final Sketch skB) {
    if (empty_ || skB == null || skB.isEmpty()) { return; }
    //local and skB is not empty
    ThetaUtil.checkSeedHashes(seedHash_, skB.getSeedHash());

    thetaLong_ = Math.min(thetaLong_,  skB.getThetaLong());

    //process B
    hashArr_ = getResultHashArr(thetaLong_, curCount_, hashArr_, skB);
    curCount_ = hashArr_.length;
    empty_ = curCount_ == 0 && thetaLong_ == Long.MAX_VALUE;
  }

  @Override
  public CompactSketch getResult(final boolean reset) {
    return getResult(true, null, reset);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final MemorySegment dstSeg,
      final boolean reset) {
    final CompactSketch result = CompactOperations.componentsToCompact(
      thetaLong_, curCount_, seedHash_, empty_, true, false, dstOrdered, dstSeg, hashArr_.clone());
    if (reset) { reset(); }
    return result;
  }

  @Override
  public CompactSketch aNotB(final Sketch skA, final Sketch skB, final boolean dstOrdered,
      final MemorySegment dstSeg) {
    if (skA == null || skB == null) {
      throw new SketchesArgumentException("Neither argument may be null");
    }
    //Both skA & skB are not null

    final long minThetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong());

    if (skA.isEmpty()) { return skA.compact(dstOrdered, dstSeg); }
    //A is not Empty
    ThetaUtil.checkSeedHashes(skA.getSeedHash(), seedHash_);

    if (skB.isEmpty()) {
      return skA.compact(dstOrdered, dstSeg);
    }
    ThetaUtil.checkSeedHashes(skB.getSeedHash(), seedHash_);
    //Both skA & skB are not empty

    //process A
    final long[] hashArrA = getHashArrA(skA);
    final int countA = hashArrA.length;

    //process B
    final long[] hashArrOut = getResultHashArr(minThetaLong, countA, hashArrA, skB); //out is clone
    final int countOut = hashArrOut.length;
    final boolean empty = countOut == 0 && minThetaLong == Long.MAX_VALUE;

    final CompactSketch result = CompactOperations.componentsToCompact(
          minThetaLong, countOut, seedHash_, empty, true, false, dstOrdered, dstSeg, hashArrOut);
    return result;
  }

  @Override
  int getRetainedEntries() {
    return curCount_;
  }

  //restricted

  private static long[] getHashArrA(final Sketch skA) { //returns a new array
    //Get skA cache as array
    final CompactSketch cskA = skA.compact(false, null); //sorting not required
    final long[] hashArrA = cskA.getCache().clone();
    return hashArrA;
  }

  private static long[] getResultHashArr( //returns a new array
      final long minThetaLong,
      final int countA,
      final long[] hashArrA,
      final Sketch skB) {

    // Rebuild or get hashtable of skB
    final long[] hashTableB; //read only
    if (skB instanceof CompactSketch) {
      hashTableB = convertToHashTable(skB, minThetaLong, ThetaUtil.REBUILD_THRESHOLD);
    } else {
      hashTableB = skB.getCache();
    }

    //build temporary result arrays of skA
    final long[] tmpHashArrA = new long[countA];

    //search for non matches and build temp arrays
    final int lgHTBLen = exactLog2OfLong(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if (hash != 0 && hash < minThetaLong) { //only allows hashes of A < minTheta
        final int index = hashSearch(hashTableB, lgHTBLen, hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          nonMatches++;
        }
      }
    }
    return Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
  }

  private static long[] convertToHashTable(
      final Sketch sketch,
      final long thetaLong,
      final double rebuildThreshold) {
    final int lgArrLongs = minLgHashTableSize(sketch.getRetainedEntries(true), rebuildThreshold);
    final int arrLongs = 1 << lgArrLongs;
    final long[] hashTable = new long[arrLongs];
    checkThetaCorruption(thetaLong);
    final HashIterator it = sketch.iterator();
    while (it.next()) {
      final long hash = it.get();
      if (continueCondition(thetaLong, hash) ) {
        continue;
      }
      hashSearchOrInsert(hashTable, lgArrLongs, hash);
    }
    return hashTable;
  }

  private void reset() {
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    hashArr_ = new long[0];
    curCount_ = 0;
  }

  @Override
  long[] getCache() {
    return hashArr_.clone();
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

  @Override
  public boolean hasMemorySegment() { return false; }

  @Override
  public boolean isDirect() { return false; }

  @Override
  public boolean isSameResource( final MemorySegment that) { return false; }

  @Override
  boolean isEmpty() {
    return empty_;
  }

}
