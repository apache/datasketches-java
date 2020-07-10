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

package org.apache.datasketches.theta;

import static org.apache.datasketches.HashOperations.convertToHashTable;
import static org.apache.datasketches.HashOperations.hashSearch;
import static org.apache.datasketches.Util.REBUILD_THRESHOLD;
import static org.apache.datasketches.Util.checkSeedHashes;
import static org.apache.datasketches.Util.simpleIntLog2;

import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Implements the A-and-not-B operations.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class AnotBimpl extends AnotB {
  private final short seedHash_;
  private boolean empty_;
  private long thetaLong_;
  private long[] hashArr_; //compact array w curCount_ entries
  private int curCount_;

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by SetOperation.Builder.
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  AnotBimpl(final long seed) {
    this(computeSeedHash(seed));
  }

  /**
   * Construct a new AnotB SetOperation on the java heap.  Called by PairwiseSetOperation.
   *
   * @param seedHash 16 bit hash of the chosen update seed.
   */
  AnotBimpl(final short seedHash) {
    seedHash_ = seedHash;
    reset();
  }

  @Override
  public void setA(final Sketch skA) {
    if (skA == null) {
      reset();
      throw new SketchesArgumentException("The input argument must not be null");
    }
    if (skA.isEmpty()) {
      reset();
      return;
    }
    //skA is not empty
    checkSeedHashes(seedHash_, skA.getSeedHash());
    empty_ = false;
    thetaLong_ = skA.getThetaLong();
    final CompactSketch cskA = (skA instanceof CompactSketch)
        ? (CompactSketch) skA
        : ((UpdateSketch) skA).compact();
    hashArr_ = skA.isDirect() ? cskA.getCache() : cskA.getCache().clone();
    curCount_ = cskA.getRetainedEntries(true);
  }

  @Override
  public void notB(final Sketch skB) {
    if (empty_ || (skB == null) || skB.isEmpty()) { return; }
    //skB is not empty
    checkSeedHashes(seedHash_, skB.getSeedHash());
    final long thetaLongB = skB.getThetaLong();
    thetaLong_ = Math.min(thetaLong_, thetaLongB);

    //Build hashtable and removes hashes of skB >= theta
    final int countB = skB.getRetainedEntries(true);
    CompactSketch cskB = null;
    UpdateSketch uskB = null;
    final long[] hashTableB;
    if (skB instanceof CompactSketch) {
      cskB = (CompactSketch) skB;
      hashTableB = convertToHashTable(cskB.getCache(), countB, thetaLong_, REBUILD_THRESHOLD);
    } else {
      uskB = (UpdateSketch) skB;
      hashTableB = (thetaLong_ < thetaLongB)
          ? convertToHashTable(uskB.getCache(), countB, thetaLong_, REBUILD_THRESHOLD)
          : uskB.getCache();
      cskB = uskB.compact();
    }

    //build temporary arrays of skA
    final long[] tmpHashArrA = new long[curCount_];

    //search for non matches and build temp arrays
    final int lgHTBLen = simpleIntLog2(hashTableB.length);
    int nonMatches = 0;
    for (int i = 0; i < curCount_; i++) {
      final long hash = hashArr_[i];
      if ((hash != 0) && (hash < thetaLong_)) { //skips hashes of A >= theta
        final int index = hashSearch(hashTableB, lgHTBLen, hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          nonMatches++;
        }
      }
    }
    hashArr_ = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    curCount_ = nonMatches;
    empty_ = (nonMatches == 0) && (thetaLong_ == Long.MAX_VALUE);
  }

  @Override
  public CompactSketch getResult(final boolean reset) {
    return getResult(true, null, reset);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem,
      final boolean reset) {
    final CompactSketch result =
        CompactOperations.componentsToCompact(
            thetaLong_, curCount_, seedHash_, empty_, true, false, dstOrdered, dstMem, hashArr_);
    if (reset) { reset(); }
    return result;
  }

  @Override
  public CompactSketch aNotB(final Sketch skA, final Sketch skB, final boolean dstOrdered,
      final WritableMemory dstMem) {
    if ((skA == null) || skA.isEmpty()) { return EmptyCompactSketch.getInstance(); }
    if ((skB == null) || skB.isEmpty()) { return skA.compact(dstOrdered, dstMem); }
    final short seedHashA = skA.getSeedHash();
    final short seedHashB = skB.getSeedHash();
    checkSeedHashes(seedHashA, seedHash_);
    checkSeedHashes(seedHashB, seedHash_);

    //Both skA & skB are not empty
    //Load skA into local tmp registers
    boolean empty = false;
    final long thetaLongA = skA.getThetaLong();
    final CompactSketch cskA = (skA instanceof CompactSketch)
        ? (CompactSketch)skA
        : ((UpdateSketch)skA).compact();
    final long[] hashArrA = cskA.getCache().clone();
    final int countA = cskA.getRetainedEntries(true);

    //Compare with skB
    final long thetaLongB = skB.getThetaLong();
    final long thetaLong = Math.min(thetaLongA, thetaLongB);
    final int countB = skB.getRetainedEntries(true);

    //Rebuild hashtable and removes hashes of skB >= thetaLong
    final long[] hashTableB = convertToHashTable(skB.getCache(), countB, thetaLong, REBUILD_THRESHOLD);

    //build temporary hash array for values from skA
    final long[] tmpHashArrA = new long[countA];

    //search for non matches and build temp hash array
    int nonMatches = 0;
    for (int i = 0; i < countA; i++) {
      final long hash = hashArrA[i];
      if ((hash != 0) && (hash < thetaLong)) { //skips hashes of A >= theta
        final int index =
            hashSearch(hashTableB, simpleIntLog2(hashTableB.length), hash);
        if (index == -1) {
          tmpHashArrA[nonMatches] = hash;
          nonMatches++;
        }
      }
    }
    //final compaction
    empty = ((nonMatches == 0) && (thetaLong == Long.MAX_VALUE));
    final long[] hashArrOut = Arrays.copyOfRange(tmpHashArrA, 0, nonMatches);
    final CompactSketch result =
        CompactOperations.componentsToCompact(
            thetaLong, nonMatches, seedHash_, empty, true, false, dstOrdered, dstMem, hashArrOut);
    return result;
  }

  @Override
  int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  boolean isEmpty() {
    return empty_;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return false;
  }

  //restricted

  private void reset() {
    thetaLong_ = Long.MAX_VALUE;
    empty_ = true;
    hashArr_ = null;
    curCount_ = 0;
  }

  @Override
  long[] getCache() {
    return null;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

  //Deprecated methods

  @Deprecated
  @Override
  public void update(final Sketch skA, final Sketch skB) {
    reset();
    setA(skA);
    notB(skB);
  }

  @Deprecated
  @Override
  public CompactSketch getResult() {
    return getResult(true, null, true);
  }

  @Deprecated
  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    return getResult(dstOrdered, dstMem, true);
  }

}
