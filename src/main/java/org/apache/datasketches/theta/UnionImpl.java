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

import static java.lang.Math.min;
import static org.apache.datasketches.theta.PreambleUtil.UNION_THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.clearEmpty;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractUnionThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertUnionThetaLong;
import static org.apache.datasketches.thetacommon.QuickSelect.selectExcludingZeros;

import java.nio.ByteBuffer;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.HashOperations;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * Shared code for the HeapUnion and DirectUnion implementations.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class UnionImpl extends Union {

  /**
   * Although the gadget object is initially an UpdateSketch, in the context of a Union it is used
   * as a specialized buffer that happens to leverage much of the machinery of an UpdateSketch.
   * However, in this context some of the key invariants of the sketch algorithm are intentionally
   * violated as an optimization. As a result this object can not be considered as an UpdateSketch
   * and should never be exported as an UpdateSketch. It's internal state is not necessarily
   * finalized and may contain garbage. Also its internal concept of "nominal entries" or "k" can
   * be meaningless. It is private for very good reasons.
   */
  private final UpdateSketch gadget_;
  private final short expectedSeedHash_; //eliminates having to compute the seedHash on every union.
  private long unionThetaLong_; //when on-heap, this is the only copy
  private boolean unionEmpty_;  //when on-heap, this is the only copy

  private UnionImpl(final UpdateSketch gadget, final long seed) {
    gadget_ = gadget;
    expectedSeedHash_ = ThetaUtil.computeSeedHash(seed);
  }

  /**
   * Construct a new Union SetOperation on the java heap.
   * Called by SetOperationBuilder.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return instance of this sketch
   */
  static UnionImpl initNewHeapInstance(
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf) {
    final UpdateSketch gadget = //create with UNION family
        new HeapQuickSelectSketch(lgNomLongs, seed, p, rf, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = gadget.getThetaLong();
    unionImpl.unionEmpty_ = gadget.isEmpty();
    return unionImpl;
  }

  /**
   * Construct a new Direct Union in the off-heap destination Memory.
   * Called by SetOperationBuilder.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param memReqSvr a given instance of a MemoryRequestServer
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   * @return this class
   */
  static UnionImpl initNewDirectInstance(
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf,
      final MemoryRequestServer memReqSvr,
      final WritableMemory dstMem) {
    final UpdateSketch gadget = //create with UNION family
        new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, memReqSvr, dstMem, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = gadget.getThetaLong();
    unionImpl.unionEmpty_ = gadget.isEmpty();
    return unionImpl;
  }

  /**
   * Heapify a Union from a Memory Union object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory Union object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl heapifyInstance(final Memory srcMem, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = HeapQuickSelectSketch.heapifyInstance(srcMem, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcMem);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union Memory object containing data.
   * This does NO validity checking of the given Memory.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrap(final Memory srcMem, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketchR.fastReadOnlyWrap(srcMem, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcMem);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union WritableMemory object containing data.
   * This does NO validity checking of the given Memory.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrap(final WritableMemory srcMem, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketch.fastWritableWrap(srcMem, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcMem);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union Memory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(final Memory srcMem, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketchR.readOnlyWrap(srcMem, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcMem);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union WritableMemory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param expectedSeed the seed used to validate the given Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(final WritableMemory srcMem, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketch.writableWrap(srcMem, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcMem);
    return unionImpl;
  }

  @Override
  public int getCurrentBytes() {
    return gadget_.getCurrentBytes();
  }

  @Override
  public int getMaxUnionBytes() {
    final int lgK = gadget_.getLgNomLongs();
    return (16 << lgK) + (Family.UNION.getMaxPreLongs() << 3);
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    final int gadgetCurCount = gadget_.getRetainedEntries(true);
    final int k = 1 << gadget_.getLgNomLongs();
    final long[] gadgetCacheCopy =
        gadget_.hasMemory() ? gadget_.getCache() : gadget_.getCache().clone();

    //Pull back to k
    final long curGadgetThetaLong = gadget_.getThetaLong();
    final long adjGadgetThetaLong = gadgetCurCount > k
        ? selectExcludingZeros(gadgetCacheCopy, gadgetCurCount, k + 1) : curGadgetThetaLong;

    //Finalize Theta and curCount
    final long unionThetaLong = gadget_.hasMemory()
        ? gadget_.getMemory().getLong(UNION_THETA_LONG) : unionThetaLong_;

    final long minThetaLong = min(min(curGadgetThetaLong, adjGadgetThetaLong), unionThetaLong);
    final int curCountOut = minThetaLong < curGadgetThetaLong
        ? HashOperations.count(gadgetCacheCopy, minThetaLong)
        : gadgetCurCount;

    //Compact the cache
    final long[] compactCacheOut =
        CompactOperations.compactCache(gadgetCacheCopy, curCountOut, minThetaLong, dstOrdered);
    final boolean empty = gadget_.isEmpty() && unionEmpty_;
    final short seedHash = gadget_.getSeedHash();
    return CompactOperations.componentsToCompact(
        minThetaLong, curCountOut, seedHash, empty, true, dstOrdered, dstOrdered, dstMem, compactCacheOut);
  }

  @Override
  public boolean hasMemory() {
    return gadget_ instanceof DirectQuickSelectSketchR
        ? gadget_.hasMemory() : false;
  }

  @Override
  public boolean isDirect() {
    return gadget_ instanceof DirectQuickSelectSketchR
        ? gadget_.isDirect() : false;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return gadget_ instanceof DirectQuickSelectSketchR
        ? gadget_.isSameResource(that) : false;
  }

  @Override
  public void reset() {
    gadget_.reset();
    unionThetaLong_ = gadget_.getThetaLong();
    unionEmpty_ = gadget_.isEmpty();
  }

  @Override
  public byte[] toByteArray() {
    final byte[] gadgetByteArr = gadget_.toByteArray();
    final WritableMemory mem = WritableMemory.writableWrap(gadgetByteArr);
    insertUnionThetaLong(mem, unionThetaLong_);
    if (gadget_.isEmpty() != unionEmpty_) {
      clearEmpty(mem);
      unionEmpty_ = false;
    }
    return gadgetByteArr;
  }

  @Override //Stateless Union
  public CompactSketch union(final Sketch sketchA, final Sketch sketchB, final boolean dstOrdered,
      final WritableMemory dstMem) {
    reset();
    union(sketchA);
    union(sketchB);
    final CompactSketch csk = getResult(dstOrdered, dstMem);
    reset();
    return csk;
  }

  @Override
  public void union(final Sketch sketchIn) {
    //UNION Empty Rule: AND the empty states.

    if (sketchIn == null || sketchIn.isEmpty()) {
      //null and empty is interpreted as (Theta = 1.0, count = 0, empty = T).  Nothing changes
      return;
    }
    //sketchIn is valid and not empty
    ThetaUtil.checkSeedHashes(expectedSeedHash_, sketchIn.getSeedHash());
    if (sketchIn instanceof SingleItemSketch) {
      gadget_.hashUpdate(sketchIn.getCache()[0]);
      return;
    }
    Sketch.checkSketchAndMemoryFlags(sketchIn);

    unionThetaLong_ = min(min(unionThetaLong_, sketchIn.getThetaLong()), gadget_.getThetaLong()); //Theta rule
    unionEmpty_ = false;
    final boolean isOrdered = sketchIn.isOrdered();
    final HashIterator it = sketchIn.iterator();
    while (it.next()) {
      final long hash = it.get();
      if (hash < unionThetaLong_ && hash < gadget_.getThetaLong()) {
        gadget_.hashUpdate(hash); // backdoor update, hash function is bypassed
      } else {
        if (isOrdered) { break; }
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
    if (gadget_.hasMemory()) {
      final WritableMemory wmem = (WritableMemory)gadget_.getMemory();
      PreambleUtil.insertUnionThetaLong(wmem, unionThetaLong_);
      PreambleUtil.clearEmpty(wmem);
    }
  }

  @Override
  public void union(final Memory skMem) {
    if (skMem != null) {
      union(Sketch.wrap(skMem));
    }
  }

  @Override
  public void update(final long datum) {
    gadget_.update(datum);
  }

  @Override
  public void update(final double datum) {
    gadget_.update(datum);
  }

  @Override
  public void update(final String datum) {
    gadget_.update(datum);
  }

  @Override
  public void update(final byte[] data) {
    gadget_.update(data);
  }

  @Override
  public void update(final ByteBuffer data) {
    gadget_.update(data);
  }

  @Override
  public void update(final char[] data) {
    gadget_.update(data);
  }

  @Override
  public void update(final int[] data) {
    gadget_.update(data);
  }

  @Override
  public void update(final long[] data) {
    gadget_.update(data);
  }

  //Restricted

  @Override
  long[] getCache() {
    return gadget_.getCache();
  }

  @Override
  int getRetainedEntries() {
    return gadget_.getRetainedEntries(true);
  }

  @Override
  short getSeedHash() {
    return gadget_.getSeedHash();
  }

  @Override
  long getThetaLong() {
    return min(unionThetaLong_, gadget_.getThetaLong());
  }

  @Override
  boolean isEmpty() {
    return gadget_.isEmpty() && unionEmpty_;
  }

}
