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
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.common.QuickSelect.selectExcludingZeros;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.UNION_THETA_LONG;
import static org.apache.datasketches.theta.PreambleUtil.clearEmpty;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractUnionThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertUnionThetaLong;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.thetacommon.HashOperations;

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
    expectedSeedHash_ = Util.computeSeedHash(seed);
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
   * Construct a new Direct Union in the destination MemorySegment.
   * Called by SetOperationBuilder.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstSeg the given MemorySegment object destination. It will be cleared prior to use.
   * @return this class
   */
  static UnionImpl initNewDirectInstance(
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf,
      final MemorySegment dstSeg) {
    final UpdateSketch gadget = //create with UNION family
        new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, dstSeg, null, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = gadget.getThetaLong();
    unionImpl.unionEmpty_ = gadget.isEmpty();
    return unionImpl;
  }

  /**
   * Heapify a Union from a MemorySegment Union object containing data.
   * Called by SetOperation.
   * @param srcSeg The source MemorySegment Union object.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl heapifyInstance(final MemorySegment srcSeg, final long expectedSeed) {
    final MemorySegment srcSegRO = srcSeg.asReadOnly();
    Family.UNION.checkFamilyID(extractFamilyID(srcSegRO));
    final UpdateSketch gadget = HeapQuickSelectSketch.heapifyInstance(srcSegRO, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcSegRO);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcSegRO);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union MemorySegment object containing data.
   * This does NO validity checking of the given MemorySegment.
   * @param srcSeg The source MemorySegment object.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrapInstance(final MemorySegment srcSeg, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcSeg));
    final UpdateSketch gadget = srcSeg.isReadOnly()
        ? DirectQuickSelectSketchR.fastReadOnlyWrap(srcSeg, expectedSeed)
        : DirectQuickSelectSketch.fastWritableWrap(srcSeg, null, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcSeg);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcSeg);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union MemorySegment object containing data.
   * @param srcSeg The source MemorySegment object.
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  //Called by SetOperation and Union
  static UnionImpl wrapInstance(final MemorySegment srcSeg, final long expectedSeed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcSeg));
    final UpdateSketch gadget = srcSeg.isReadOnly()
        ? DirectQuickSelectSketchR.readOnlyWrap(srcSeg, expectedSeed)
        : DirectQuickSelectSketch.writableWrap(srcSeg, null, expectedSeed);
    final UnionImpl unionImpl = new UnionImpl(gadget, expectedSeed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcSeg);
    unionImpl.unionEmpty_ = PreambleUtil.isEmptyFlag(srcSeg);
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
  MemorySegment getMemorySegment() {
    return hasMemorySegment() ? gadget_.getMemorySegment() : null;
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final MemorySegment dstSeg) {
    final int gadgetCurCount = gadget_.getRetainedEntries(true);
    final int k = 1 << gadget_.getLgNomLongs();
    final long[] gadgetCacheCopy =
        gadget_.hasMemorySegment() ? gadget_.getCache() : gadget_.getCache().clone();

    //Pull back to k
    final long curGadgetThetaLong = gadget_.getThetaLong();
    final long adjGadgetThetaLong = gadgetCurCount > k
        ? selectExcludingZeros(gadgetCacheCopy, gadgetCurCount, k + 1) : curGadgetThetaLong;

    //Finalize Theta and curCount
    final long unionThetaLong = gadget_.hasMemorySegment()
        ? gadget_.getMemorySegment().get(JAVA_LONG_UNALIGNED, UNION_THETA_LONG)
        : unionThetaLong_;

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
        minThetaLong, curCountOut, seedHash, empty, true, dstOrdered, dstOrdered, dstSeg, compactCacheOut);
  }

  @Override
  public boolean hasMemorySegment() {
    return gadget_.hasMemorySegment();
  }

  @Override
  public boolean isOffHeap() {
    return gadget_.isOffHeap();
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return gadget_.isSameResource(that);
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
    final MemorySegment seg = MemorySegment.ofArray(gadgetByteArr);
    insertUnionThetaLong(seg, unionThetaLong_);
    if (gadget_.isEmpty() != unionEmpty_) {
      clearEmpty(seg);
      unionEmpty_ = false;
    }
    return gadgetByteArr;
  }

  @Override //Stateless Union
  public CompactSketch union(final Sketch sketchA, final Sketch sketchB, final boolean dstOrdered,
      final MemorySegment dstSeg) {
    reset();
    union(sketchA);
    union(sketchB);
    final CompactSketch csk = getResult(dstOrdered, dstSeg);
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
    Util.checkSeedHashes(expectedSeedHash_, sketchIn.getSeedHash());
    if (sketchIn instanceof SingleItemSketch) {
      gadget_.hashUpdate(sketchIn.getCache()[0]);
      return;
    }
    UnionImpl.checkSketchAndMemorySegmentFlags(sketchIn);

    unionThetaLong_ = min(min(unionThetaLong_, sketchIn.getThetaLong()), gadget_.getThetaLong()); //Theta rule
    unionEmpty_ = false;
    final boolean isOrdered = sketchIn.isOrdered();
    final HashIterator it = sketchIn.iterator();
    while (it.next()) {
      final long hash = it.get();
      if (hash < unionThetaLong_ && hash < gadget_.getThetaLong()) {
        gadget_.hashUpdate(hash); // backdoor update, hash function is bypassed
      } else if (isOrdered) { break; }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
    if (gadget_.hasMemorySegment()) {
      final MemorySegment wseg = gadget_.getMemorySegment();
      PreambleUtil.insertUnionThetaLong(wseg, unionThetaLong_);
      PreambleUtil.clearEmpty(wseg);
    }
  }

  @Override
  public void union(final MemorySegment seg) {
    Objects.requireNonNull(seg, "MemorySegment must be non-null");
    union(Sketch.wrap(seg.asReadOnly()));
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

  /**
   * Checks Ordered and Compact flags for integrity between sketch and its MemorySegment
   * @param sketch the given sketch
   */
  private static final void checkSketchAndMemorySegmentFlags(final Sketch sketch) {
    final MemorySegment seg = sketch.getMemorySegment();
    if (seg == null) { return; }
    final int flags = PreambleUtil.extractFlags(seg);
    if ((flags & COMPACT_FLAG_MASK) > 0 ^ sketch.isCompact()) {
      throw new SketchesArgumentException("Possible corruption: "
          + "MemorySegment Compact Flag inconsistent with Sketch");
    }
    if ((flags & ORDERED_FLAG_MASK) > 0 ^ sketch.isOrdered()) {
      throw new SketchesArgumentException("Possible corruption: "
          + "MemorySegment Ordered Flag inconsistent with Sketch");
    }
  }

}
