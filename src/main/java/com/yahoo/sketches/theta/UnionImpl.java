/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.clearEmpty;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractUnionThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.insertUnionThetaLong;
import static java.lang.Math.min;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

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
  private final short seedHash_; //eliminates having to compute the seedHash on every update.
  private long unionThetaLong_; //when on-heap, this is the only copy
  private boolean unionEmpty_;  //when on-heap, this is the only copy

  private UnionImpl(final UpdateSketch gadget, final long seed) {
    gadget_ = gadget;
    seedHash_ = computeSeedHash(seed);
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
  static UnionImpl initNewHeapInstance(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf) {
    final UpdateSketch gadget = new HeapQuickSelectSketch(
        lgNomLongs, seed, p, rf, true); //create with UNION family
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
    final UpdateSketch gadget = new DirectQuickSelectSketch(
        lgNomLongs, seed, p, rf, memReqSvr, dstMem, true); //create with UNION family
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
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl heapifyInstance(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = HeapQuickSelectSketch.heapifyInstance(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union Memory object containing data.
   * This does NO validity checking of the given Memory.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrap(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketchR.fastReadOnlyWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union Memory object containing data.
   * This does NO validity checking of the given Memory.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrap(final WritableMemory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketch.fastWritableWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union Memory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketchR.readOnlyWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union Memory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(final WritableMemory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketch.writableWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
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
        (gadget_.hasMemory()) ? gadget_.getCache() : gadget_.getCache().clone();

    //Pull back to k
    final long curGadgetThetaLong = gadget_.getThetaLong();
    final long adjGadgetThetaLong = (gadgetCurCount > k)
        ? selectExcludingZeros(gadgetCacheCopy, gadgetCurCount, k + 1) : curGadgetThetaLong;

    //Finalize Theta and curCount
    final long unionThetaLong = (gadget_.hasMemory())
        ? gadget_.getMemory().getLong(UNION_THETA_LONG) : unionThetaLong_;

    final long minThetaLong = min(min(curGadgetThetaLong, adjGadgetThetaLong), unionThetaLong);
    final int curCountOut = (minThetaLong < curGadgetThetaLong)
        ? HashOperations.count(gadgetCacheCopy, minThetaLong)
        : gadgetCurCount;

    //Compact the cache
    final long[] compactCacheOut =
        compactCache(gadgetCacheCopy, curCountOut, minThetaLong, dstOrdered);
    final boolean empty = gadget_.isEmpty() && unionEmpty_;
    return createCompactSketch(
        compactCacheOut, empty, seedHash_, curCountOut, minThetaLong, dstOrdered, dstMem);
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
    final WritableMemory mem = WritableMemory.wrap(gadgetByteArr);
    insertUnionThetaLong(mem, unionThetaLong_);
    if (gadget_.isEmpty() != unionEmpty_) {
      clearEmpty(mem);
      unionEmpty_ = false;
    }
    return gadgetByteArr;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return (gadget_ instanceof DirectQuickSelectSketchR)
        ? gadget_.getMemory().isSameResource(that) : false;
  }

  @Override
  public void update(final Sketch sketchIn) { //Only valid for theta Sketches using SerVer = 3
    //UNION Empty Rule: AND the empty states.

    if ((sketchIn == null) || sketchIn.isEmpty()) {
      //null and empty is interpreted as (Theta = 1.0, count = 0, empty = T).  Nothing changes
      return;
    }
    //sketchIn is valid and not empty
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    Sketch.checkSketchAndMemoryFlags(sketchIn);

    unionThetaLong_ = min(unionThetaLong_, sketchIn.getThetaLong()); //Theta rule
    unionEmpty_ = unionEmpty_ && sketchIn.isEmpty();
    final int curCountIn = sketchIn.getRetainedEntries(true);
    if (curCountIn > 0) {
      if (sketchIn.isOrdered()) { //Only true if Compact. Use early stop
        //Ordered, thus compact
        if (sketchIn.hasMemory()) {
          final Memory skMem = ((CompactSketch) sketchIn).getMemory();
          final int preambleLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
          for (int i = 0; i < curCountIn; i++ ) {
            final int offsetBytes = (preambleLongs + i) << 3;
            final long hashIn = skMem.getLong(offsetBytes);
            if (hashIn >= unionThetaLong_) { break; } // "early stop"
            gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
          }
        }
        else { //sketchIn is on the Java Heap or has array
          final long[] cacheIn = sketchIn.getCache(); //not a copy!
          for (int i = 0; i < curCountIn; i++ ) {
            final long hashIn = cacheIn[i];
            if (hashIn >= unionThetaLong_) { break; } // "early stop"
            gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
          }
        }
      } //End ordered, compact
      else { //either not-ordered compact or Hash Table form. A HT may have dirty values.
        final long[] cacheIn = sketchIn.getCache(); //if off-heap this will be a copy
        final int arrLongs = cacheIn.length;
        for (int i = 0, c = 0; (i < arrLongs) && (c < curCountIn); i++ ) {
          final long hashIn = cacheIn[i];
          if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) { continue; } //rejects dirty values
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
          c++; //insures against invalid state inside the incoming sketch
        }
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
  }

  @Override
  public void update(final Memory skMem) {
    //UNION Empty Rule: AND the empty states
    if (skMem == null) { return; }
    final int cap = (int)skMem.getCapacity();
    final int fam = skMem.getByte(FAMILY_BYTE);
    final int serVer = skMem.getByte(SER_VER_BYTE);
    if (serVer == 1) { //very old SetSketch, which is compact and ordered
      if (fam != 3) { //the original SetSketch
        throw new SketchesArgumentException(
            "Family must be old SET_SKETCH: " + Family.idToFamily(fam));
      }
      if (cap <= 24) { return; } //empty
      processVer1(skMem);
    }
    else if (serVer == 2) { //older SetSketch, which is compact and ordered
      if (fam != 3) { //the original SetSketch
        throw new SketchesArgumentException(
            "Family must be old SET_SKETCH: " + Family.idToFamily(fam));
      }
      if (cap <= 8) { return; } //empty
      processVer2(skMem);
    }
    else if (serVer == 3) { //The OpenSource sketches
      if ((fam < 1) || (fam > 3)) {
        throw new SketchesArgumentException(
            "Family must be Alpha, QuickSelect, or Compact: " + Family.idToFamily(fam));
      }
      if (cap <= 8) { return; } //empty and Theta = 1.0
      processVer3(skMem);
    }
    else {
      throw new SketchesArgumentException("SerVer is unknown: " + serVer);
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
  int getRetainedEntries(final boolean valid) {
    return gadget_.getRetainedEntries(valid);
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

  //no seedHash, assumes given seed is correct. No p, no empty flag, no concept of direct
  // can only be compact, ordered, size > 24
  private void processVer1(final Memory skMem) {
    final long thetaLongIn = skMem.getLong(THETA_LONG);
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    final int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    final int preLongs = 3;
    for (int i = 0; i < curCount; i++ ) {
      final int offsetBytes = (preLongs + i) << 3;
      final long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) { break; } // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule
    unionEmpty_ = unionEmpty_ && gadget_.isEmpty();
  }

  //has seedHash and p, could have 0 entries & theta,
  // can only be compact, ordered, size >= 8
  private void processVer2(final Memory skMem) {
    Util.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    final int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    final long thetaLongIn;
    if (preLongs == 1) { //does not change anything {1.0, 0, T}
      return;
    }
    if (preLongs == 2) { //exact mode
      assert curCount > 0;
      thetaLongIn = Long.MAX_VALUE;
    } else { //prelongs == 3, curCount may be 0 (e.g., from intersection)
      thetaLongIn = skMem.getLong(THETA_LONG);
    }
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    for (int i = 0; i < curCount; i++ ) {
      final int offsetBytes = (preLongs + i) << 3;
      final long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) { break; } // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    unionEmpty_ = unionEmpty_ && gadget_.isEmpty();
  }

  //has seedHash, p, could have 0 entries & theta,
  // could be unordered, ordered, compact, or not, size >= 8
  private void processVer3(final Memory skMem) {
    Util.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    final int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int curCount;
    final long thetaLongIn;
    if (preLongs == 1) { //SingleItemSketch if not empty, Read-Only, Compact and Ordered
      final int flags = skMem.getByte(FLAGS_BYTE);
      if (flags == (READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | ORDERED_FLAG_MASK)) {
        curCount = 1;
        thetaLongIn = Long.MAX_VALUE;
      } else {
        return; //otherwise an empty sketch {1.0, 0, T}
      }
    }
    else if (preLongs == 2) { //curCount has to be > 0 and exact mode. Cannot be from intersection.
      curCount = skMem.getInt(RETAINED_ENTRIES_INT);
      assert curCount > 0;
      thetaLongIn = Long.MAX_VALUE;
    }
    else { //prelongs == 3, curCount may be 0 (e.g., from intersection).
      curCount = skMem.getInt(RETAINED_ENTRIES_INT);
      assert curCount > 0;
      thetaLongIn = skMem.getLong(THETA_LONG);
    }
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //theta rule
    final boolean ordered = (skMem.getByte(FLAGS_BYTE) & ORDERED_FLAG_MASK) != 0;
    if (ordered) { //must be compact
      for (int i = 0; i < curCount; i++ ) {
        final int offsetBytes = (preLongs + i) << 3;
        final long hashIn = skMem.getLong(offsetBytes);
        if (hashIn >= unionThetaLong_) { break; } // "early stop"
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    else { //not-ordered, could be compact or hash-table form
      final boolean compact = (skMem.getByte(FLAGS_BYTE) & COMPACT_FLAG_MASK) != 0;
      final int size = (compact) ? curCount : 1 << skMem.getByte(LG_ARR_LONGS_BYTE);
      for (int i = 0; i < size; i++ ) {
        final int offsetBytes = (preLongs + i) << 3;
        final long hashIn = skMem.getLong(offsetBytes);
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) { continue; }
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //sync thetaLongs
    unionEmpty_ = unionEmpty_ && gadget_.isEmpty();
  }

}
