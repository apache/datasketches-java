/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.CompactSketch.createCompactSketch;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static java.lang.Math.min;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
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
final class UnionImpl extends SetOperation implements Union {
  private UpdateSketch gadget_;
  private long unionThetaLong_;
  private short seedHash_;
  private Memory unionMem_;

  private UnionImpl(final UpdateSketch gadget, final long seed) {
    gadget_ = gadget;
    unionThetaLong_ = gadget.getThetaLong();
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
    final UpdateSketch gadget = HeapQuickSelectSketch.initNewHeapInstance(lgNomLongs, seed, p, rf, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = null;
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
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   * @return this class
   */
  static UnionImpl
      initNewDirectInstance(final int lgNomLongs, final long seed, final float p,
          final ResizeFactor rf, final Memory dstMem) {
    final UpdateSketch gadget =
        DirectQuickSelectSketch.initNewDirectInstance(lgNomLongs, seed, p, rf, dstMem, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = dstMem;
    return unionImpl;
  }

  /**
   * Heapify a Union from a Memory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl heapifyInstance(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    final UpdateSketch gadget = HeapQuickSelectSketch.heapifyInstance(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = null;
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
    Family.UNION.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    final UpdateSketch gadget = DirectQuickSelectSketch.wrapInstance(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionMem_ = srcMem;
    return unionImpl;
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final Memory dstMem) {
    final int gadgetCurCount = gadget_.getRetainedEntries(true);
    final long gadgetThetaLong = gadget_.getThetaLong();
    final int arrLongs = 1 << gadget_.getLgArrLongs();
    final int k = 1 << gadget_.getLgNomLongs();
    long[] gadgetCache = gadget_.getCache(); //if direct a copy, otherwise a reference
    long gNewThetaLong = gadgetThetaLong;

    if (gadgetCurCount > k) {
      if (!gadget_.isDirect()) {
        gadgetCache = new long[arrLongs];
        System.arraycopy(gadget_.getCache(), 0, gadgetCache, 0, arrLongs);
      }
      gNewThetaLong = selectExcludingZeros(gadgetCache, gadgetCurCount, k + 1);//messes the cache_
    }

    final long thetaLongR = min(gNewThetaLong, unionThetaLong_);

    final int curCountR = (thetaLongR < gadget_.getThetaLong())
        ? HashOperations.count(gadgetCache, thetaLongR)
        : gadgetCurCount;

    final long[] compactCacheR = compactCache(gadgetCache, curCountR, thetaLongR, dstOrdered);

    final boolean emptyR = (gadget_.isEmpty()
        && (gadget_.getP() >= thetaLongR / MAX_THETA_LONG_AS_DOUBLE) && (curCountR == 0));

    return createCompactSketch(compactCacheR, emptyR, seedHash_, curCountR, thetaLongR,
        dstOrdered, dstMem);
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Override
  public void reset() {
    gadget_.reset();
    unionThetaLong_ = gadget_.getThetaLong();
  }

  @Override
  public byte[] toByteArray() {
    final byte[] gadgetByteArr = gadget_.toByteArray();
    final Memory mem = new NativeMemory(gadgetByteArr);
    mem.putLong(UNION_THETA_LONG, unionThetaLong_); // union theta
    return gadgetByteArr;
  }

  @Override
  public Family getFamily() {
    return Family.UNION;
  }

  @Override
  public void update(final Sketch sketchIn) { //Only valid for theta Sketches using SerVer = 3
    //UNION Empty Rule: AND the empty states

    if ((sketchIn == null)  || sketchIn.isEmpty()) {
      //null/empty is interpreted as (Theta = 1.0, count = 0, empty = T).  Nothing changes
      return;
    }
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    final long thetaLongIn = sketchIn.getThetaLong();
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule with incoming
    final int curCountIn = sketchIn.getRetainedEntries(true);

    if (sketchIn.isOrdered()) { //Only true if Compact. Use early stop

      if (sketchIn.isDirect()) { //ordered, direct thus compact
        final Memory skMem = sketchIn.getMemory();
        final int preambleLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
        for (int i = 0; i < curCountIn; i++ ) {
          final int offsetBytes = (preambleLongs + i) << 3;
          final long hashIn = skMem.getLong(offsetBytes);
          if (hashIn >= unionThetaLong_) { break; } // "early stop"
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
        }
      }
      else { //sketchIn is on the Java Heap, ordered, thus compact
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
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
    if (unionMem_ != null) {
      unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
    }
  }

  @Override
  public void update(final Memory skMem) {
    //UNION Empty Rule: AND the empty states
    if (skMem == null) { return; }
    final int cap = (int)skMem.getCapacity();
    final int fam = skMem.getByte(FAMILY_BYTE);
    if (fam != 3) { //
      throw new SketchesArgumentException("Family must be COMPACT or SET_SKETCH (old): " + fam);
    }
    final int serVer = skMem.getByte(SER_VER_BYTE);
    if (serVer == 1) { //older SetSketch, which is compact and ordered
      if (cap <= 24) { return; } //empty
      processVer1(skMem);
    }
    else if (serVer == 2) { //older SetSketch, which is compact and ordered
      if (cap <= 8) { return; } //empty
      processVer2(skMem);
    }
    else if (serVer == 3) { //Only the OpenSource sketches
      if (cap <= 8) { return; } //empty
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

  //no seedhash, assumes given seed is correct. No p, no empty flag,
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
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    if (unionMem_ != null) {
      unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
    }
  }

  //has seedhash and p, could have 0 entries & theta,
  // can only be compact, ordered, size >= 8
  private void processVer2(final Memory skMem) {
    Util.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    final int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    final long thetaLongIn;
    if (preLongs == 1) {
      return;
    }
    if (preLongs == 2) {
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
    if (unionMem_ != null) {
      unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
    }
  }

  //has seedhash, p, could have 0 entries & theta,
  // could be unordered, ordered, compact, or not, size >= 8
  private void processVer3(final Memory skMem) {
    Util.checkSeedHashes(seedHash_, skMem.getShort(SEED_HASH_SHORT));
    final int preLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int curCount = skMem.getInt(RETAINED_ENTRIES_INT);
    final long thetaLongIn;
    if (preLongs == 1) {
      return;
    }
    if (preLongs == 2) { //curCount has to be > 0 and exact mode. Cannot be from intersection.
      assert curCount > 0;
      thetaLongIn = Long.MAX_VALUE;
    }
    else { //prelongs == 3, curCount may be 0 (e.g., from intersection).
      thetaLongIn = skMem.getLong(THETA_LONG);
    }
    unionThetaLong_ = min(unionThetaLong_, thetaLongIn); //Theta rule
    final boolean ordered = skMem.isAnyBitsSet(FLAGS_BYTE, (byte) ORDERED_FLAG_MASK);
    if (ordered) { //must be compact
      for (int i = 0; i < curCount; i++ ) {
        final int offsetBytes = (preLongs + i) << 3;
        final long hashIn = skMem.getLong(offsetBytes);
        if (hashIn >= unionThetaLong_) { break; } // "early stop"
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    else { //not-ordered, could be compact or hash-table form
      final boolean compact = skMem.isAnyBitsSet(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
      final int size = (compact) ? curCount : 1 << skMem.getByte(LG_ARR_LONGS_BYTE);
      for (int i = 0; i < size; i++ ) {
        final int offsetBytes = (preLongs + i) << 3;
        final long hashIn = skMem.getLong(offsetBytes);
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) { continue; }
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());
    if (unionMem_ != null) {
      unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
    }
  }

}
