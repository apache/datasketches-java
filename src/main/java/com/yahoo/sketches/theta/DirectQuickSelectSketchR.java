/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesReadOnlyException;
import com.yahoo.sketches.Util;

/**
 * The default Theta Sketch using the QuickSelect algorithm.
 * This is the read-only implementation with non-functional methods, which affect the state.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectQuickSelectSketchR extends UpdateSketch {
  static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  //These values are also in Memory image and are also kept on-heap for speed.
  final int lgNomLongs_;
  final int preambleLongs_;

  final long seed_; //provided, kept only on heap, never serialized.
  final short seedHash_; //computed from seed_

  int hashTableThreshold_; //computed, kept only on heap, never serialized.

  WritableMemory mem_; //A WritableMemory for child class, but no write methods here

  //only called by DirectQuickSelectSketch
  DirectQuickSelectSketchR(final int lgNomLongs, final long seed, final int preambleLongs,
          final WritableMemory wmem) {
    lgNomLongs_ = Math.max(lgNomLongs, MIN_LG_NOM_LONGS);
    seed_ = seed;
    seedHash_ = Util.computeSeedHash(seed_);
    preambleLongs_ = preambleLongs;
    mem_ = wmem;
  }

  /**
   * Wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR readOnlyWrap(final Memory srcMem, final long seed) {
    final Object memObj = ((WritableMemory)srcMem).getArray(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preambleLongs = extractPreLongs(memObj, memAdd);                  //byte 0
    final int lgNomLongs = extractLgNomLongs(memObj, memAdd);                   //byte 3
    final int lgArrLongs = extractLgArrLongs(memObj, memAdd);                   //byte 4

    UpdateSketch.checkUnionQuickSelectFamily(memObj, memAdd, preambleLongs, lgNomLongs);
    checkMemIntegrity(srcMem, memObj, memAdd, seed, preambleLongs, lgNomLongs, lgArrLongs);

    final DirectQuickSelectSketchR dqssr =
        new DirectQuickSelectSketchR(lgNomLongs, seed, preambleLongs, (WritableMemory) srcMem);
    dqssr.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqssr;
  }


  /**
   * Fast-wrap a sketch around the given source Memory containing sketch data that originated from
   * this sketch.  This does NO validity checking of the given Memory.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * The given Memory object must be in hash table form and not read only.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @return instance of this sketch
   */
  static DirectQuickSelectSketchR fastReadOnlyWrap(final Memory srcMem, final long seed) {
    final int preambleLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int lgNomLongs = srcMem.getByte(LG_NOM_LONGS_BYTE) & 0XFF;
    final int lgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;

    final DirectQuickSelectSketchR dqss =
        new DirectQuickSelectSketchR(lgNomLongs, seed, preambleLongs, (WritableMemory) srcMem);
    dqss.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    return dqss;
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) {
    if (!compact) {
      final byte lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE);
      final int lengthBytes = (preambleLongs_ + (1 << lgArrLongs)) << 3;
      return lengthBytes;
    }
    final int preLongs = getCurrentPreambleLongs(true);
    final int curCount = getRetainedEntries(true);

    return (preLongs + curCount) << 3;
  }

  @Override
  public Family getFamily() {
    final int familyID = mem_.getByte(FAMILY_BYTE) & 0XFF;
    return Family.idToFamily(familyID);
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return ResizeFactor.getRF(getLgRF());
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //always valid
    return mem_.getInt(RETAINED_ENTRIES_INT);
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public boolean isEmpty() {
    return (mem_.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) > 0;
  }

  @Override
  public boolean isSameResource(final Memory mem) {
    return mem_.isSameResource(mem);
  }

  @Override
  public byte[] toByteArray() { //MY_FAMILY is stored in mem_
    final byte lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE);
    final int lengthBytes = (preambleLongs_ + (1 << lgArrLongs)) << 3;
    final byte[] byteArray = new byte[lengthBytes];
    final WritableMemory mem = WritableMemory.wrap(byteArray);
    mem_.copyTo(0, mem, 0, lengthBytes);
    return byteArray;
  }

  //UpdateSketch

  @Override
  public UpdateSketch rebuild() {
    throw new SketchesReadOnlyException();
  }

  @Override
  public void reset() {
    throw new SketchesReadOnlyException();
  }

  @Override
  public int getLgNomLongs() {
    return lgNomLongs_;
  }

  //restricted methods

  @Override
  int getCurrentPreambleLongs(final boolean compact) {
    if (!compact) { return preambleLongs_; }
    return computeCompactPreLongs(getThetaLong(), isEmpty(), getRetainedEntries(true));
  }

  @Override
  long[] getCache() {
    final long lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final long[] cacheArr = new long[1 << lgArrLongs];
    final WritableMemory mem = WritableMemory.wrap(cacheArr);
    mem_.copyTo(preambleLongs_ << 3, mem, 0, 8 << lgArrLongs);
    return cacheArr;
  }

  @Override
  WritableMemory getMemory() {
    return mem_;
  }

  @Override
  float getP() {
    return mem_.getFloat(P_FLOAT);
  }

  @Override
  long getSeed() {
    return seed_;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return mem_.getLong(THETA_LONG);
  }

  @Override
  boolean isDirty() {
    return false; //Always false for QuickSelectSketch
  }

  @Override
  int getLgArrLongs() {
    return mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
  }

  @Override
  UpdateReturnState hashUpdate(final long hash) {
    throw new SketchesReadOnlyException();
  }

  int getLgRF() {
    return (mem_.getByte(PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0X3;
  }

  /**
   * Returns the cardinality limit given the current size of the hash table array.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    //FindBugs may complain if DQS_RESIZE_THRESHOLD == REBUILD_THRESHOLD, but this allows us
    // to tune these constants for different sketches.
    final double fraction = (lgArrLongs <= lgNomLongs) ? DQS_RESIZE_THRESHOLD : REBUILD_THRESHOLD;
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

}
