package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.Util.REBUILD_THRESHOLD;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_RESIZE_FACTOR_BIT;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesReadOnlyException;
import com.yahoo.sketches.Util;

class DirectQuickSelectSketchR extends UpdateSketch {
  static final double DQS_RESIZE_THRESHOLD  = 15.0 / 16.0; //tuned for space
  //These values are also in Memory and are also kept on-heap for speed.
  final int lgNomLongs_;
  final int preambleLongs_;

  final long seed_; //provided, kept only on heap, never serialized.
  final short seedHash_; //computed from seed_

  int hashTableThreshold_; //computed, kept only on heap, never serialized.

  Memory mem_; //Becomes WritableMemory, but no write methods

  //only called by DirectQuickSelectSketch
  DirectQuickSelectSketchR(final int lgNomLongs, final long seed, final int preambleLongs,
          final Memory wmem) {
    lgNomLongs_ = Math.max(lgNomLongs, MIN_LG_NOM_LONGS);
    seed_ = seed;
    seedHash_ = Util.computeSeedHash(seed_);
    preambleLongs_ = preambleLongs;
    mem_ = wmem;
  }

  //Sketch

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
  public int getRetainedEntries(final boolean valid) {
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
  public byte[] toByteArray() { //MY_FAMILY is stored in mem_
    final byte lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE);
    final int lengthBytes = (preambleLongs_ + (1 << lgArrLongs)) << 3;
    final byte[] byteArray = new byte[lengthBytes];
    final Memory mem = new NativeMemory(byteArray);
    mem_.copy(0, mem, 0, lengthBytes);
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

  //restricted methods

  @Override
  int getPreambleLongs() {
    return preambleLongs_;
  }

  @Override
  long[] getCache() {
    final long lgArrLongs = mem_.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final long[] cacheArr = new long[1 << lgArrLongs];
    final Memory mem = new NativeMemory(cacheArr);
    mem_.copy(preambleLongs_ << 3, mem, 0, 8 << lgArrLongs);
    return cacheArr;
  }

  @Override
  int getLgNomLongs() {
    return lgNomLongs_;
  }

  @Override
  Memory getMemory() {
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
