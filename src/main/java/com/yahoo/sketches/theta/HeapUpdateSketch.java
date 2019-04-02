/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertLgResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertThetaLong;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

/**
 * The parent class for Heap Updatable Theta Sketches.
 *
 * @author Lee Rhodes
 */
abstract class HeapUpdateSketch extends UpdateSketch {
  final int lgNomLongs_;
  private final long seed_;
  private final float p_;
  private final ResizeFactor rf_;

  HeapUpdateSketch(final int lgNomLongs, final long seed, final float p, final ResizeFactor rf) {
    lgNomLongs_ = Math.max(lgNomLongs, MIN_LG_NOM_LONGS);
    seed_ = seed;
    p_ = p;
    rf_ = rf;
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) {
    final int preLongs = getCurrentPreambleLongs(compact);
    final int dataLongs = getCurrentDataLongs(compact);
    return (preLongs + dataLongs) << 3;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean hasMemory() {
    return false;
  }

  //UpdateSketch

  @Override
  public int getLgNomLongs() {
    return lgNomLongs_;
  }

  @Override
  public ResizeFactor getResizeFactor() {
    return rf_;
  }

  //restricted methods

  @Override
  float getP() {
    return p_;
  }

  @Override
  long getSeed() {
    return seed_;
  }

  @Override
  short getSeedHash() {
    return Util.computeSeedHash(getSeed());
  }

  byte[] toByteArray(final int preLongs, final byte familyID) {
    if (isDirty()) { rebuild(); }
    final int preBytes = (preLongs << 3) & 0X3F;
    final int dataBytes = getCurrentDataLongs(false) << 3;
    final byte[] byteArrOut = new byte[preBytes + dataBytes];
    final WritableMemory memOut = WritableMemory.wrap(byteArrOut);

    //preamble first 8 bytes. Note: only compact can be reduced to 8 bytes.
    final int lgRf = getResizeFactor().lg() & 0x3;
    insertPreLongs(memOut, preLongs);
    insertLgResizeFactor(memOut, lgRf);
    insertSerVer(memOut, SER_VER);
    insertFamilyID(memOut, familyID);
    insertLgNomLongs(memOut, getLgNomLongs());
    insertLgArrLongs(memOut, getLgArrLongs());
    insertSeedHash(memOut, getSeedHash());

    insertCurCount(memOut, this.getRetainedEntries(true));
    insertP(memOut, getP());
    insertThetaLong(memOut, getThetaLong());

    //Flags: BigEnd=0, ReadOnly=0, Empty=X, compact=0, ordered=0
    final byte flags = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    insertFlags(memOut, flags);

    //Data
    final int arrLongs = 1 << getLgArrLongs();
    final long[] cache = getCache();
    memOut.putLongArray(preBytes, cache, 0, arrLongs); //load byteArrOut

    return byteArrOut;
  }

}
