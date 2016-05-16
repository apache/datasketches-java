/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.P_FLOAT;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.Util.MIN_LG_NOM_LONGS;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 */
abstract class HeapUpdateSketch extends UpdateSketch {
  final int lgNomLongs_;
  private final long seed_;
  private final float p_;
  private final ResizeFactor rf_;
  
  HeapUpdateSketch(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    lgNomLongs_ = Math.max(lgNomLongs, MIN_LG_NOM_LONGS);
    seed_ = seed;
    p_ = p;
    rf_ = rf;
  }

  //Sketch
  
  @Override
  public boolean isDirect() {
    return false; 
  }
  
  @Override
  public ResizeFactor getResizeFactor() {
    return rf_;
  }
  
  //restricted methods
  
  @Override
  int getLgNomLongs() {
    return lgNomLongs_;
  }

  @Override
  int getLgResizeFactor() {
    return rf_.lg();
  }

  @Override
  long getSeed() {
    return seed_;
  }
  
  @Override
  float getP() {
    return p_;
  }

  @Override
  short getSeedHash() {
    return Util.computeSeedHash(getSeed());
  }

  @Override
  Memory getMemory() {
    return null;
  }
  
  byte[] toByteArray(int preLongs, byte family) {
    if (isDirty()) rebuild();
    int preBytes = preLongs << 3;
    int dataBytes = getCurrentDataLongs(false) << 3;
    byte[] byteArrOut = new byte[preBytes + dataBytes];
    NativeMemory memOut = new NativeMemory(byteArrOut);
    
    //preamble
    byte byte0 = (byte) ((this.getLgResizeFactor() << 6) | preLongs);
    memOut.putByte(PREAMBLE_LONGS_BYTE, byte0);
    memOut.putByte(SER_VER_BYTE, (byte) SER_VER);
    memOut.putByte(FAMILY_BYTE, family);
    memOut.putByte(LG_NOM_LONGS_BYTE, (byte) this.getLgNomLongs());
    memOut.putByte(LG_ARR_LONGS_BYTE, (byte) this.getLgArrLongs());
    
    memOut.putShort(SEED_HASH_SHORT, this.getSeedHash());
    memOut.putInt(RETAINED_ENTRIES_INT, this.getRetainedEntries(true));
    memOut.putFloat(P_FLOAT, this.getP());
    memOut.putLong(THETA_LONG, this.getThetaLong());

    //Flags: BigEnd=0, ReadOnly=0, Empty=X, compact=0, ordered=0
    byte flags = this.isEmpty()? (byte) EMPTY_FLAG_MASK : 0;
    memOut.putByte(FLAGS_BYTE, flags);
    
    //Data
    int arrLongs = 1 << this.getLgArrLongs();
    long[] cache = this.getCache();
    memOut.putLongArray(preBytes, cache, 0, arrLongs); //load byteArrOut

    return byteArrOut;
  }
  
}
