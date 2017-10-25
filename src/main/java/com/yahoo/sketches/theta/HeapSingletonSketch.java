/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class HeapSingletonSketch extends SingletonSketch {
  private byte[] arr = new byte[16];
  private static final byte[] staticArr;

  static {
    final byte[] sArr = new byte[16];
    final WritableMemory wmem = WritableMemory.wrap(sArr);
    wmem.putByte(0, (byte) 1); //preLongs
    wmem.putByte(1, (byte) 3); //serVer
    wmem.putByte(2, (byte) 3); //FamilyID
    final byte flags = (byte) (READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK  | ORDERED_FLAG_MASK);
    wmem.putByte(5, flags);
    wmem.putShort(6, computeSeedHash(DEFAULT_UPDATE_SEED));
    staticArr = sArr;
  }

  HeapSingletonSketch(final long hash) {
    arr = staticArr;
    final WritableMemory wmem = WritableMemory.wrap(arr);
    wmem.putLong(8, hash);
  }

  HeapSingletonSketch(final long hash, final long seed) {
    arr = staticArr;
    final WritableMemory wmem = WritableMemory.wrap(arr);
    wmem.putLong(8, hash);
    wmem.putShort(6, computeSeedHash(seed));
  }

  //Sketch

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public byte[] toByteArray() {
    return arr;
  }

  //restricted methods

  @Override
  long[] getCache() {
    final Memory mem = Memory.wrap(arr);
    return new long[] { mem.getLong(8) };
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    final Memory mem = Memory.wrap(arr);
    return mem.getShort(6);
  }

}
