/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.CompactSketch.createCompactSketch;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static java.lang.Math.min;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
abstract class UnionImpl extends SetOperation implements Union {
  protected static final Family MY_FAMILY = Family.UNION;
  protected final short seedHash_;
  protected final UpdateSketch gadget_;
  
  private long unionThetaLong_;
  
  UnionImpl(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    
    seedHash_ = computeSeedHash(seed);
    gadget_ = new HeapQuickSelectSketch(lgNomLongs, seed, p, rf, true);
    
    unionThetaLong_ = gadget_.getThetaLong();
  }
  
  UnionImpl(Memory srcMem, long seed) {
    
    seedHash_ = computeSeedHash(seed);
    MY_FAMILY.checkFamilyID(srcMem.getByte(FAMILY_BYTE));
    gadget_ = new HeapQuickSelectSketch(srcMem, seed);
    unionThetaLong_ = srcMem.getLong(UNION_THETA_LONG);
  }
  
  
  
  
  
}
