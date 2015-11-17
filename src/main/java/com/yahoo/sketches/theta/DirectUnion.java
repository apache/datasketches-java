/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class DirectUnion extends UnionImpl {
  private final Memory unionMem_;
  
  /**
   * Construct a new Union SetOperation in off-heap Memory. Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   */
  DirectUnion(int lgNomLongs, long seed, float p, ResizeFactor rf, Memory dstMem) {
    super(new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, dstMem, true));
    unionMem_ = dstMem;
    unionMem_.putByte(FAMILY_BYTE, (byte) Family.UNION.getID());
    unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
  }
  
  /**
   * Wrap a Union around a Memory object containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  DirectUnion(Memory srcMem, long seed) {
    super(new DirectQuickSelectSketch(srcMem, seed), srcMem, seed);
    unionMem_ = srcMem;
  }
  
  @Override
  public void update(Sketch sketchIn) {
    super.update(sketchIn);
    unionMem_.putLong(UNION_THETA_LONG, unionThetaLong_);
  }
  
}