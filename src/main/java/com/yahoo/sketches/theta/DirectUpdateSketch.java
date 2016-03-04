/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

/**
 * @author Lee Rhodes
 */
abstract class DirectUpdateSketch extends UpdateSketch {
  private final long seed_;
  final int lgNomLongs_;
  private final float p_;
  private final ResizeFactor rf_;

  DirectUpdateSketch(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    seed_ = seed;
    lgNomLongs_ = lgNomLongs;
    p_ = p;
    rf_ = rf;
  }

  @Override
  public boolean isDirect() {
    return true; 
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

}
