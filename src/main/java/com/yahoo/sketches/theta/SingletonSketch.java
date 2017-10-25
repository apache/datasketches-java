/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;

/**
 * @author Lee Rhodes
 */
public abstract class SingletonSketch extends CompactSketch {

public static SingletonSketch update(final long datum) {
  final long[] data = { datum };
  return null;
}




  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) {
    return 16;
  }

  @Override
  public Family getFamily() {
    return Family.COMPACT;
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return 1;
  }

  @Override
  public boolean isCompact() {
    return true;
  }

  @Override
  public abstract boolean isDirect();

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

  @Override
  public abstract byte[] toByteArray();

  //restricted methods

  @Override
  abstract long[] getCache();

  @Override
  int getCurrentPreambleLongs(final boolean compact) {
    return 1;
  }

  @Override
  abstract Memory getMemory();

  @Override
  abstract short getSeedHash();

  @Override
  long getThetaLong() {
    return Long.MAX_VALUE;
  }



}
