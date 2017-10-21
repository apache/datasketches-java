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
public abstract class SingletonSketch extends Sketch {

  //Sketch

  @Override
  public int getRetainedEntries(final boolean valid) {
    return 1;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public Family getFamily() {
    return Family.COMPACT;
  }

  @Override
  public boolean isCompact() {
    return true;
  }

  //restricted methods

  @Override
  abstract short getSeedHash();

  @Override
  long getThetaLong() {
    return Long.MAX_VALUE;
  }

  @Override
  int getPreambleLongs() {
    return 1; //normally 2 when empty=false, need to confirm that this will work
  }

  /**
   * Gets the <a href="{@docRoot}/resources/dictionary.html#mem">Memory</a>
   * if available, otherwise returns null.
   * @return the backing Memory or null.
   */
  abstract Memory getMemory();
}
