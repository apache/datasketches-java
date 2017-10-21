/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 */
class HeapSingletonSketch extends SingletonSketch {

  @Override
  short getSeedHash() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  public byte[] toByteArray() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isOrdered() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  long[] getCache() {
    // TODO Auto-generated method stub
    return null;
  }

}
