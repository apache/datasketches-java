/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

/**
 * For the Families that accept this configuration parameter, it controls the size multiple that
 * affects how fast the internal cache grows, when more space is required.
 * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
 */
public enum ResizeFactor {
  X1(0), X2(1), X4(2), X8(3);
  
  private int lg_;
  
  private ResizeFactor (int lg) {
    this.lg_  = lg;
  }
  
  /**
   * Returns the Log-base 2 of the Resize Factor
   * @return the Log-base 2 of the Resize Factor
   */
  public int lg() {
    return lg_;
  }
  
  /**
   * Returns the Resize Factor given the Log-base 2 of the Resize Factor
   * @param lg a value between zero and 3, inclusive.
   * @return the Resize Factor given the Log-base 2 of the Resize Factor
   */
  public static ResizeFactor getRF(int lg) {
    if (X1.lg() == lg) return X1;
    if (X2.lg() == lg) return X2;
    if (X4.lg() == lg) return X4;
    return X8;
  }
  
  /**
   * Returns the Resize Factor
   * @return the Resize Factor
   */
  public int getValue() {
    return 1 << lg_;
  }
}