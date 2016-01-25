/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;

/**
 * Utility functions for computing space consumed by the MergeableQuantileSketch.
 * 
 * @author Kevin Lang
 */
public final class Space {
  
  private Space() {}
  
  /**
   * Returns a pretty print string of a table of the maximum sizes of a QuantileSketch 
   * data structure configured as a single array over a range of <i>n</i> and <i>k</i>. 
   * @param elementSizeBytes the given element size in bytes
   * @return a pretty print string of a table of the maximum sizes of a QuantileSketch
   */
  public static String spaceTableGuide(int elementSizeBytes) {
    StringBuilder sb = new StringBuilder();
    sb.append("Table Guide for Quantiles Size, Bytes:").append(LS);
    sb.append("      N : K => |");
    for (int kpow = 4; kpow <= 10; kpow++) {
      int k = 1 << kpow;
      sb.append(String.format("%,8d", k));
    }
    sb.append(LS);
    sb.append("-------------------------------------------------------------------------\n");
    for (int npow = 2; npow <= 32; npow++) {
      long n = (1L << npow) -1L;
      sb.append(String.format("%,14d |", n));
      for (int kpow = 4; kpow <= 10; kpow++) {
        int k = 1 << kpow;
        int ubSpace = Util.bufferElementCapacity(k, n);
        int ubBytes = ubSpace * elementSizeBytes;
        sb.append(String.format("%,8d", ubBytes));
      }
      sb.append(LS);
    }
    return sb.toString();
  }
  
  static void println(String s) { System.out.println(s); }
  
  /**
   * Pretty prints a table of the maximum sizes of a QuantileSketch 
   * data structure configured as a single array over a range of <i>n</i> and <i>k</i> and given
   * an element size of 8 bytes.
   * @param args Not used.
   */
  public static void main(String[] args) {
    println(spaceTableGuide(8));
    long n = (1L << 19) ;
    int k = 1024;
    int maxLevels = Util.computeNumLevelsNeeded(k, n);
    int bbCnt = (maxLevels > 0)? 2*k : Util.computeBaseBufferCount(k, n);
    int bytes = bbCnt*8 + maxLevels*k*8;
    println("K: "+k);
    println("N: "+n);
    println("bbCnt: "+bbCnt);
    println("maxLvs: "+maxLevels);
    println("bytes: "+bytes);
  }
}
