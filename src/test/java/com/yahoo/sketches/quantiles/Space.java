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
    sb.append("Table Guide for QuantilesSketch Size in Bytes and Approximate Error:").append(LS);
    sb.append("          K => |");
    for (int kpow = 4; kpow <= 10; kpow++) { //the header row of k values
      int k = 1 << kpow;
      sb.append(String.format("%,8d", k));
    }
    sb.append(LS);
    sb.append("    ~ Error => |");
    for (int kpow = 4; kpow <= 10; kpow++) { //the header row of k values
      int k = 1 << kpow;
      sb.append(String.format("%7.3f%%", 100*Util.EpsilonFromK.getAdjustedEpsilon(k)));
    }
    sb.append(LS);
    sb.append("             N | Size in Bytes ->").append(LS);
    
    sb.append("------------------------------------------------------------------------").append(LS);
    for (int npow = 0; npow <= 32; npow++) {
      long n = (1L << npow) -1L;
      sb.append(String.format("%,14d |", n));
      for (int kpow = 4; kpow <= 10; kpow++) {
        int k = 1 << kpow;
        int elCap = (n == 0)? 1 : (Util.computeCombBufItemCapacity(k, n)) + 5;
        int ubBytes = elCap * elementSizeBytes;
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
  }
}
