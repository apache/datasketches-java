/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.Util.LS;

/**
 * Utility functions for computing space consumed by the QuantileSketch.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
public final class Space {
  
  
  private Space() {}
  
  /**
   * Returns a pretty print string of a table of the maximum sizes of a QuantileSketch 
   * data structure configured as a single array over a range of <i>n</i> and <i>k</i>. 
   * @param lgKlo the starting value of k expressed as log_base2(k)
   * @param lgKhi the ending value of k expressed as log_base2(k)
   * @param maxLgN the ending value of N expressed as log_base2(N)
   * @return a pretty print string of a table of the maximum sizes of a QuantileSketch
   */
  public static String spaceTableGuide(int lgKlo, int lgKhi, int maxLgN) {
    int cols = lgKhi-lgKlo +1;
    int maxUBbytes = elemCapacity(1<<lgKhi, (1L<<maxLgN) -1L);
    int tblColWidth = String.format("%,d", maxUBbytes).length() +1;
    int leftColWidth = 16;
    String leftColStrFmt = "%"+leftColWidth+"s";
    String dFmt = "%,"+tblColWidth+"d";
    String fFmt = "%"+(tblColWidth-1)+".3f%%";
    StringBuilder sb = new StringBuilder();
    sb.append("Table Guide for Quantiles DoublesSketch Size in Bytes and Approximate Error:").append(LS);
    sb.append(String.format(leftColStrFmt, "K => |"));
    for (int kpow = lgKlo; kpow <= lgKhi; kpow++) { //the header row of k values
      int k = 1 << kpow;
      sb.append(String.format(dFmt, k));
    }
    sb.append(LS);
    sb.append(String.format(leftColStrFmt,"~ Error => |"));
    //sb.append("    ~ Error => |");
    for (int kpow = lgKlo; kpow <= lgKhi; kpow++) { //the header row of k values
      int k = 1 << kpow;
      sb.append(String.format(fFmt, 100*getEpsilon(k)));
    }
    sb.append(LS);
    sb.append(String.format(leftColStrFmt, "N |"));
    sb.append(" Size in Bytes ->").append(LS);
    int numDashes = leftColWidth + tblColWidth * cols;
    StringBuilder sb2 = new StringBuilder();
    for (int i=0; i<numDashes; i++) sb2.append("-");
    sb.append(sb2.toString()).append(LS);
    String leftColNumFmt = "%," + (leftColWidth-2)+"d |";
    for (int npow = 0; npow <= maxLgN; npow++) {
      long n = (1L << npow) -1L;
      sb.append(String.format(leftColNumFmt, n)); //first column
      for (int kpow = lgKlo; kpow <= lgKhi; kpow++) { //table columns
        int k = 1 << kpow;
        int ubBytes = elemCapacity(k, n);
        sb.append(String.format(dFmt, ubBytes));
      }
      sb.append(LS);
    }
    return sb.toString();
  }
  
  //External calls
  private static int elemCapacity(int k, long n) {
    return (n == 0)? 8 : (Util.computeExpandedCombinedBufferItemCapacity(k, n) + 4) * Double.BYTES;
  }
  
  private static double getEpsilon(int k) {
    return Util.EpsilonFromK.getAdjustedEpsilon(k);
  }
  
  /**
   * Pretty prints a table of the maximum sizes of a QuantileSketch 
   * data structure configured as a single array over a range of <i>n</i> and <i>k</i> and given
   * an element size of 8 bytes.
   * @param args Not used.
   */
  public static void main(String[] args) {
    println(spaceTableGuide(4, 15, 32));
  }
  
  static void println(String s) { System.out.println(s); }
}
