/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

//import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.PowerLawGenerator.Pair;


/**
 * This provides sample code that generates tables of values that can be used for plotting or 
 * other analytic computations. The first column is just the point number. The second column is
 * the generating index for the coordinate pair.  The third column represents the "X-axis" and the
 * last column represents the "Y-axis".  The first two columns are informational only.
 */
public class PowerLawGeneratorTest {
  
  //FIRST CODE EXAMPLE
  
  /**
   * Generate a table of (x,y) Pairs.<br>
   * X varies from 1 to 2^20.<br>
   * Y varies from 2^20 to 1.<br>
   * The x-axis is log-base2.<br>
   * The resolution is 2 points per power of 2.<br>
   * Skip duplicate integers for the X-coordinate.
   */
  @Test
  public void testPLG() {
    Pair start = new Pair(1, 1<<20);
    Pair end = new Pair(1<<20, 1);
    double xLogBase = 2.0;
    int ptsPerXBase = 2;
    
    PowerLawGenerator plg = new PowerLawGenerator(xLogBase, ptsPerXBase, start, end);
    int numGenIdxs = plg.getNumGenIndices();
    int startGenIdx = plg.getStartGenIndex();
    int del = plg.getDelta();
    
    println("Slope: "+plg.getSlope());
    
    Pair cur = plg.getPair(startGenIdx);
    /*
     * The iteration variable "i" guarantees that no more than numGenIdxs are output. 
     * Fewer than numGenIdxs may be output based on the user-configured skip method.
     * The iteration variable "gi" is the generating index and can subsequently increase or 
     * decrease based on whether del == 1 or -1.
     * The iteration variable "row" just counts the actual output rows for convenience.
     */
    for (int i=1, gi=startGenIdx, row=1; i<=numGenIdxs; i++, gi += del) {
      if (gi != startGenIdx) {
        Pair next = plg.getPair(gi);
        if (skip(cur, next)) continue;
        cur = next;
      }
      outputPair(row++, gi, cur);
    }
    
  }
  
  /**
   * Returns true if the next Pair should be skipped. Change this function to the desired criteria.
   * @param cur the current Pair
   * @param next the next Pair
   * @return true to skip the next pair
   */
  private static boolean skip(Pair cur, Pair next) {
    return ((int)next.x == (int)cur.x);
  }
  
  /**
   * Place-holder for a plotting or other analysis function
   * @param row the row number (for convenience)
   * @param gi generating index (for convenience)
   * @param p the point Pair
   */
  private static void outputPair(int row, int gi, Pair p) {
    int x = (int)p.x;
    int y = (int)p.y;
    println(row+"\t"+gi+"\t"+x+"\t"+y);
  }
  
  // SECOND CODE EXAMPLE
  
  
  /**
   * Generate a series of equally spaced values for a log axis using static methods.<br>
   * X varies from 1E9 to 10. (backwards)<br>
   * The x-axis is log-base10.<br>
   * The resolution is 5 points per power of 10.<br>
   * Skip duplicate integers
   */
  @Test
  public void testPLG2() {
    Pair start = new Pair(1E9, 1); //Y is not used
    Pair end   = new Pair(10, 1);  //Y is not used
    double xLogBase = 10.0;
    int ptsPerXBase = 5;
    
    int numGenIdxs = PowerLawGenerator.getNumGenIndices(start, end, xLogBase, ptsPerXBase);
    int startGenIdx = PowerLawGenerator.getStartGenIndex(start, end, xLogBase, ptsPerXBase);
    int del = PowerLawGenerator.getDelta(start, end);
    
    double cur = PowerLawGenerator.getX(startGenIdx, xLogBase, ptsPerXBase);
    /*
     * The iteration variable "i" guarantees that no more than numGenIdxs are output. 
     * Fewer than numGenIdxs may be output based on the user-configured skip method.
     * The iteration variable "gi" is the generating index and can subsequently increase or 
     * decrease based on whether del == 1 or -1.
     * The iteration variable "row" just counts the actual output rows for convenience.
     */
    for (int i=1, gi=startGenIdx, row=1; i<=numGenIdxs; i++, gi += del) {
      if (gi != startGenIdx) {
        double next = PowerLawGenerator.getX(gi, xLogBase, ptsPerXBase);
        if (skip(cur, next)) continue;
        cur = next;
      }
      outputX(row++, gi, cur);
    }
    
  }
  
  /**
   * Returns true if cur == next. Change this function to the desired criteria.
   * @param cur the current value of x
   * @param next the next value of x
   * @return true if cur == next
   */
  private static boolean skip(double cur, double next) {
    return ((int)next == (int)cur);
  }
  
  /**
   * Place-holder for a plotting or other analysis function
   * @param row the row number (for convenience)
   * @param gi generating index (for convenience)
   * @param x the value x
   */
  private static void outputX(int row, int gi, double x) {
    println(row+"\t"+gi+"\t"+((int)x));
  }
  
  /**
   * @param s value to print 
   */
  private static void println(String s) {
    System.out.println(s); //disable here
  }
  
}