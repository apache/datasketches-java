/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches;

import static java.lang.Math.*;

/**
 * A simple tool to generate (x,y) pairs of points that follow a power-law relationship.
 * This class provides both a convenient dynamic class with simplified get methods or the 
 * equalivant static calls with full arguments. The basic static methods can also be used to 
 * generate equally spaced points for a single axis.
 * 
 * <p>The equal spacing is created using "generating indices" that are first obtained from 
 * these methods and then used to generate the x-coordiantes or full (x,y) Pairs.  Please refer
 * to example code in the associated test class.
 * </p>
 * 
 * @author Lee Rhodes
 */
public class PowerLawGenerator {
  
  /**
   * A simple x,y coordinate pair of values
   */
  public static class Pair {
    public double x;
    public double y;
    public Pair (double x, double y) {
      this.x = x;
      this.y = y;
    }
  }
  //Inputs
  private int ppxb_;
  private double xLogBase_;
  private Pair start_;
  private Pair end_;
  
  //Derived
  private int iStart_;
  private int iEnd_;
  private int numIdxs_;
  private double slope_;
  private int delta_;
  
  /**
   * Creates a convenient dynamic class used for generating (x,y) Pairs for a power-law relation. 
   * This creates the intermediate mathematical values from the given parameters will allow
   * simplified gets with few or no parameters.
   * 
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced points per 
   * power of the x logarithmic base.  For example, if x is log-base2 specifying 4 would
   * result in 4 equally spaced points for every power of 2 of x.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   */
  public PowerLawGenerator(double xLogBase, int ptsPerXBase, Pair start, Pair end) {
    xLogBase_ = xLogBase;
    ppxb_ = ptsPerXBase;
    start_ = start;
    end_ = end;
    
    slope_ = getSlope(start_, end_);
    iStart_ = getStartGenIndex(start_, end_, xLogBase_, ppxb_);
    iEnd_ = getEndGenIndex(start_, end_, xLogBase_, ppxb_);
    delta_ = getDelta(start_, end_);
    numIdxs_ = getNumGenIndices(start_, end_, xLogBase_, ppxb_);
  }
  
  /**
   * The total number of generating indices available given the start and end Pairs.
   * @return the number of generating indices.
   */
  public int getNumGenIndices() {
    return numIdxs_;
  }
  
  /**
   * Returns the generating index delta value. If start.x &lt; end.x then this returns 1, else -1.
   * @return the generating index delta value
   */
  public int getDelta() {
    return delta_;
  }
  
  /**
   * Returns the power-law slope derived from the start and end Pairs.
   * @return the power-law slope derived from the start and end Pairs.
   */
  public double getSlope() {
    return slope_;
  }
  
  /**
   * Returns the starting generating index used to generate the start Point.
   * @return the starting generating index used to obtain the start Point.
   */
  public int getStartGenIndex() {
    return iStart_;
  }
  
  /**
   * Returns the ending generating index used to generate the end Point.
   * @return the ending generating index used to obtain the end Point.
   */
  public int getEndGenIndex() {
    return iEnd_;
  }
  
  /**
   * Returns a coordinate Pair based on the given generating index.
   * @param genIndex the given generating index
   * @return a coordinate Pair based on the given generating index.
   */
  public Pair getPair(int genIndex) {
    double x = getX(genIndex, xLogBase_, ppxb_);
    double y = getY(start_, slope_, x);
    return new Pair(x, y);
  }
  
  //Static method equivalents
  
  /**
   * The total number of generating indices available given the start and end Pairs.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced points per 
   * power of the x logarithmic base.  For example, if x is log-base2 specifying 4 would
   * result in 4 equally spaced points for every power of 2 of x.
   * @return The total number of generating indices available given the start and end Pairs.
   */
  public static int getNumGenIndices(Pair start, Pair end, double xLogBase, int ptsPerXBase) {
    int iStrt = getStartGenIndex(start, end, xLogBase, ptsPerXBase);
    int iEnd =  getEndGenIndex(start, end, xLogBase, ptsPerXBase);
    return abs(iStrt - iEnd) +1;
  }
  
  /**
   * Returns the generating index delta value. If start.x &lt; end.x then this returns 1, else -1.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @return the generating index delta value
   */
  public static int getDelta(Pair start, Pair end) {
    boolean xIncreasing = end.x > start.x;
    return (xIncreasing)? 1 : -1;
  }
  
  /**
   * Returns the power-law slope derived from the start and end Pairs.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @return the power-law slope derived from the start and end Pairs
   */
  public static double getSlope(Pair start, Pair end) {
    return log(start.y/end.y)/log(start.x/end.x);
  }
  
  /**
   * Returns the starting generating index used to generate the start Point.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced points per 
   * power of the x logarithmic base.  For example, if x is log-base2 specifying 4 would
   * result in 4 equally spaced points for every power of 2 of x.
   * @return the starting generating index used to generate the start Point
   */
  public static int getStartGenIndex(Pair start, Pair end, double xLogBase, int ptsPerXBase) {
    double lnXbase = log(xLogBase);
    boolean xIncreasing = end.x > start.x;
    double iStartDbl = ptsPerXBase * log(start.x)/lnXbase;
    return (int) ((xIncreasing)? Math.floor(iStartDbl) : Math.ceil(iStartDbl));
  }
  
  /**
   * Returns the ending generating index used to generate the end Point.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced points per 
   * power of the x logarithmic base.  For example, if x is log-base2 specifying 4 would
   * result in 4 equally spaced points for every power of 2 of x.
   * @return the ending generating index used to generate the end Point
   */
  public static int getEndGenIndex(Pair start, Pair end, double xLogBase, int ptsPerXBase) {
    double lnXbase = log(xLogBase);
    boolean xIncreasing = end.x > start.x;
    double iEndDbl = ptsPerXBase * log(end.x)/lnXbase;
    return (int) ((!xIncreasing)? Math.floor(iEndDbl) : Math.ceil(iEndDbl));
  }
  
  /**
   * Returns the x-coordinate given the generating index.
   * @param genIndex the generating index
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced points per 
   * power of the x logarithmic base.  For example, if x is log-base2 specifying 4 would
   * result in 4 equally spaced points for every power of 2 of x.
   * @return the x-coordinate based on the generating index
   */
  public static double getX(int genIndex, double xLogBase, int ptsPerXBase) {
    return Math.pow(xLogBase, ((double)genIndex)/ptsPerXBase);
  }
  
  /**
   * Returns the y-coordinate given the computed x-coordinate and slope.
   * @param start the desired starting (x,y) Pair
   * @param slope the log-log slope
   * @param x the computed x-coordinate
   * @return the y-coordinate
   */
  public static double getY(Pair start, double slope, double x) {
    return start.y * Math.exp(slope * log(x/start.x));
  }
  
}
