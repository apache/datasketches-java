/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.performance;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * Holds key metrics from a single trial
 * 
 * @author Lee Rhodes
 */
public class Stats implements Comparable<Stats> {
  double estimate; //The estimate from the sketch
  double re = 0;   //Relative Error. Will sort by this
  double lb2del;   //LowerBound delta at -2 StdDev
  double lb1del;   //LowerBound delta at -1 StdDev
  double ub1del;   //UpperBound delta at +1 StdDev
  double ub2del;   //UpperBound delta at +2 StdDev
  double updateTimePerU_nS;
  
  /**
   * Update this Stats with a theta UpdateSketch
   * @param sketch the sketch to update with
   * @param uPerTrial the number of uniques fed to the sketch in this trial
   * @param updateTime_nS the update time requred for all the updates in nanoSeconds. 
   */
  public void update(UpdateSketch sketch, int uPerTrial, long updateTime_nS) {
    estimate = sketch.getEstimate();
    re = estimate/uPerTrial - 1.0;
    lb2del = sketch.getLowerBound(2) - estimate;
    lb1del = sketch.getLowerBound(1) - estimate;
    ub1del = sketch.getUpperBound(1) - estimate;
    ub2del = sketch.getUpperBound(2) - estimate;
    updateTimePerU_nS = (double)updateTime_nS / uPerTrial;
  }
  
  /**
   * Update this Stats with an HLL Sketch
   * @param sketch the sketch to update with
   * @param uPerTrial the number of uniques fed to the sketch in this trial
   * @param updateTime_nS the update time requred for all the updates in nanoSeconds. 
   */
  public void update(HllSketch sketch, int uPerTrial, long updateTime_nS) {
    estimate = sketch.getEstimate();
    re = estimate/uPerTrial - 1.0;
    lb2del = sketch.getLowerBound(2) - estimate;
    lb1del = sketch.getLowerBound(1) - estimate;
    ub1del = sketch.getUpperBound(1) - estimate;
    ub2del = sketch.getUpperBound(2) - estimate;
    updateTimePerU_nS = (double)updateTime_nS / uPerTrial;
  }
  
  @Override
  public int compareTo(Stats that) {
      return (this.re < that.re)? -1 : (this.re > that.re)? 1 : 0;
  }
}