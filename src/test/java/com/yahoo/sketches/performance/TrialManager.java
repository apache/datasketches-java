/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.performance;

import static java.lang.Math.log;
import static java.lang.Math.pow;

import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.HllSketchBuilder;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

/**
 * Manages the execution of every trial.  One of these for the entire process.
 * 
 * @author Lee Rhodes
 */
public class TrialManager {
  private static final double LN2 = log(2.0);
  private UpdateSketch udSketch_ = null;
  private HllSketchBuilder hllBuilder_ = null;
  //Global counter that increments for every new unique value. 
  //Assures that all sketches are virtually independent.
  private long vIn_;
  private int lgBP_; //The break point
  private int lgMinTrials_;
  private int lgMaxTrials_;
  private int lgMaxU_;
  private int ppo_;
  private double slope_;
  
  /**
   * Sets the theta UpdateSketch builder used to create the theta UpdateSketches.
   * @param udBldr the theta UpdateSketchBuilder
   * @param direct true if direct (off heap) mode is desired.  Instead of actual off heap memory
   * this will emulate that behavior by using an on-heap byte array accessed by the Memory package.
   * Performance-wise it is the same except for issues of garbage collection, which is not the
   * purpose of this test.
   */
  public void setUpdateSketchBuilder(UpdateSketchBuilder udBldr,  boolean direct) {
    int lgK = udBldr.getLgNominalEntries();
    lgBP_ = lgK + 1; //set the break point where the #trials starts to decrease.
    Memory mem = null;
    if (direct) {
      int bytes = Sketch.getMaxUpdateSketchBytes(1 << lgK);
      byte[] memArr = new byte[bytes];
      mem = new NativeMemory(memArr);
      udBldr.initMemory(mem);
    }
    udSketch_ = udBldr.initMemory(mem).build(1 << lgK);
  }
  
  /**
   * Sets the HLL builder used to create the HLL sketches.
   * @param hllBldr the HllSketchBuilder
   */
  public void setHllSketchBuilder(HllSketchBuilder hllBldr) {
    udSketch_ = null;
    hllBuilder_ = hllBldr;
  }
  
  /**
   * This sets the profile for how the number of trials vary with the number of uniques.
   * The number of trials is the maximum until the number of uniques exceeds k, whereby
   * the number of trials starts to decrease in a power-law fashion until the minimum
   * number of trials is reached at the maximum number of uniques to be tested.
   * @param lgMinTrials The minimum number of trials in a trial set specified as the 
   * exponent of 2.  This will occur at the maximum uniques value.
   * @param lgMaxTrials The maximum number of trials in a trial set specified as the 
   * exponent of 2. 
   * @param lgMaxU The maximum number of uniques for this entire test specified as the
   * exponent of 2. The first trail set starts at uniques (u = 1).
   * @param ppo  The number of Points Per Octave along the unique value number line
   * that will be used for generating trial sets. Recommended values are one point per octave
   * to 16 points per octave.
   */
  public void setTrialsProfile(int lgMinTrials, int lgMaxTrials, int lgMaxU, int ppo) {
    lgMinTrials_ = lgMinTrials;
    lgMaxTrials_ = lgMaxTrials;
    lgMaxU_ = lgMaxU;
    ppo_ = ppo;
    slope_ = (double)(lgMaxTrials - lgMinTrials) / (lgBP_ - lgMaxU_);
  }
  
  /**
   * Create (or reset) a sketch and perform uPerTrial updates then update the given Stats.
   * @param stats The given Stats object
   * @param uPerTrial the number of updates for this trial.
   */
  public void doTrial(Stats stats, int uPerTrial) {
    if (udSketch_ != null) { //UpdateSketch
      udSketch_.reset(); //reuse the same sketch
      long startUpdateTime_nS = System.nanoTime();
      for (int u=uPerTrial; u--> 0; ) udSketch_.update(vIn_++);
      //udSketch_.rebuild(); //Optional. Resizes down to k. Only useful with QuickSelectSketch 
      long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
      stats.update(udSketch_, uPerTrial, updateTime_nS);
    }
    else { //HllSketch
      HllSketch hllSketch = hllBuilder_.build();
      long startUpdateTime_nS = System.nanoTime();
      for (int u=uPerTrial; u--> 0; ) hllSketch.update(new long[]{vIn_++});
      long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
      stats.update(hllSketch, uPerTrial, updateTime_nS);
    }
  }
  
  /**
   * Computes the number of trials for a given current number of uniques for a trial set.
   * @param curU the given current number of uniques for a trial set.
   * @return the number of trials for a given current number of uniques for a trial set.
   */
  public int getTrials(int curU) {
    if ((lgMinTrials_ == lgMaxTrials_) || (curU <= (1 << lgBP_))) {
      return 1 << lgMaxTrials_;
    }
    double lgCurU = log(curU)/LN2;
    double lgTrials = slope_ * (lgCurU - lgBP_) + lgMaxTrials_;
    return (int) pow(2.0, lgTrials);
  }
  
  /**
   * Return the configured Points-Per-Octave.
   * @return the configured Points-Per-Octave.
   */
  public int getPPO() {
    return ppo_;
  }
  
  /**
   * Returns the maximum generating index (gi) from the log_base2 of the maximum number of uniques
   * for the entire test run.
   * @return the maximum generating index (gi)
   */
  public int getMaximumGeneratingIndex() {
    return ppo_*lgMaxU_;
  }
  
  @Override
  public String toString() {
    return "Trials Profile: LgMinTrials: "+lgMinTrials_+", LgMaxTrials: "+lgMaxTrials_+
        ", lgMaxU: "+lgMaxU_+", PPO: "+ppo_;
  }

}