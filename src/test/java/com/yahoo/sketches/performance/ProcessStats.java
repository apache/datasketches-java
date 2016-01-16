/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.performance;

import static java.lang.Math.abs;
import static java.lang.Math.sqrt;

import java.util.Arrays;

/**
 * Processes the statistics collected from an array of Stats objects from a trial set
 * and creates an output row
 * 
 * @author Lee Rhodes
 */
public class ProcessStats {
  private static final char TAB = '\t';
  //Quantile fractions computed from the standard normal cumulative distribution.
  private static final double M2SD = 0.022750131948179; //minus 2 StdDev
  private static final double M1SD = 0.158655253931457; //minus 1 StdDev
  private static final double P1SD = 0.841344746068543; //plus  1 StdDev
  private static final double P2SD = 0.977249868051821; //plus  2 StdDev
  
  /**
   * Process the Stats[] array and place the output row into the dataStr.
   * @param statsArr the input Stats array
   * @param uPerTrial the number of uniques per trial for this trial set.
   * @param lgK log base 2 of configured nominal entries, or k.
   * @param p the probability sampling rate. 0 &lt; p &le; 1.0.
   * @param dataStr The StringBuilder object that is reused for each row of output
   */
  public static void process(Stats[] statsArr, int uPerTrial, int lgK, double p, StringBuilder dataStr) {
    int k = 1 << lgK;
    int trials = statsArr.length;
    Arrays.sort(statsArr, 0, trials);
    
    //Computing the quantiles from the sorted array. 
    double min = statsArr[0].re;
    double qM2SD = statsArr[quantileIndex(M2SD,trials)].re;
    double qM1SD = statsArr[quantileIndex(M1SD,trials)].re;
    double q50 = statsArr[quantileIndex(.5,trials)].re;
    double qP1SD = statsArr[quantileIndex(P1SD,trials)].re;
    double qP2SD = statsArr[quantileIndex(P2SD,trials)].re;
    double max = statsArr[trials-1].re;
    
    int cntLB2 = 0, cntLB1 = 0, cntUB1 = 0, cntUB2 = 0;
//    double sumLB2 = 0, sumLB1 = 0, sumUB1 = 0, sumUB2 = 0;
    double sumEst = 0, sumEstErr = 0, sumSqEstErr = 0;
    double sumUpdateTimePerU_nS = 0;
    //Scan the sorted statsArr
    for (int i=0; i<trials; i++) {
      Stats stats = statsArr[i];
      if (uPerTrial > stats.ub2est) cntUB2++; //should be <  2.275%; under estimate
      if (uPerTrial > stats.ub1est) cntUB1++; //should be < 15.866%; under estimate
      if (uPerTrial < stats.lb1est) cntLB1++; //should be < 15.866%;  over estimate
      if (uPerTrial < stats.lb2est) cntLB2++; //should be <  2.275%;  over estimate
//      sumLB2 += stats.lb2est;
//      sumLB1 += stats.lb1est;
//      sumUB1 += stats.ub1est;
//      sumUB2 += stats.ub2est;
      //divide by uPerTrial to normalize betweeen 0 and 1.0, sum over all trials
      //Components for the mean and variance of the estimate error
      sumEst += statsArr[i].estimate;
      double estErr = statsArr[i].re;
      sumEstErr += estErr;
      sumSqEstErr += estErr*estErr;
      
      sumUpdateTimePerU_nS += statsArr[i].updateTimePerU_nS;
    }
    //normalize counts
    double fracTgtUB2 = (double)cntUB2/trials;
    double fracTgtUB1 = (double)cntUB1/trials;
    double fracTltLB1  = (double)cntLB1/trials;
    double fracTltLB2  = (double)cntLB2/trials;
    
    //Compute the average results over the trial set
    double meanEst = sumEst/trials;
    double meanEstErr = sumEstErr/trials;
    double deltaSqEstErr = abs(sumSqEstErr - (sumEstErr*sumEstErr)/trials);
    double varEstErr = (trials == 1)? deltaSqEstErr/trials : deltaSqEstErr/(trials-1);
    double rse = sqrt(varEstErr);
    //compute theoretical sketch RSE
    double invKm1 = 1.0/(k-1);
    double oneMinusKoverN = 1.0 - (double)k/uPerTrial;
    double thrse = (sumEstErr == 0.0)? 0.0 : sqrt(invKm1 * oneMinusKoverN);
    //compute Bernoulli RSE
    double invUperTrial = 1.0/uPerTrial;
    double varOverN = (p == 1.0)? 0.0 : 1.0/p - 1.0;
    double prse = (p == 1.0)? 0.0 : sqrt(invUperTrial * varOverN);
    
    //Compute average of each of the bounds estimates
//    double meanLB2est = sumLB2/(uPerTrial*trials) -1;
//    double meanLB1est = sumLB1/(uPerTrial*trials) -1;
//    double meanUB1est = sumUB1/(uPerTrial*trials) -1;
//    double meanUB2est = sumUB2/(uPerTrial*trials) -1;
    
    //Speed
    double meanUpdateTimePerU_nS = sumUpdateTimePerU_nS/trials;
    
    //OUTPUT
    dataStr.setLength(0);
    dataStr.append(uPerTrial).append(TAB).
    
    //Sketch estimates, mean, variance
    append(meanEst).append(TAB).
    append(meanEstErr).append(TAB).
    append(rse).append(TAB).
    append(thrse).append(TAB).
    append(prse).append(TAB).
    
    //Quantiles measured from the actual distribution of values from all trials.
    //Because of quantization effects these values will be noisier than the values
    //computed statistically above.
    append(min).append(TAB).
    append(qM2SD).append(TAB).
    append(qM1SD).append(TAB).
    append(q50).append(TAB).
    append(qP1SD).append(TAB).
    append(qP2SD).append(TAB).
    append(max).append(TAB).
    
    //Fractional Bounds measurements
    append(fracTltLB2).append(TAB).
    append(fracTltLB1).append(TAB).
    append(fracTgtUB1).append(TAB).
    append(fracTgtUB2).append(TAB).
    
    //The bounds estimates are computed mathematically based on the sketch
    // estimate, the number of valid values in the cache and the value of theta. 
    // Because of this thes values will be relatively smooth from point to point along the
    // unique value axis.
//    append(meanLB2est).append(TAB).
//    append(meanLB1est).append(TAB).
//    append(meanUB1est).append(TAB).
//    append(meanUB2est).append(TAB).
    //Trials
    append(trials).append(TAB).
    //Speed
    append(meanUpdateTimePerU_nS);
  }
  
  /**
   * Returns a column header row
   * @return a column header row
   */
  public static String getHeader() {
    StringBuilder sb = new StringBuilder();
    sb. append("InU").append(TAB).
    //Estimates
    append("MeanEst").append(TAB).
    append("MeanErr").append(TAB).
    append("RSE").append(TAB).
    append("thRSE").append(TAB).
    append("pRSE").append(TAB).
    //Quantiles
    append("Min").append(TAB).
    append("QM2SD").append(TAB).
    append("QM1SD").append(TAB).
    append("Q50").append(TAB).
    append("QP1SD").append(TAB).
    append("QP2SD").append(TAB).
    append("Max").append(TAB).
    //Fractional Bounds measurements
    append("FracTltLB2").append(TAB).
    append("FracTltLB1").append(TAB).
    append("FracTgtUB1").append(TAB).
    append("FracTgtUB2").append(TAB).
    
    //Trials
    append("Trials").append(TAB).
    //Speed
    append("nS/u");
    return sb.toString();
  }
  
  /**
   * Returns the trial index = floor(quantile-fraction, #trials)
   * @param frac the desired quantile fraction (0.0 - 1.0)
   * @param trials the number of total trials
   * @return the trial index
   */
  private static int quantileIndex(double frac, int trials) {
      int idx1 = (int) Math.floor(frac*trials);
      return (idx1 >= trials)? trials-1: idx1;
  }
}
