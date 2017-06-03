/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static com.yahoo.sketches.BinomialBoundsN.checkArgs;
import static com.yahoo.sketches.BinomialBoundsN.getLowerBound;
import static com.yahoo.sketches.BinomialBoundsN.getUpperBound;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.annotations.Test;

/**
 * @author Kevin Lang
 */
public class BinomialBoundsNTest {

  public static double[] runTestAux (long max_numSamplesI, int ci, double min_p) {
    long numSamplesI = 0;
    double p, lb, ub;
    double sum1 = 0.0;
    double sum2 = 0.0;
    double sum3 = 0.0;
    double sum4 = 0.0;
    long count = 0;
  
    while (numSamplesI <= max_numSamplesI) { /* was <= */
      p = 1.0;
      
      while (p >= min_p) {
        lb = BinomialBoundsN.getLowerBound (numSamplesI, p, ci, false);
        ub = BinomialBoundsN.getUpperBound (numSamplesI, p, ci, false);
        
        // if (numSamplesI == 300 && p > 0.365 && p < 0.367) { ub += 0.01; }  // artificial discrepancy
        
        // the logarithm helps discrepancies to not be swamped out of the total
        sum1 += Math.log (lb + 1.0); 
        sum2 += Math.log (ub + 1.0);
        count += 2;
        
        if (p < 1.0) {
          lb = BinomialBoundsN.getLowerBound (numSamplesI, 1.0 - p, ci, false);
          ub = BinomialBoundsN.getUpperBound (numSamplesI, 1.0 - p, ci, false);
          sum3 += Math.log (lb + 1.0);
          sum4 += Math.log (ub + 1.0);
          count += 2;
        }
        
        p *= 0.99;
      }
      numSamplesI = Math.max (numSamplesI+1, 1001*numSamplesI/1000);
    }
    
    println(String.format("{%.15e, %.15e, %.15e, %.15e, %d}", sum1, sum2, sum3, sum4, count));
    double[] arrOut = {sum1, sum2, sum3, sum4, count};
    return arrOut;
  }
  
  private static final double TOL = 1E-15;
  
  @Test
  public static void checkBounds() {
    int i = 0;
    for (int ci = 1; ci <= 3; ci++, i++) {
      double[] arr = runTestAux (20, ci, 1e-3);
      for (int j=0; j<5; j++) {
        assertTrue((arr[j] / std[i][j] -1.0) < TOL);
      }
    }
    for (int ci = 1; ci <= 3; ci++, i++) {
      double[] arr = runTestAux (200, ci, 1e-5);
      for (int j=0; j<5; j++) {
        assertTrue((arr[j] / std[i][j] -1.0) < TOL);
      }
    }
    //comment last one out for a shorter test
//    for (int ci = 1; ci <= 3; ci++, i++) {
//      double[] arr = runTestAux (2000, ci, 1e-7);
//      for (int j=0; j<5; j++) {
//        assertTrue((arr[j] / std[i][j] -1.0) < TOL);
//      }
//    }
  }
  
  // With all 3 enabled the test should produce in groups of 3 */
  private static final double[][] std = {
    {7.083330682531043e+04, 8.530373642825481e+04, 3.273647725073409e+04, 3.734024243699785e+04, 57750},
    {6.539415269641498e+04, 8.945522372568645e+04, 3.222302546497840e+04, 3.904738469737429e+04, 57750},
    {6.006043493107306e+04, 9.318105731423477e+04, 3.186269956585285e+04, 4.096466221922520e+04, 57750},
    
    {2.275584770163813e+06, 2.347586549014998e+06, 1.020399409477305e+06, 1.036729927598294e+06, 920982},
    {2.243569126699713e+06, 2.374663344107342e+06, 1.017017233582122e+06, 1.042597845553438e+06, 920982},
    {2.210056231903739e+06, 2.400441267999687e+06, 1.014081235946986e+06, 1.049480769755676e+06, 920982},
    
    {4.688240115809608e+07, 4.718067204619278e+07, 2.148362024482338e+07, 2.153118905212302e+07, 12834414},
    {4.674205938540214e+07, 4.731333757486791e+07, 2.146902141966406e+07, 2.154916650733873e+07, 12834414},
    {4.659896614422579e+07, 4.744404182094614e+07, 2.145525391547799e+07, 2.156815612325058e+07, 12834414}
  };
  
  @Test
  public static void checkCheckArgs() {
    try {
      checkArgs(-1L, 1.0, 1);
      checkArgs(10L, 0.0, 1);
      checkArgs(10L, 1.01, 1);
      checkArgs(10L, 1.0, 3);
      checkArgs(10L, 1.0, 0);
      checkArgs(10L, 1.0, 4);
      fail("Expected SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //pass
    }
  }
  
  @Test
  public static void checkComputeApproxBino_LB_UB() {
    long n = 100;
    double theta = (2.0 - 1e-5)/2.0;
    double result = getLowerBound(n, theta, 1, false);
    assertEquals(result, n, 0.0);
    result = getUpperBound(n, theta, 1, false);
    assertEquals(result, n+1, 0.0);
    result = getLowerBound(n, theta, 1, true);
    assertEquals(result, 0.0, 0.0);
    result = getUpperBound(n, theta, 1, true);
    assertEquals(result, 0.0, 0.0);
  }
  
  @Test(expectedExceptions = SketchesArgumentException.class)
  public static void checkThetaLimits1() {
    BinomialBoundsN.getUpperBound(100, 1.1, 1, false);
  }
  
  @Test
  public static void boundsExample() {
    println("BinomialBoundsN Example:");
    int k = 500;
    double theta = 0.001;
    int stdDev = 2;
    double ub = BinomialBoundsN.getUpperBound(k, theta, stdDev, false);
    double est = k/theta;
    double lb = BinomialBoundsN.getLowerBound(k, theta, stdDev, false);
    println("K="+k+", Theta="+theta+", SD="+stdDev);
    println("UB:  "+ub);
    println("Est: "+est);
    println("LB:  "+lb);
    println("");
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }
  
  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
  
}
