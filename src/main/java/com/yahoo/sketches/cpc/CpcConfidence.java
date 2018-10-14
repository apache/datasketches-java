/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.IconEstimator.getIconEstimate;
import static java.lang.Math.ceil;
import static java.lang.Math.log;
import static java.lang.Math.sqrt;

/**
 * Tables and methods for estimating upper and lower bounds.
 *
 * <p>Tables were generated from empirical measurements at N = 1000 * K using millions of trials.
 *
 * @author Lee Rhodes
 */
final class CpcConfidence {
  private static final double iconErrorConstant = log(2.0); //0.693147180559945286
  private static final double hipErrorConstant = sqrt(log(2.0) / 2.0); //0.588705011257737332

  static short[] iconLowSideData = {
    //1,    2,    3,   kappa
    //                 lgK numtrials
    6037, 5720, 5328, // 4 1000000
    6411, 6262, 5682, // 5 1000000
    6724, 6403, 6127, // 6 1000000
    6665, 6411, 6208, // 7 1000000
    6959, 6525, 6427, // 8 1000000
    6892, 6665, 6619, // 9 1000000
    6792, 6752, 6690, // 10 1000000
    6899, 6818, 6708, // 11 1000000
    6871, 6845, 6812, // 12 1046369
    6909, 6861, 6828, // 13 1043411
    6919, 6897, 6842, // 14 1000297
  };

  static short[] iconHighSideData = {
    //1,    2,    3,   kappa
    //                 lgK numtrials
    8031, 8559, 9309, // 4 1000000
    7084, 7959, 8660, // 5 1000000
    7141, 7514, 7876, // 6 1000000
    7458, 7430, 7572, // 7 1000000
    6892, 7141, 7497, // 8 1000000
    6889, 7132, 7290, // 9 1000000
    7075, 7118, 7185, // 10 1000000
    7040, 7047, 7085, // 11 1000000
    6993, 7019, 7053, // 12 1046369
    6953, 7001, 6983, // 13 1043411
    6944, 6966, 7004, // 14 1000297
  };

  static short[] hipLowSideData = {
    //1,    2,    3,   kappa
    //                 lgK numtrials
    5871, 5247, 4826, // 4 1000000
    5877, 5403, 5070, // 5 1000000
    5873, 5533, 5304, // 6 1000000
    5878, 5632, 5464, // 7 1000000
    5874, 5690, 5564, // 8 1000000
    5880, 5745, 5619, // 9 1000000
    5875, 5784, 5701, // 10 1000000
    5866, 5789, 5742, // 11 1000000
    5869, 5827, 5784, // 12 1046369
    5876, 5860, 5827, // 13 1043411
    5881, 5853, 5842, // 14 1000297
  };

  static short[] hipHighSideData = {
    //1,    2,    3,   kappa
    //                 lgK numtrials
    5855, 6688, 7391, // 4 1000000
    5886, 6444, 6923, // 5 1000000
    5885, 6254, 6594, // 6 1000000
    5889, 6134, 6326, // 7 1000000
    5900, 6072, 6203, // 8 1000000
    5875, 6005, 6089, // 9 1000000
    5871, 5980, 6040, // 10 1000000
    5889, 5941, 6015, // 11 1000000
    5871, 5926, 5973, // 12 1046369
    5866, 5901, 5915, // 13 1043411
    5880, 5914, 5953, // 14 1000297
  };

  static double getIconConfidenceLB(final int lgK, final long numCoupons, final int kappa) {
    if (numCoupons == 0) { return 0.0; }
    assert lgK >= 4;
    assert (kappa >= 1) && (kappa <= 3);
    double x = iconErrorConstant;
    if (lgK <= 14) { x = (iconHighSideData[(3 * (lgK - 4)) + (kappa - 1)]) / 10000.0; }
    final double rel = x / sqrt(1 << lgK);
    final double eps = kappa * rel;
    final double est = getIconEstimate(lgK, numCoupons);
    double result = est / (1.0 + eps);
    if (result < numCoupons) { result = numCoupons; }
    return result;
  }

  static double getIconConfidenceUB(final int lgK, final long numCoupons, final int kappa) {
    if (numCoupons == 0) { return 0.0; }
    assert lgK >= 4;
    assert (kappa >= 1) && (kappa <= 3);
    double x = iconErrorConstant;
    if (lgK <= 14) { x = (iconLowSideData[(3 * (lgK - 4)) + (kappa - 1)]) / 10000.0; }
    final double rel = x / sqrt(1 << lgK);
    final double eps = kappa * rel;
    final double est = getIconEstimate(lgK, numCoupons);
    final double result = est / (1.0 - eps);
    return ceil(result);  // slight widening of interval to be conservative
  }

  //mergeFlag must already be checked as false
  static double getHipConfidenceLB(final int lgK, final long numCoupons, final double hipEstAccum,
      final int kappa) {
    if (numCoupons == 0) { return 0.0; }
    assert lgK >= 4;
    assert (kappa >= 1) && (kappa <= 3);
    double x = hipErrorConstant;
    if (lgK <= 14) { x = (hipHighSideData[(3 * (lgK - 4)) + (kappa - 1)]) / 10000.0; }
    final double rel = x / sqrt(1 << lgK);
    final double eps = kappa * rel;
    final double est = hipEstAccum;
    double result = est / (1.0 + eps);
    if (result < numCoupons) { result = numCoupons; }
    return result;
  }

  //mergeFlag must already be checked as false
  static double getHipConfidenceUB(final int lgK, final long numCoupons, final double hipEstAccum,
      final int kappa) {
    if (numCoupons == 0) { return 0.0; }
    assert lgK >= 4;
    assert (kappa >= 1) && (kappa <= 3);
    double x = hipErrorConstant;
    if (lgK <= 14) { x = (hipLowSideData[(3 * (lgK - 4)) + (kappa - 1)]) / 10000.0; }
    final double rel = x / sqrt(1 << lgK);
    final double eps = kappa * rel;
    final double est = hipEstAccum;
    final double result = est / (1.0 - eps);
    return ceil(result); // widening for coverage
  }

}
