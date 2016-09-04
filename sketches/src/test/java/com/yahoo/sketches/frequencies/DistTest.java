/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import org.testng.Assert;
//import org.testng.annotations.Test;

public class DistTest {

  /**
   * @param prob the probability of success for the geometric distribution.
   * @return a random number generated from the geometric distribution.
   */
  public static long randomGeometricDist(double prob) {
    assert (prob > 0.0 && prob < 1.0);
    return 1 + (long) (Math.log(Math.random()) / Math.log(1.0 - prob));
  }

  public static double zeta(long n, double theta) {
    // the zeta function, used by the below zipf function
    // (this is not often called from outside this library)
    // ... but have made it public now to speed things up
    int i;
    double ans = 0.0;

    for (i = 1; i <= n; i++)
      ans += Math.pow(1. / i, theta);
    return (ans);
  }

  // This draws values from the zipf distribution
  // n is range, theta is skewness parameter
  // theta = 0 gives uniform distribution,
  // theta > 1 gives highly skewed distribution.
  public static long zipf(double theta, long n, double zetan) {
    double alpha;
    double eta;
    double u;
    double uz;
    double val;

    // randinit must be called before entering this procedure for
    // the first time since it uses the random generators

    alpha = 1. / (1. - theta);
    eta = (1. - Math.pow(2. / n, 1. - theta)) / (1. - zeta(2, theta) / zetan);

    u = 0.0;
    while (u == 0.0)
      u = Math.random();
    uz = u * zetan;
    if (uz < 1.)
      val = 1;
    else if (uz < (1. + Math.pow(0.5, theta)))
      val = 2;
    else
      val = 1 + (n * Math.pow(eta * u - eta + 1., alpha));

    return (long) val;
  }

  //@Test
  public static void testRandomGeometricDist() {
    long maxItem = 0L;
    double prob = .1;
    for (int i = 0; i < 100; i++) {
      long item = randomGeometricDist(prob);
      if (item > maxItem)
        maxItem = item;
      // If you succeed with probability p the probability
      // of failing 20/p times is smaller than 1/2^20.
      Assert.assertTrue(maxItem < 20.0 / prob);
    }
  }

}
