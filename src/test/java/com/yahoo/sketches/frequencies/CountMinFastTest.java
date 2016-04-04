/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

//import org.testng.annotations.Test;
import org.testng.Assert;
import java.lang.Math;

/**
 * Tests CountMinFast class
 * 
 * @author Justin8712
 * 
 */
public class CountMinFastTest {

  //@Test(expectedExceptions = IllegalArgumentException.class)
  public void construct() {
    int size = 100;
    double eps = 1.0 / size;
    double delta = .01;
    CountMinFast countmin = new CountMinFast(eps, delta);
    Assert.assertNotNull(countmin);
    // Should throw exception
    countmin = new CountMinFast(-134, delta);
  }

  //@Test
  public void updateOneTime() {
    int size = 100;
    double eps = 1.0 / size;
    double delta = .01;
    CountMinFast countmin = new CountMinFast(eps, delta);
    countmin.update(13L);
    Assert.assertEquals(countmin.getEstimate(13L), 1);
  }

  //@Test
  public void ErrorCorrect() {
    int size = 100;
    double eps = 1.0 / size;
    double delta = .01;
    CountMinFast countmin = new CountMinFast(eps, delta);
    for (long key = 0L; key < 10000L; key++) {
      countmin.update(key, 1);
      Assert.assertTrue(countmin.getMaxError() == (long) (Math.ceil((key + 1) * eps)));
    }
  }


  /**
   * @param prob the probability of success for the geometric distribution.
   * @return a random number generated from the geometric distribution.
   */
  static private long randomGeometricDist(double prob) {
    assert (prob > 0.0 && prob < 1.0);
    return (long) (Math.log(Math.random()) / Math.log(1.0 - prob));
  }

  //@Test
  public void testRandomGeometricDist() {
    long maxKey = 0L;
    double prob = .1;
    for (int i = 0; i < 100; i++) {
      long key = randomGeometricDist(prob);
      if (key > maxKey)
        maxKey = key;
      // If you succeed with probability p the probability
      // of failing 20/p times is smaller than 1/2^20.
      Assert.assertTrue(maxKey < 20.0 / prob);
    }
  }

  //@Test
  public void realCountsInBounds() {
    int n = 4213;
    int size = 50;
    long key;
    double prob = .04;
    double eps = 1.0 / size;
    double delta = .01;
    int bad = 0;

    CountMinFast countmin = new CountMinFast(eps, delta);
    PositiveCountersMap realCounts = new PositiveCountersMap();
    for (int i = 0; i < n; i++) {
      key = randomGeometricDist(prob);
      countmin.update(key);
      realCounts.increment(key);
      long realCount = realCounts.get(key);
      long upperBound = countmin.getEstimateUpperBound(key);
      long lowerBound = countmin.getEstimateLowerBound(key);
      if (upperBound >= realCount && realCount >= lowerBound) {
        continue;
      } else {
        System.out.format("upperbound: %d, realCount: %d, lowerbound: %d \n", upperBound, realCount,
            lowerBound);
        bad += 1;
      }
    }
    // System.out.format("bad is: %d and eps * n is: %f \n", bad, eps*n);
    Assert.assertTrue(bad <= eps * n);
  }



  //@Test
  public void realCountsInBoundsCU() {
    int n = 4213;
    int size = 50;
    long key;
    double prob = .04;
    double eps = 1.0 / size;
    double delta = .01;
    int bad = 0;

    CountMinFast countmin = new CountMinFast(eps, delta);
    PositiveCountersMap realCounts = new PositiveCountersMap();
    for (int i = 0; i < n; i++) {
      key = randomGeometricDist(prob);
      countmin.conservative_update(key);
      realCounts.increment(key);
      long realCount = realCounts.get(key);
      long upperBound = countmin.getEstimateUpperBound(key);
      long lowerBound = countmin.getEstimateLowerBound(key);
      if (upperBound >= realCount && realCount >= lowerBound) {
        continue;
      } else {
        System.out.format("upperbound: %d, realCount: %d, lowerbound: %d \n", upperBound, realCount,
            lowerBound);
        bad += 1;
      }
    }
    // System.out.format("bad is: %d and eps * n is: %f \n", bad, eps*n);
    Assert.assertTrue(bad <= eps * n);
  }


  //@Test
  public void ConservativeBetterThanNon() {
    int n = 4213;
    int size = 50;
    long key;
    double prob = .04;
    double eps = 1.0 / size;
    double delta = .01;

    CountMinFast countmin1 = new CountMinFast(eps, delta);
    CountMinFast countmin2 = new CountMinFast(eps, delta);
    PositiveCountersMap realCounts = new PositiveCountersMap();
    for (int i = 0; i < n; i++) {
      key = randomGeometricDist(prob);
      countmin1.conservative_update(key);
      countmin2.update(key);
      realCounts.increment(key);

      long upperBound = countmin1.getEstimateUpperBound(key);

      Assert.assertTrue(upperBound <= countmin2.getEstimateUpperBound(key));
    }
  }


  //@Test(expectedExceptions = IllegalArgumentException.class)
  public void UnionErrorCheck() {
    int size1 = 100;
    int size2 = 400;
    double delta = .01;
    double eps1 = 1.0 / size1;
    double eps2 = 1.0 / size2;

    CountMinFast countmin1 = new CountMinFast(eps1, delta);
    CountMinFast countmin2 = new CountMinFast(eps2, delta);

    // should throw an exception
    countmin1.merge(countmin2);
  }

  //@Test
  public void realCountsInBoundsAfterUnion() {
    int n = 1000;
    int size = 400;
    double delta = .01;
    double eps = 1.0 / size;

    double prob1 = .01;
    double prob2 = .005;

    PositiveCountersMap realCounts = new PositiveCountersMap();
    CountMinFast countmin1 = new CountMinFast(eps, delta);
    CountMinFast countmin2 = new CountMinFast(eps, delta);
    for (int i = 0; i < n; i++) {
      long key1 = randomGeometricDist(prob1);
      long key2 = randomGeometricDist(prob2);

      countmin1.update(key1);
      countmin2.update(key2);

      // Updating the real counters
      realCounts.increment(key1);
      realCounts.increment(key2);
    }
    CountMinFast countmin = countmin1.merge(countmin2);

    int bad = 0;
    int i = 0;
    for (long key : realCounts.keys()) {
      i = i + 1;

      long realCount = realCounts.get(key);
      long upperBound = countmin.getEstimateUpperBound(key);
      long lowerBound = countmin.getEstimateLowerBound(key);

      if (upperBound < realCount || realCount < lowerBound) {
        bad = bad + 1;
        System.out.format("upperbound: %d, realCount: %d, lowerbound: %d \n", upperBound, realCount,
            lowerBound);
      }
    }
    Assert.assertTrue(bad <= delta * i);
  }

  //@Test
  public void realCountsInBoundsAfterUnionCU() {
    int n = 1000;
    int size = 400;
    double delta = .01;
    double eps = 1.0 / size;

    double prob1 = .01;
    double prob2 = .005;

    PositiveCountersMap realCounts = new PositiveCountersMap();
    CountMinFast countmin1 = new CountMinFast(eps, delta);
    CountMinFast countmin2 = new CountMinFast(eps, delta);
    for (int i = 0; i < n; i++) {
      long key1 = randomGeometricDist(prob1);
      long key2 = randomGeometricDist(prob2);

      countmin1.conservative_update(key1);
      countmin2.conservative_update(key2);

      // Updating the real counters
      realCounts.increment(key1);
      realCounts.increment(key2);
    }
    CountMinFast countmin = countmin1.merge(countmin2);

    int bad = 0;
    int i = 0;
    for (long key : realCounts.keys()) {
      i = i + 1;

      long realCount = realCounts.get(key);
      long upperBound = countmin.getEstimateUpperBound(key);
      long lowerBound = countmin.getEstimateLowerBound(key);

      if (upperBound < realCount || realCount < lowerBound) {
        bad = bad + 1;
        System.out.format("upperbound: %d, realCount: %d, lowerbound: %d \n", upperBound, realCount,
            lowerBound);
      }
    }
    Assert.assertTrue(bad <= delta * i);
  }

  //run only from command line
  public void stressTestUpdateTime() {
    int n = 2000000;
    int size = 100000;
    double eps = 1.0 / size;
    double delta = .1;
    int trials = 100;
    double total_updates_per_s = 0;
    for (int trial = 0; trial < trials; trial++) {
      CountMinFast countmin = new CountMinFast(eps, delta);
      int key = 0;
      double startTime = System.nanoTime();
      for (int i = 0; i < n; i++) {
        // long key = randomGeometricDist(prob);
        countmin.update(key++);
      }
      double endTime = System.nanoTime();
      double timePerUpdate = (endTime - startTime) / (1000000.0 * n);
      double updatesPerSecond = 1000.0 / timePerUpdate;
      total_updates_per_s += updatesPerSecond;
    }
    System.out.format("Amortized updates per second for update: %f\n",
        (total_updates_per_s / trials));
    Assert.assertTrue(total_updates_per_s / trials > 1000000);
  }

  //run only from command line
  public void stressTestUpdateTimeCU() {
    int n = 2000000;
    int size = 100000;
    double eps = 1.0 / size;
    double delta = .1;
    int trials = 100;
    double total_updates_per_s = 0;
    for (int trial = 0; trial < trials; trial++) {

      CountMinFast countmin = new CountMinFast(eps, delta);
      int key = 0;
      double startTime = System.nanoTime();
      for (int i = 0; i < n; i++) {
        // long key = randomGeometricDist(prob);
        countmin.conservative_update(key++);
      }
      double endTime = System.nanoTime();
      double timePerUpdate = (endTime - startTime) / (1000000.0 * n);
      double updatesPerSecond = 1000.0 / timePerUpdate;
      total_updates_per_s += updatesPerSecond;
    }
    System.out.format("Amortized updates per second for conservate_update: %f\n",
        (total_updates_per_s / trials));
    Assert.assertTrue(total_updates_per_s / trials > 1000000);
  }

  //@Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    // System.out.println(s); //disable here
  }
}
