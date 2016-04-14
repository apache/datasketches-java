package com.yahoo.sketches.frequencies;

/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

import java.util.Collection;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.frequencies.FrequentLongsEstimator.ErrorSpecification;
import com.yahoo.sketches.frequencies.ReversePurgeHashMap;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MasterFETester {

  public static void main(String[] args) {
    HashMapRESerialTest();
    System.out.println("Done HashMap Serialization Test");
    FrequentItemsStringSerialTest();
    System.out.println("Done FrequentItems String Serialization Test");
    FrequentItemsByteSerialTest();
    System.out.println("Done FrequentItems Byte Serialization Test");
    FrequentItemsByteResetandEmptySerialTest();
    System.out.println("Done FrequentItems Byte Empty Serialization Test");
    FETest();
    System.out.println("Done FE Test");
    realCountsInBoundsAfterMerge();
    System.out.println("Done realCountsoinBoundsAFterMerge Test");
    strongMergeTest();
    System.out.println("Done StrongMerge Test");
    updateOneTime();
    System.out.println("Done UpdateOneTime Test");
    ErrorTestZipfBigParam();
    System.out.println("Done Error Test Big Param");
    ErrorTestZipfSmallParam();
    System.out.println("Done Error Test Small Param");
    ErrorTestZipfBigParamSmallSketch();
    System.out.println("Done Error Test BigParamSmallSketch");
  }

  @Test
  private static void HashMapRESerialTest() {
    ReversePurgeHashMap map = new ReversePurgeHashMap(8);
    map.adjustOrPutValue(10, 15, 15);
    map.adjustOrPutValue(10, 5, 5);
    map.adjustOrPutValue(1, 1, 1);
    map.adjustOrPutValue(2, 3, 3);
    String string = map.serializeToString();
    //println(string);
    //println(map.toString());
    ReversePurgeHashMap new_map =
        ReversePurgeHashMap.getInstance(string);
    String new_string = new_map.serializeToString();
    Assert.assertTrue(string.equals(new_string));
  }

  @Test
  private static void FrequentItemsStringSerialTest() {
    FrequentLongsSketch sketch = new FrequentLongsSketch(8);
    FrequentLongsSketch sketch2 = new FrequentLongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    String string0 = sketch.serializeToString();
    FrequentLongsSketch new_sketch0 = FrequentLongsSketch.getInstance(string0);
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    Assert.assertTrue(new_sketch0.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch0.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());

    sketch2.update(190, 12902390);
    sketch2.update(191, 12902390);
    sketch2.update(192, 12902390);
    sketch2.update(193, 12902390);
    sketch2.update(194, 12902390);
    sketch2.update(195, 12902390);
    sketch2.update(196, 12902390);
    sketch2.update(197, 12902390);
    sketch2.update(198, 12902390);
    sketch2.update(199, 12902390);
    sketch2.update(200, 12902390);
    sketch2.update(201, 12902390);
    sketch2.update(202, 12902390);
    sketch2.update(203, 12902390);
    sketch2.update(204, 12902390);
    sketch2.update(205, 12902390);
    sketch2.update(206, 12902390);
    sketch2.update(207, 12902390);
    sketch2.update(208, 12902390);

    String string2 = sketch2.serializeToString();
    FrequentLongsSketch new_sketch2 = FrequentLongsSketch.getInstance(string2);
    String new_string2 = new_sketch2.serializeToString();
    Assert.assertTrue(string2.equals(new_string2));
    Assert.assertTrue(new_sketch2.getMaximumMapCapacity() == sketch2.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch2.getCurrentMapCapacity() == sketch2.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch2.getStreamLength() == sketch2.getStreamLength());

    FrequentLongsSketch merged_sketch = (FrequentLongsSketch) sketch.merge(sketch2);

    String string = merged_sketch.serializeToString();
    FrequentLongsSketch new_sketch = FrequentLongsSketch.getInstance(string);
    String new_string = new_sketch.serializeToString();
    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  private static void FrequentItemsByteSerialTest() {
    FrequentLongsSketch sketch = new FrequentLongsSketch(16, 8);
    FrequentLongsSketch sketch2 = new FrequentLongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    byte[] bytearray0 = sketch.serializeToByteArray();
    Memory mem0 = new NativeMemory(bytearray0);
    FrequentLongsSketch new_sketch0 = FrequentLongsSketch.getInstance(mem0);

    String string0 = sketch.serializeToString();
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    Assert.assertTrue(new_sketch0.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch0.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());

    sketch2.update(190, 12902390);
    sketch2.update(191, 12902390);
    sketch2.update(192, 12902390);
    sketch2.update(193, 12902390);
    sketch2.update(194, 12902390);
    sketch2.update(195, 12902390);
    sketch2.update(196, 12902390);
    sketch2.update(197, 12902390);
    sketch2.update(198, 12902390);
    sketch2.update(199, 12902390);
    sketch2.update(200, 12902390);
    sketch2.update(201, 12902390);
    sketch2.update(202, 12902390);
    sketch2.update(203, 12902390);
    sketch2.update(204, 12902390);
    sketch2.update(205, 12902390);
    sketch2.update(206, 12902390);
    sketch2.update(207, 12902390);
    sketch2.update(208, 12902390);

    byte[] bytearray2 = sketch2.serializeToByteArray();
    Memory mem2 = new NativeMemory(bytearray2);
    FrequentLongsSketch new_sketch2 = FrequentLongsSketch.getInstance(mem2);

    String string2 = sketch2.serializeToString();
    String new_string2 = new_sketch2.serializeToString();

    Assert.assertTrue(string2.equals(new_string2));
    Assert.assertTrue(new_sketch2.getMaximumMapCapacity() == sketch2.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch2.getCurrentMapCapacity() == sketch2.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch2.getStreamLength() == sketch2.getStreamLength());

    FrequentLongsSketch merged_sketch = (FrequentLongsSketch) sketch.merge(sketch2);

    byte[] bytearray = sketch.serializeToByteArray();
    Memory mem = new NativeMemory(bytearray);
    FrequentLongsSketch new_sketch = FrequentLongsSketch.getInstance(mem);

    String string = sketch.serializeToString();
    String new_string = new_sketch.serializeToString();

    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  private static void FrequentItemsByteResetandEmptySerialTest() {
    FrequentLongsSketch sketch = new FrequentLongsSketch(16);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);
    sketch.reset();

    byte[] bytearray0 = sketch.serializeToByteArray();
    Memory mem0 = new NativeMemory(bytearray0);
    FrequentLongsSketch new_sketch0 = FrequentLongsSketch.getInstance(mem0);

    String string0 = sketch.serializeToString();
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    Assert.assertTrue(new_sketch0.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch0.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());
  }

  @Test
  private static void FETest(){
    int numEstimators = 1; 
    int n = 2222;
    double error_tolerance = 1.0/100;
    
    FrequentLongsEstimator[] estimators = new FrequentLongsEstimator[numEstimators];
    for (int h = 0; h < numEstimators; h++) {
      estimators[h] = newFrequencyEstimator(error_tolerance);
    }

    PositiveCountersMap realCounts = new PositiveCountersMap();
    long key;
    double prob = .001;
    for (int i = 0; i < n; i++) {
      key = randomGeometricDist(prob) + 1;
      realCounts.increment(key);
      for (int h = 0; h < numEstimators; h++)
        estimators[h].update(key);
    }
    
    long threshold = 10;
    for(int h=0; h<numEstimators; h++) {
      long[] freq = estimators[h].getFrequentItems(threshold, ErrorSpecification.NO_FALSE_NEGATIVES);

      for (int i = 0; i < freq.length; i++)
        Assert.assertTrue(estimators[h].getUpperBound(freq[i]) > threshold);
    }
  }

  @Test
  private static void realCountsInBoundsAfterMerge() {
    int n = 1000;
    int size = 150;
    double delta = .1;
    double error_tolerance = 1.0 / size;

    double prob1 = .01;
    double prob2 = .005;
    int numEstimators = 1;

    for (int h = 0; h < numEstimators; h++) {
      FrequentLongsEstimator estimator1 = newFrequencyEstimator(error_tolerance);
      FrequentLongsEstimator estimator2 = newFrequencyEstimator(error_tolerance);
      PositiveCountersMap realCounts = new PositiveCountersMap();
      for (int i = 0; i < n; i++) {
        long key1 = randomGeometricDist(prob1) + 1;
        long key2 = randomGeometricDist(prob2) + 1;

        estimator1.update(key1);
        estimator2.update(key2);

        // Updating the real counters
        realCounts.increment(key1);
        realCounts.increment(key2);
      }
      FrequentLongsEstimator merged = estimator1.merge(estimator2);

      int bad = 0;
      int i = 0;
      for (long key : realCounts.keys()) {
        i = i + 1;

        long realCount = realCounts.get(key);
        long upperBound = merged.getUpperBound(key);
        long lowerBound = merged.getLowerBound(key);

        if (upperBound < realCount || realCount < lowerBound) {
          bad = bad + 1;
        }
      }
      Assert.assertTrue(bad <= delta * i);
    }
  }

  @Test
  private static void strongMergeTest() {
    int n = 100;
    int size = 150;
    double delta = .1;
    double error_tolerance = 1.0 / size;
    int num_to_merge = 10;
    FrequentLongsEstimator[] estimators = new FrequentLongsEstimator[num_to_merge];

    double prob = .01;
    int numEstimators = 1;

    for (int h = 0; h < numEstimators; h++) {
      for (int z = 0; z < num_to_merge; z++)
        estimators[z] = newFrequencyEstimator(error_tolerance);

      PositiveCountersMap realCounts = new PositiveCountersMap();
      for (int i = 0; i < n; i++) {
        for (int z = 0; z < num_to_merge; z++) {
          long key = randomGeometricDist(prob) + 1;

          estimators[z].update(key);
          // Updating the real counters
          realCounts.increment(key);
        }
      }

      FrequentLongsEstimator merged = estimators[0];
      for (int z = 0; z < num_to_merge; z++) {
        if (z == 0)
          continue;
        merged = merged.merge(estimators[z]);
      }

      int bad = 0;
      int i = 0;
      for (long key : realCounts.keys()) {
        i = i + 1;

        long realCount = realCounts.get(key);
        long upperBound = merged.getUpperBound(key);
        long lowerBound = merged.getLowerBound(key);

        if (upperBound < realCount || realCount < lowerBound) {
          bad = bad + 1;
        }
      }
      Assert.assertTrue(bad <= delta * i);
    }
  }

  @Test
  private static void updateOneTime() {
    int size = 100;
    double error_tolerance = 1.0 / size;
    //double delta = .01;
    int numEstimators = 1;
    for (int h = 0; h < numEstimators; h++) {
      FrequentLongsEstimator estimator = newFrequencyEstimator(error_tolerance);
      Assert.assertEquals(estimator.getUpperBound(13L), 0);
      Assert.assertEquals(estimator.getLowerBound(13L), 0);
      Assert.assertEquals(estimator.getMaximumError(), 0);
      Assert.assertEquals(estimator.getEstimate(13L), 0);
      estimator.update(13L);
      // Assert.assertEquals(estimator.getEstimate(13L), 1);
    }
  }
  
  @Test
  private static void ErrorTestZipfBigParam() {
    int size = 512;
    int n = 200 * size;
    //double delta = .1;
    double error_tolerance = 1.0 / size;
    int trials = 1;
    long stream[] = new long[n];

    double zet = zeta(n, 1.1);
    PositiveCountersMap realCounts = new PositiveCountersMap();

    for (int i = 0; i < n; i++) {
      stream[i] = zipf(1.1, n, zet);
      realCounts.increment(stream[i]);
    }

    int numEstimators = 1;

    for (int h = 0; h < numEstimators; h++) {
      FrequentLongsEstimator estimator = newFrequencyEstimator(error_tolerance);

      for (int trial = 0; trial < trials; trial++) {
        estimator = newFrequencyEstimator(error_tolerance);
        for (int i = 0; i < n; i++) {
          // long key = randomGeometricDist(prob);
          estimator.update(stream[i]);
        }
        long sum = 0;
        long max_error = 0;
        long error;
        long max_freq = 0;

        Collection<Long> keysCollection = realCounts.keys();

        for (long the_key : keysCollection) {
          if (realCounts.get(the_key) > max_freq) {
            max_freq = realCounts.get(the_key);
          }
          if (realCounts.get(the_key) > estimator.getEstimate(the_key)) {
            error = (realCounts.get(the_key) - estimator.getEstimate(the_key));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          } else {
            error = (estimator.getEstimate(the_key) - realCounts.get(the_key));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          }
        }
        Assert.assertTrue(max_error <= 2 * n * error_tolerance);
      }
    }
  }

  @Test
  private static void ErrorTestZipfSmallParam() {
    int size = 512;
    int n = 200 * size;
    //double delta = .1;
    double error_tolerance = 1.0 / size;
    int trials = 1;
    long stream[] = new long[n];

    double zet = zeta(n, 0.7);
    PositiveCountersMap realCounts = new PositiveCountersMap();

    for (int i = 0; i < n; i++) {
      stream[i] = zipf(0.7, n, zet);
      realCounts.increment(stream[i]);
    }

    int numEstimators = 1;

    for (int h = 0; h < numEstimators; h++) {
      FrequentLongsEstimator estimator = newFrequencyEstimator(error_tolerance);

      for (int trial = 0; trial < trials; trial++) {
        estimator = newFrequencyEstimator(error_tolerance);
        for (int i = 0; i < n; i++) {
          // long key = randomGeometricDist(prob);
          estimator.update(stream[i]);
        }
        long sum = 0;
        long max_error = 0;
        long error;
        long max_freq = 0;

        Collection<Long> keysCollection = realCounts.keys();

        for (long the_key : keysCollection) {
          if (realCounts.get(the_key) > max_freq) {
            max_freq = realCounts.get(the_key);
          }
          if (realCounts.get(the_key) > estimator.getEstimate(the_key)) {
            error = (realCounts.get(the_key) - estimator.getEstimate(the_key));
            if (error > max_error) {
              max_error = error;
            }
            sum = sum + error;
          } else {
            error = (estimator.getEstimate(the_key) - realCounts.get(the_key));
            if (error > max_error) {
              max_error = error;
            }
            sum = sum + error;
          }
        }
        Assert.assertTrue(max_error <= 2 * n * error_tolerance);
      }
    }
  }

  @Test
  private static void ErrorTestZipfBigParamSmallSketch() {
    int size = 64;
    int n = 200 * size;
    //double delta = .1;
    double error_tolerance = 1.0 / size;
    int trials = 1;
    long stream[] = new long[n];

    double zet = zeta(n, 1.1);
    PositiveCountersMap realCounts = new PositiveCountersMap();

    for (int i = 0; i < n; i++) {
      stream[i] = zipf(1.1, n, zet);
      realCounts.increment(stream[i]);
    }

    int numEstimators = 1;

    for (int h = 0; h < numEstimators; h++) {
      FrequentLongsEstimator estimator = newFrequencyEstimator(error_tolerance);

      for (int trial = 0; trial < trials; trial++) {
        estimator = newFrequencyEstimator(error_tolerance);
        for (int i = 0; i < n; i++) {
          // long key = randomGeometricDist(prob);
          estimator.update(stream[i]);
        }
        long sum = 0;
        long max_error = 0;
        long error;
        long max_freq = 0;

        Collection<Long> keysCollection = realCounts.keys();

        for (long the_key : keysCollection) {
          if (realCounts.get(the_key) > max_freq) {
            max_freq = realCounts.get(the_key);
          }
          if (realCounts.get(the_key) > estimator.getEstimate(the_key)) {
            error = (realCounts.get(the_key) - estimator.getEstimate(the_key));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          } else {
            error = (estimator.getEstimate(the_key) - realCounts.get(the_key));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          }
        }
        Assert.assertTrue(max_error <= 2 * n * error_tolerance);
      }
    }
  }

  //@Test
  public void freqItemsStressTest() {
    FrequentLongsSketch fi = new FrequentLongsSketch(512);
    int u = 1 << 20;
    for (int i = 0; i< u; i++) {
      fi.update(randomGeometricDist(0.002));
    }
  }
  
  
  ////////////////////////////////////////

  /**
   * @param prob the probability of success for the geometric distribution.
   * @return a random number generated from the geometric distribution.
   */
  private static long randomGeometricDist(double prob) {
    assert (prob > 0.0 && prob < 1.0);
    return 1 + (long) (Math.log(Math.random()) / Math.log(1.0 - prob));
  }

  static double zeta(long n, double theta) {

    // the zeta function, used by the below zipf function
    // (this is not often called from outside this library)
    // ... but have made it public now to speed things up

    int i;
    double ans = 0.0;

    for (i = 1; i <= n; i++)
      ans += Math.pow(1. / i, theta);
    return (ans);
  }


  // this draws values from the zipf distribution
  // n is range, theta is skewness parameter
  // theta = 0 gives uniform dbn,
  // theta > 1 gives highly skewed dbn.
  private static long zipf(double theta, long n, double zetan) {
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


  private static FrequentLongsEstimator newFrequencyEstimator(double error_parameter) {
    return new FrequentLongsSketch(
        Util.ceilingPowerOf2((int) (1.0 /(error_parameter*ReversePurgeHashMap.getLoadFactor()))));
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
