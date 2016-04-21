package com.yahoo.sketches.frequencies;

/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

import com.yahoo.sketches.Util;
import com.yahoo.sketches.frequencies.FrequentLongsSketch.ErrorType;
import com.yahoo.sketches.frequencies.ReversePurgeLongHashMap;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import gnu.trove.map.hash.TLongLongHashMap;

import static org.testng.Assert.*;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FrequentLongsSketchTest {

  public static void main(String[] args) {
    hashMapSerialTest();
    System.out.println("Done hashMapSerialTest");
    frequentItemsStringSerialTest();
    System.out.println("Done frequentItemsStringSerialTest");
    frequentItemsByteSerialTest();
    System.out.println("Done frequentItemsByteSerialTest");
    frequentItemsByteResetAndEmptySerialTest();
    System.out.println("Done frequentItemsByteResetAndEmptySerialTest");
    checkFreqLongs();
    System.out.println("Done FE Test");
    realCountsInBoundsAfterMerge();
    System.out.println("Done realCountsoInBoundsAFterMerge Test");
    strongMergeTest();
    System.out.println("Done strongMergeTest");
    updateOneTime();
    System.out.println("Done updateOneTime Test");
    errorTestZipfBigParam();
    System.out.println("Done errorTestZipfBigParam");
    errorTestZipfSmallParam();
    System.out.println("Done errorTestZipfSmallParam");
    errorTestZipfBigParamSmallSketch();
    System.out.println("Done errorTestZipfBigParamSmallSketch");
  }

  @Test
  private static void hashMapSerialTest() {
    ReversePurgeLongHashMap map = new ReversePurgeLongHashMap(8);
    map.adjustOrPutValue(10, 15, 15);
    map.adjustOrPutValue(10, 5, 5);
    map.adjustOrPutValue(1, 1, 1);
    map.adjustOrPutValue(2, 3, 3);
    String string = map.serializeToString();
    //println(string);
    //println(map.toString());
    ReversePurgeLongHashMap new_map =
        ReversePurgeLongHashMap.getInstance(string);
    String new_string = new_map.serializeToString();
    Assert.assertTrue(string.equals(new_string));
  }

  //@Test
  private static void frequentItemsStringSerialTest() {
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

    FrequentLongsSketch merged_sketch = sketch.merge(sketch2);

    String string = merged_sketch.serializeToString();
    FrequentLongsSketch new_sketch = FrequentLongsSketch.getInstance(string);
    String new_string = new_sketch.serializeToString();
    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  private static void frequentItemsByteSerialTest() {
    FrequentLongsSketch sketch = new FrequentLongsSketch(16);
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

    FrequentLongsSketch merged_sketch = sketch.merge(sketch2);

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
  private static void frequentItemsByteResetAndEmptySerialTest() {
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
  private static void checkFreqLongs(){
    int numSketches = 1; 
    int n = 2222;
    double error_tolerance = 1.0/100;
    
    FrequentLongsSketch[] sketches = new FrequentLongsSketch[numSketches];
    for (int h = 0; h < numSketches; h++) {
      sketches[h] = newFrequencySketch(error_tolerance);
    }

    long item;
    double prob = .001;
    for (int i = 0; i < n; i++) {
      item = randomGeometricDist(prob) + 1;
      for (int h = 0; h < numSketches; h++)
        sketches[h].update(item);
    }
    
    long threshold = 10;
    for(int h=0; h<numSketches; h++) {
      long[] freq = sketches[h].getFrequentItems(threshold, ErrorType.NO_FALSE_NEGATIVES);

      for (int i = 0; i < freq.length; i++)
        Assert.assertTrue(sketches[h].getUpperBound(freq[i]) > threshold);
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
    int numSketches = 1;

    for (int h = 0; h < numSketches; h++) {
      FrequentLongsSketch sketch1 = newFrequencySketch(error_tolerance);
      FrequentLongsSketch sketch2 = newFrequencySketch(error_tolerance);
      TLongLongHashMap realCounts = new TLongLongHashMap();
      
      for (int i = 0; i < n; i++) {
        long item1 = randomGeometricDist(prob1) + 1;
        long item2 = randomGeometricDist(prob2) + 1;

        sketch1.update(item1);
        sketch2.update(item2);

        // Updating the real counters
        realCounts.adjustOrPutValue(item1, 1, 1);
        realCounts.adjustOrPutValue(item2, 1, 1);
        
      }
      FrequentLongsSketch merged = sketch1.merge(sketch2);
      
      int bad = 0;
      int i = 0;
      long[] keys = realCounts.keys();
      //println("size: "+keys.length);
      for (long item : keys) {
        i = i + 1;

        long realCount = realCounts.get(item);
        long upperBound = merged.getUpperBound(item);
        long lowerBound = merged.getLowerBound(item);

        if (upperBound < realCount || realCount < lowerBound) {
          bad = bad + 1;
        }
      }
      //println("bad: "+bad+", delta*i: "+(delta*i));
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
    FrequentLongsSketch[] sketches = new FrequentLongsSketch[num_to_merge];

    double prob = .01;
    int numSketches = 1;

    for (int h = 0; h < numSketches; h++) {
      for (int z = 0; z < num_to_merge; z++) {
        sketches[z] = newFrequencySketch(error_tolerance);
      }
      TLongLongHashMap realCounts = new TLongLongHashMap();
      for (int i = 0; i < n; i++) {
        for (int z = 0; z < num_to_merge; z++) {
          long item = randomGeometricDist(prob) + 1;

          sketches[z].update(item);
          // Updating the real counters
          realCounts.adjustOrPutValue(item, 1, 1);
        }
      }

      FrequentLongsSketch merged = sketches[0];
      for (int z = 0; z < num_to_merge; z++) {
        if (z == 0)
          continue;
        merged = merged.merge(sketches[z]);
      }

      int bad = 0;
      int i = 0;
      long[] keys = realCounts.keys();
      //println("size: "+keys.length);
      for (long item : keys) {
        i = i + 1;

        long realCount = realCounts.get(item);
        long upperBound = merged.getUpperBound(item);
        long lowerBound = merged.getLowerBound(item);

        if (upperBound < realCount || realCount < lowerBound) {
          bad = bad + 1;
        }
      }
      //println("bad: "+bad+", delta*i: "+(delta*i));
      Assert.assertTrue(bad <= delta * i);
    }
  }

  @Test
  private static void updateOneTime() {
    int size = 100;
    double error_tolerance = 1.0 / size;
    //double delta = .01;
    int numSketches = 1;
    for (int h = 0; h < numSketches; h++) {
      FrequentLongsSketch sketch = newFrequencySketch(error_tolerance);
      Assert.assertEquals(sketch.getUpperBound(13L), 0);
      Assert.assertEquals(sketch.getLowerBound(13L), 0);
      Assert.assertEquals(sketch.getMaximumError(), 0);
      Assert.assertEquals(sketch.getEstimate(13L), 0);
      sketch.update(13L);
      // Assert.assertEquals(sketch.getEstimate(13L), 1);
    }
  }
  
  @Test
  private static void errorTestZipfBigParam() {
    int size = 512;
    int n = 200 * size;
    //double delta = .1;
    double error_tolerance = 1.0 / size;
    int trials = 1;
    long stream[] = new long[n];

    double zet = zeta(n, 1.1);
    TLongLongHashMap realCounts = new TLongLongHashMap();

    for (int i = 0; i < n; i++) {
      stream[i] = zipf(1.1, n, zet);
      realCounts.adjustOrPutValue(stream[i], 1, 1);
    }

    int numSketches = 1;

    for (int h = 0; h < numSketches; h++) {
      FrequentLongsSketch sketch = newFrequencySketch(error_tolerance);

      for (int trial = 0; trial < trials; trial++) {
        sketch = newFrequencySketch(error_tolerance);
        for (int i = 0; i < n; i++) {
          // long item = randomGeometricDist(prob);
          sketch.update(stream[i]);
        }
        long sum = 0;
        long max_error = 0;
        long error;
        long max_freq = 0;

        long[] keys = realCounts.keys();
        //println("size: "+keys.length);
        for (long the_item : keys) {
          if (realCounts.get(the_item) > max_freq) {
            max_freq = realCounts.get(the_item);
          }
          if (realCounts.get(the_item) > sketch.getEstimate(the_item)) {
            error = (realCounts.get(the_item) - sketch.getEstimate(the_item));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          } else {
            error = (sketch.getEstimate(the_item) - realCounts.get(the_item));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          }
        }
        double v = 2 * n * error_tolerance;
        //println("max_error: "+max_error+" <= : "+v);
        Assert.assertTrue(max_error <= v);
      }
    }
  }

  @Test
  private static void errorTestZipfSmallParam() {
    int size = 512;
    int n = 200 * size;
    //double delta = .1;
    double error_tolerance = 1.0 / size;
    int trials = 1;
    long stream[] = new long[n];

    double zet = zeta(n, 0.7);
    TLongLongHashMap realCounts = new TLongLongHashMap();

    for (int i = 0; i < n; i++) {
      stream[i] = zipf(0.7, n, zet);
      realCounts.adjustOrPutValue(stream[i], 1, 1);
    }

    int numSketches = 1;

    for (int h = 0; h < numSketches; h++) {
      FrequentLongsSketch sketch = newFrequencySketch(error_tolerance);

      for (int trial = 0; trial < trials; trial++) {
        sketch = newFrequencySketch(error_tolerance);
        for (int i = 0; i < n; i++) {
          // long item = randomGeometricDist(prob);
          sketch.update(stream[i]);
        }
        long sum = 0;
        long max_error = 0;
        long error;
        long max_freq = 0;

        long[] keys = realCounts.keys();
        for (long the_item : keys) {
          if (realCounts.get(the_item) > max_freq) {
            max_freq = realCounts.get(the_item);
          }
          if (realCounts.get(the_item) > sketch.getEstimate(the_item)) {
            error = (realCounts.get(the_item) - sketch.getEstimate(the_item));
            if (error > max_error) {
              max_error = error;
            }
            sum = sum + error;
          } else {
            error = (sketch.getEstimate(the_item) - realCounts.get(the_item));
            if (error > max_error) {
              max_error = error;
            }
            sum = sum + error;
          }
        }
        double v = 2 * n * error_tolerance;
        //println("max_error: "+max_error+" <= : "+v);
        Assert.assertTrue(max_error <= v);
      }
    }
  }

  @Test
  private static void errorTestZipfBigParamSmallSketch() {
    int size = 64;
    int n = 200 * size;
    //double delta = .1;
    double error_tolerance = 1.0 / size;
    int trials = 1;
    long stream[] = new long[n];

    double zet = zeta(n, 1.1);
    TLongLongHashMap realCounts = new TLongLongHashMap();

    for (int i = 0; i < n; i++) {
      stream[i] = zipf(1.1, n, zet);
      realCounts.adjustOrPutValue(stream[i], 1, 1);
    }

    int numSketches = 1;

    for (int h = 0; h < numSketches; h++) {
      FrequentLongsSketch sketch = newFrequencySketch(error_tolerance);

      for (int trial = 0; trial < trials; trial++) {
        sketch = newFrequencySketch(error_tolerance);
        for (int i = 0; i < n; i++) {
          // long item = randomGeometricDist(prob);
          sketch.update(stream[i]);
        }
        long sum = 0;
        long max_error = 0;
        long error;
        long max_freq = 0;

        long[] keys = realCounts.keys();
        for (long the_item : keys) {
          if (realCounts.get(the_item) > max_freq) {
            max_freq = realCounts.get(the_item);
          }
          if (realCounts.get(the_item) > sketch.getEstimate(the_item)) {
            error = (realCounts.get(the_item) - sketch.getEstimate(the_item));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          } else {
            error = (sketch.getEstimate(the_item) - realCounts.get(the_item));
            if (error > max_error)
              max_error = error;
            sum = sum + error;
          }
        }
        double v = 2 * n * error_tolerance;
        //println("max_error: "+max_error+" <= : "+v);
        Assert.assertTrue(max_error <= v);
      }
    }
  }

  //@Test
  public void freqItemsStressTest() {
    FrequentLongsSketch fls = new FrequentLongsSketch(512);
    int u = 1 << 20;
    for (int i = 0; i< u; i++) {
      fls.update(randomGeometricDist(0.002));
    }
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetInstanceMemory() {
    NativeMemory mem = new NativeMemory(new byte[4]);
    FrequentLongsSketch.getInstance(mem);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetInstanceString() {
    String s = "";
    FrequentLongsSketch.getInstance(s);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkUpdateNegative() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    fls.update(1, 0);
    fls.update(1, -1);
  }
  
  @Test
  public void checkSerToMem() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    byte[] byteArr = fls.serializeToByteArray();
    assertEquals(byteArr.length, 8);
    
    NativeMemory mem = new NativeMemory(new byte[8]);
    fls.serializeToMemory(mem);
    assertEquals(mem.getCapacity(), 8);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkSerToMemException() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    NativeMemory mem = new NativeMemory(new byte[4]);
    fls.serializeToMemory(mem);
  }
  
  @Test
  public void checkGetFrequentItems1() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    fls.update(1);
    long[] itemArr = fls.getFrequentItems(0, ErrorType.NO_FALSE_POSITIVES);
    assertEquals(itemArr[0], 1);
  }
  
  @Test
  public void checkGetStorageBytes() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    int bytes = fls.getStorageBytes();
    assertEquals(bytes, 8);
    fls.update(1);
    bytes = fls.getStorageBytes();
    assertEquals(bytes, 64);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void checkDeSerFromStringArray() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    String ser = fls.serializeToString();
    //println(ser);
    fls.update(1);
    ser = fls.serializeToString();
    //println(ser);
  }
  
  @Test
  public void checkMerge() {
    FrequentLongsSketch fls1 = new FrequentLongsSketch(4);
    FrequentLongsSketch fls2 = null;
    FrequentLongsSketch fle = fls1.merge(fls2);
    assertTrue(fle.isEmpty());
    
    fls2 = new FrequentLongsSketch(4);
    fle = fls1.merge(fls2);
    assertTrue(fle.isEmpty());
  }
  
  @Test
  public void checkSortItems() {
    int numSketches = 1; 
    int n = 2222;
    double error_tolerance = 1.0/100;
    int sketchSize = Util.ceilingPowerOf2((int) (1.0 /(error_tolerance*ReversePurgeLongHashMap.getLoadFactor())));
    println("sketchSize: "+sketchSize);
    
    FrequentLongsSketch[] sketches = new FrequentLongsSketch[numSketches];
    for (int h = 0; h < numSketches; h++) {
      sketches[h] = new FrequentLongsSketch(sketchSize);
    }

    long item;
    double prob = .001;
    for (int i = 0; i < n; i++) {
      item = randomGeometricDist(prob) + 1;
      for (int h = 0; h < numSketches; h++)
        sketches[h].update(item);
    }
    
    long threshold = 10;
    for(int h=0; h<numSketches; h++) {
      long[] freq = sketches[h].getFrequentItems(threshold, ErrorType.NO_FALSE_NEGATIVES);

      for (int i = 0; i < freq.length; i++)
        Assert.assertTrue(sketches[h].getUpperBound(freq[i]) > threshold);
    }
  }
  
  //Restricted methods
  
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

  // This draws values from the zipf distribution
  // n is range, theta is skewness parameter
  // theta = 0 gives uniform distribution,
  // theta > 1 gives highly skewed distribution.
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

  private static FrequentLongsSketch newFrequencySketch(double error_parameter) {
    return new FrequentLongsSketch(
        Util.ceilingPowerOf2((int) (1.0 /(error_parameter*ReversePurgeLongHashMap.getLoadFactor()))));
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
