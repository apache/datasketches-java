package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.frequencies.DistTest.*;
import static com.yahoo.sketches.frequencies.PreambleUtil.*;
import static com.yahoo.sketches.Util.LS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

import com.yahoo.sketches.Util;
import com.yahoo.sketches.frequencies.FrequentLongsSketch.ErrorType;
import com.yahoo.sketches.frequencies.FrequentLongsSketch.Row;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import gnu.trove.map.hash.TLongLongHashMap;

public class FrequentLongsSketchTest {

  public static void main(String[] args) {
    FrequentLongsSketchTest test = new FrequentLongsSketchTest();
    test.hashMapSerialTest();
    System.out.println("Done hashMapSerialTest");
    test.frequentItemsStringSerialTest();
    System.out.println("Done frequentItemsStringSerialTest");
    test.frequentItemsByteSerialTest();
    System.out.println("Done frequentItemsByteSerialTest");
    test.frequentItemsByteResetAndEmptySerialTest();
    System.out.println("Done frequentItemsByteResetAndEmptySerialTest");
    test.checkFreqLongs();
    System.out.println("Done FE Test");
    test.realCountsInBoundsAfterMerge();
    System.out.println("Done realCountsoInBoundsAFterMerge Test");
    test.strongMergeTest();
    System.out.println("Done strongMergeTest");
    test.updateOneTime();
    System.out.println("Done updateOneTime Test");
    test.errorTestZipfBigParam();
    System.out.println("Done errorTestZipfBigParam");
    test.errorTestZipfSmallParam();
    System.out.println("Done errorTestZipfSmallParam");
    test.errorTestZipfBigParamSmallSketch();
    System.out.println("Done errorTestZipfBigParamSmallSketch");
  }

  @Test
  public void hashMapSerialTest() {
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

  @Test
  public void frequentItemsStringSerialTest() {
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

  @SuppressWarnings("unused")
  @Test
  public void frequentItemsByteSerialTest() {
    //Empty Sketch
    FrequentLongsSketch sketch = new FrequentLongsSketch(16);
    byte[] bytearray0 = sketch.serializeToByteArray();
    Memory mem0 = new NativeMemory(bytearray0);
    FrequentLongsSketch new_sketch0 = FrequentLongsSketch.getInstance(mem0);
    String str0 = PreambleUtil.preambleToString(mem0);
    //println(str0);
    String string0 = sketch.serializeToString();
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    
    FrequentLongsSketch sketch2 = new FrequentLongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    byte[] bytearray1 = sketch.serializeToByteArray();
    Memory mem1 = new NativeMemory(bytearray1);
    FrequentLongsSketch new_sketch1 = FrequentLongsSketch.getInstance(mem1);
    String str1 = PreambleUtil.preambleToString(mem1);
    //println(str1);
    String string1 = sketch.serializeToString();
    String new_string1 = new_sketch1.serializeToString();
    Assert.assertTrue(string1.equals(new_string1));
    Assert.assertTrue(new_sketch1.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch1.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());

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
  public void frequentItemsByteResetAndEmptySerialTest() {
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
  public void checkFreqLongsMemSerDe() {
    FrequentLongsSketch sk1 = new FrequentLongsSketch(4);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443);
    sk1.update(1000001, 1010230);
    sk1.update(1000002, 1010230);
    
    byte[] bytearray0 = sk1.serializeToByteArray();
    Memory mem0 = new NativeMemory(bytearray0);
    FrequentLongsSketch sk2 = FrequentLongsSketch.getInstance(mem0);
    
    checkEquality(sk1, sk2);
  }
  
  @Test
  public void checkFreqLongsStringSerDe() {
    FrequentLongsSketch sk1 = new FrequentLongsSketch(16);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443);
    sk1.update(1000001, 1010230);
    sk1.update(1000002, 1010230);
    
    String string1 = sk1.serializeToString();
    FrequentLongsSketch sk2 = FrequentLongsSketch.getInstance(string1);
    
    checkEquality(sk1, sk2);
  }
  
  private static void checkEquality(FrequentLongsSketch sk1, FrequentLongsSketch sk2) {
    assertEquals(sk1.getNumActiveItems(), sk2.getNumActiveItems());
    assertEquals(sk1.getCurrentMapCapacity(), sk2.getCurrentMapCapacity());
    assertEquals(sk1.getMaximumError(), sk2.getMaximumError());
    assertEquals(sk1.getMaximumMapCapacity(), sk2.getMaximumMapCapacity());
    assertEquals(sk1.getStorageBytes(), sk2.getStorageBytes());
    assertEquals(sk1.getStreamLength(), sk2.getStreamLength());
    assertEquals(sk1.isEmpty(), sk2.isEmpty());
    
    ErrorType NFN = ErrorType.NO_FALSE_NEGATIVES;
    ErrorType NFP = ErrorType.NO_FALSE_POSITIVES;
    Row[] rowArr1 = sk1.getFrequentItems(NFN);
    Row[] rowArr2 = sk2.getFrequentItems(NFN);
    assertEquals(sk1.getFrequentItems(NFN).length, sk2.getFrequentItems(NFN).length);
    for (int i=0; i<rowArr1.length; i++) {
      String s1 = rowArr1[i].toString();
      String s2 = rowArr2[i].toString();
      assertEquals(s1, s2);
    }
    rowArr1 = sk1.getFrequentItems(NFP);
    rowArr2 = sk2.getFrequentItems(NFP);
    assertEquals(sk1.getFrequentItems(NFP).length, sk2.getFrequentItems(NFP).length);
    for (int i=0; i<rowArr1.length; i++) {
      String s1 = rowArr1[i].toString();
      String s2 = rowArr2[i].toString();
      assertEquals(s1, s2);
    }
  }
  
  @Test
  public void checkFreqLongsMemDeSerExceptions() {
    FrequentLongsSketch sk1 = new FrequentLongsSketch(4);
    sk1.update(1L);
    
    byte[] bytearray0 = sk1.serializeToByteArray();
    Memory mem = new NativeMemory(bytearray0);
    long pre0 = mem.getLong(0); 
    
    tryBadMem(mem, PREAMBLE_LONGS_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, SER_VER_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FAMILY_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FLAGS_BYTE, 4); //Corrupt to true
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FREQ_SKETCH_TYPE_BYTE, 2);
  }
  
  private static void tryBadMem(Memory mem, int byteOffset, int byteValue) {
    try {
      mem.putByte(byteOffset, (byte) byteValue); //Corrupt
      FrequentLongsSketch.getInstance(mem);
      fail();
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  @Test
  public void checkFreqLongsStringDeSerExceptions() {
    //FrequentLongsSketch sk1 = new FrequentLongsSketch(4);
    //String str1 = sk1.serializeToString();
    //String correct   = "1,10,2,4,1,0,0,0,4,";
    
    tryBadString("2,10,2,4,1,0,0,0,4,"); //bad SerVer
    tryBadString("1,10,2,0,1,0,0,0,4,"); //bad empty
    tryBadString("1,10,2,4,2,0,0,0,4,"); //bad type
    tryBadString("1,10,2,4,1,0,0,0,4,0,"); //one extra
  }
  
  private static void tryBadString(String badString) {
    try {
      FrequentLongsSketch.getInstance(badString);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  @Test
  public void checkFreqLongs(){
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
    
    for(int h=0; h<numSketches; h++) {
      long threshold = sketches[h].getMaximumError();
      Row[] rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].getUpperBound() > threshold);
      }
      
      rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].getLowerBound() > threshold);
      }
    }
  }

  @Test
  public void realCountsInBoundsAfterMerge() {
    int n = 10000;
    int size = 150;
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
      
      int maxMapCap=merged.getMaximumMapCapacity();
      double delta=2.0/maxMapCap;
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
  public void strongMergeTest() {
    int n = 10000;
    int size = 150;
    double error_tolerance = 1.0 / size;
    int num_to_merge = 10;
    FrequentLongsSketch[] sketches = new FrequentLongsSketch[num_to_merge];
    
    double prob = .01;
    int numSketches = 1;
    
    long totalstreamlength = 0;
    
    int maxMapCap = 0;

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
          totalstreamlength++;
        }
      }

      FrequentLongsSketch merged = sketches[0];
      for (int z = 0; z < num_to_merge; z++) {
        if (z == 0)
          continue;
        merged = merged.merge(sketches[z]);
      }
      
      maxMapCap=merged.getMaximumMapCapacity();
      double delta=2.0/maxMapCap;

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
      Assert.assertTrue(totalstreamlength == merged.getStreamLength());
    }
  }

  @Test
  public void updateOneTime() {
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
  public void errorTestZipfBigParam() {
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
  public void errorTestZipfSmallParam() {
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
  public void errorTestZipfBigParamSmallSketch() {
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
  public void checkGetFrequentItems1() {
    FrequentLongsSketch fls = new FrequentLongsSketch(4);
    fls.update(1);
    Row[] rowArr = fls.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    assertEquals(rowArr[0].est, 1);
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
  
  @SuppressWarnings("unused")
  @Test
  public void checkSortItems() {
    int numSketches = 1; 
    int n = 2222;
    double error_tolerance = 1.0/100;
    int sketchSize = Util.ceilingPowerOf2((int) (1.0 /(error_tolerance*ReversePurgeLongHashMap.getLoadFactor())));
    //println("sketchSize: "+sketchSize);
    
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
    
    for(int h=0; h<numSketches; h++) {
      long threshold = sketches[h].getMaximumError();
      Row[] rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      //println("ROWS: "+rows.length);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].ub > threshold);
      }
      Row first = rows[0];
      long anItem = first.getItem();
      long anEst  = first.getEstimate();
      long aLB    = first.getLowerBound();
      String s = first.toString();
      //println(s);
      assertTrue(anEst >= 0);
      assertTrue(aLB >= 0);
      assertEquals(anItem, anItem); //dummy test
    }
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkGetAndCheckPreLongs() {
    byte[] byteArr = new byte[8];
    byteArr[0] = (byte) 2;
    PreambleUtil.checkPreambleSize(new NativeMemory(byteArr));
  }
  
  @Test
  public void checkToString1() {
    int size = 4;
    printSketch(size, new long[] {1,10,100, 1000});
    printSketch(size, new long[] {1000,100,10, 1});
  }

  
  public void printSketch(int size, long[] freqArr) {
    FrequentLongsSketch fls = new FrequentLongsSketch(size);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i<freqArr.length; i++) {
      fls.update(i+1, freqArr[i]);
    }
    sb.append("Sketch Size: "+size).append(LS);
    String s = fls.toString();
    sb.append(s);
    println(sb.toString());
    printRows(fls, ErrorType.NO_FALSE_NEGATIVES);
    println("");
    printRows(fls, ErrorType.NO_FALSE_POSITIVES);
    println("");
  }

  private static void printRows(FrequentLongsSketch fls, ErrorType eType) {
    Row[] rows = fls.getFrequentItems(eType);
    String s1 = eType.toString();
    println(s1);
    for (int i=0; i<rows.length; i++) {
      String s2 = rows[i].toString();
      println(s2);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }
  
  //Restricted methods
  
  private static FrequentLongsSketch newFrequencySketch(double error_parameter) {
    return new FrequentLongsSketch(
        Util.ceilingPowerOf2((int) (1.0 /(error_parameter*ReversePurgeLongHashMap.getLoadFactor()))));
  }

}
