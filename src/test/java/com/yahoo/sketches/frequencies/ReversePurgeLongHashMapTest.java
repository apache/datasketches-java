/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.frequencies;

import static org.testng.Assert.*;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import gnu.trove.function.TLongFunction;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TLongLongProcedure;

public class ReversePurgeLongHashMapTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkgetInstanceString() {
    ReversePurgeLongHashMap.getInstance("");
  }
  
  @Test
  public void checkActiveNull() {
    ReversePurgeLongHashMap map = new ReversePurgeLongHashMap(4);
    assertNull(map.getActiveKeys());
    assertNull(map.getActiveValues());
  }
  
  @Test //test only what we use
  public void testHashMap() {
    int mapLength = 128;
    ReversePurgeLongHashMap hashMap = new ReversePurgeLongHashMap(mapLength);
    testHashMapAgainstTrove(hashMap, mapLength);
    println(hashMap.toString());
  }
  
  private void testHashMapAgainstTrove(ReversePurgeLongHashMap hashMap, int mapLength){
    Random random = new Random(); 
    random.setSeed(422);
    int valueRange = 35219;
    int keyRange = 11173;

    String s = String.format("Test: %s\n", hashMap.getClass().getSimpleName());
    println(s);
    
    // Trove is a the gold standard
    TLongLongHashMap trove = new TLongLongHashMap(mapLength);
    // Insert random keys and values
    for (int i=0; i<.75*mapLength;i++) {
      long key = random.nextInt(keyRange);
      long value = random.nextInt(valueRange);
      hashMap.adjustOrPutValue(key ,value, value);
      trove.adjustOrPutValue(key, value, value);
    }
    
    // remove a bunch of values
    long threshold = valueRange/2;
    
    hashMap.adjustAllValuesBy(-threshold);
    hashMap.keepOnlyPositiveCounts();
    
    trove.retainEntries(new GreaterThenThreshold(threshold));
    trove.transformValues(new decreaseByThreshold(threshold));
    
    long[] keys = hashMap.getActiveKeys();
    long[] values = hashMap.getActiveValues();
    int size = hashMap.getNumActive();
    
    // map is of the correct size
    Assert.assertEquals(trove.size(), size);
    // keys and values of the same correct length
    Assert.assertEquals(size, keys.length);
    Assert.assertEquals(size, values.length);
    
    // All the keys and values are correct
    for (int i=0;i<size;i++){
      Assert.assertTrue(trove.containsKey(keys[i]));
      Assert.assertEquals(trove.get(keys[i]), values[i]);
      // Testing the get function
      Assert.assertEquals(values[i], hashMap.get(keys[i]));
    }
  }
  
  private class GreaterThenThreshold implements TLongLongProcedure {
    long threshold;
    public GreaterThenThreshold(long threshold){
      this.threshold = threshold;
    }
    
    @Override
    public boolean execute(long key, long value) {
      return (value > threshold);
    }
  }
  
  private class decreaseByThreshold implements TLongFunction {
    long threshold;
    public decreaseByThreshold(long threshold){
      this.threshold = threshold;
    }
    
    @Override
    public long execute(long value) {
      return value - threshold;
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
    //System.out.println(s); //disable here
  }
  
}
