package com.yahoo.sketches.counting;

import java.util.Random;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Collection;

import org.testng.annotations.Test;
import org.testng.Assert;

public class PositiveCountersMapTest {

	@Test
  public void construct() {
    PositiveCountersMap cs = new PositiveCountersMap(); 
    Assert.assertNotNull(cs);
  }

  @Test
  public void incrementTest() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 4L;
    cs.increment(key);
    Assert.assertTrue(cs.get(key) == 1);
  }

  @Test
  public void incrementWithValueTest() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 4L;
    long delta = 24;
    cs.increment(key,delta);
    Assert.assertTrue(cs.get(key) == delta);
  }
   
  @Test
  public void decrementAllTest() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 4;
    long value = 242;
    cs.put(key,value);
    cs.decerementAll();
    Assert.assertTrue(cs.get(key) == value - 1);
  }
  
  @Test
  public void decrementAllWithValueTest() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 4L;
    long value = 242;
    long delta = 23;
    cs.put(key,value);
    cs.decerementAll(delta);
    Assert.assertTrue(cs.get(key) == value - delta);
  }

  @Test
  public void decrementDeletesNegativeCounts() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 421L;
    long value = 242;
    long delta = 3513;
    cs.put(key,value);
    cs.decerementAll(delta);
    Assert.assertTrue(cs.nnz() == 0);
  }

  @Test
  public void decrementAllAndIncrementHasAnEffectIfDeltaLargerThanValue() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 421;
    long value = 3512;
    long delta = 3513;
    cs.increment(key,value);
    cs.decerementAll(delta);
    cs.increment(key,value);
    Assert.assertTrue(cs.get(key) == value);
  }
  
  @Test
  public void decrementAllAndIncrementHasNoEffectIfDeltaSmallerEqualToValue() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 421;
    long value = 3515;
    long delta = 3513;
    cs.increment(key,value);
    cs.decerementAll(delta);
    cs.increment(key,value);
    Assert.assertTrue(cs.get(key) == 2*value-delta);
  }
  
  @Test
  public void negativeCountersReturnZero() {
    PositiveCountersMap cs = new PositiveCountersMap();  
    long key = 4252L;
    long value = 35;
    long delta = 3513;
    cs.put(key,value);
    cs.decerementAll(delta);
    Assert.assertTrue(cs.get(key) == 0);
  }
  
  @Test
  public void testGetValuesAndGetKeys(){
    int n = 100;
    PositiveCountersMap cs = new PositiveCountersMap();
    HashMap<Long,Long> counters = new HashMap<Long,Long>();
    ArrayList<Long> realValues = new ArrayList<Long>();
    ArrayList<Long> realKeys = new ArrayList<Long>();
    Random random = new Random(); 
    for (int i=0; i<n; i++){
    	long key = random.nextLong();
    	long value = (long) (random.nextInt(1000));
    	if (!counters.containsKey(key)){
    		cs.put(key, value);
    		counters.put(key, value);
    		realKeys.add(key);
    		realValues.add(value);
    	}
    }
    Collections.sort(realKeys);
    ArrayList<Long> testKeys = new ArrayList<Long>(cs.keys());
    Collections.sort(testKeys);
    
    Collections.sort(realValues);
    ArrayList<Long> testValues = new ArrayList<Long>(cs.values());
    Collections.sort(testValues);
    
    Assert.assertEquals(testKeys, realKeys);
    Assert.assertEquals(testValues, realValues);
  }

  @Test
  public void testAddOtherPositiveCounter(){
    int n = 100;
    HashMap<Long,Long> counters = new HashMap<Long,Long>();
    Random random = new Random(); 
    PositiveCountersMap cs1 = new PositiveCountersMap();
    PositiveCountersMap cs2 = new PositiveCountersMap();
    for (int i=0; i<n; i++){
    	long key = random.nextLong();
    	long value = (long) (random.nextInt(1000)+1);
    	if (!counters.containsKey(key)){
    		if (i % 3 == 0) {
    			cs1.put(key, value);
      		counters.put(key, value);
    		}
    		if (i % 3 == 1) {
    			cs2.put(key, value);
      		counters.put(key, value);
    		}
    		if (i % 3 == 2) {
    		cs1.put(key, value);
    		cs2.put(key, value);
    		counters.put(key, 2*value);
    		}
    	}
    }
    cs1.increment(cs2);
    for (Long testkey: counters.keySet()) {
    	Assert.assertEquals(cs1.get(testkey), counters.get(testkey));
    }
  }
  
}