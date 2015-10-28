package com.yahoo.sketches.counting;

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

}