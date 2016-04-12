package com.yahoo.sketches.frequencies;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.sketches.frequencies.HashMap;
import com.yahoo.sketches.frequencies.HashMapReverseEfficient;

import gnu.trove.function.TLongFunction;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TLongLongProcedure;

public class HashMapTest {
  
//  private static HashMap newHashMap(int mapLength, int i){
//    HashMap hashMap = null;
//    switch (i){
//      case 0: hashMap = new HashMapReverseEfficient(mapLength); break;
//      case 1: //hashMap = new HashMapTrove(mapLength); break;
//      case 2: //hashMap = new HashMapTroveRebuilds(mapLength); break;
//      case 3: //hashMap = new HashMapLinearProbingWithRebuilds(mapLength); break;
//      case 4: //hashMap = new HashMapDoubleHashingWithRebuilds(mapLength); break;
//      case 5: //hashMap = new HashMapWithImplicitDeletes(mapLength); break;
//      case 6: //hashMap = new HashMapWithEfficientDeletes(mapLength); break;
//      case 7: //hashMap = new HashMapRobinHood(mapLength); break; 
//      case 8: //hashMap = new HashMapReverseEfficientOneArray(mapLength); break;
//      default:
//    } 
//    return hashMap;
//  }
//  
//  @Test
//  public void testHashMaps() {
//    int mapLength = 127;
//    // Looping over all hashMap types
//    HashMap hashMap = null;
//    for (int h=0; h<10; h++) {
//      hashMap = newHashMap(mapLength, h);
//      if (hashMap == null) continue;
//      testHashMapAgainstTrove(hashMap, mapLength);
//    }
//  }
  
  @Test //test only what we use
  public void testHashMap() {
    int mapLength = 128;
    HashMap hashMap = new HashMapReverseEfficient(mapLength);
    testHashMapAgainstTrove(hashMap, mapLength);
    println(hashMap.toString());
  }
  
  private void testHashMapAgainstTrove(HashMap hashMap, int mapLength){
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
    hashMap.keepOnlyLargerThan(0);
    
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