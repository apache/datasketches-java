package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 */
public class HashUtilsTest
{
  @Test
  public void testSanity() throws Exception
  {
    int numKeys = 1 << 20;
    for (int key = 0; key < numKeys; ++key) {
      for (byte val = 0; val < 64; ++val) {
        int pair = HashUtils.pairOfKeyAndVal(key, val);
        Assert.assertEquals(HashUtils.keyOfPair(pair), key);
        Assert.assertEquals(HashUtils.valOfPair(pair), val);
      }
    }
  }
}
