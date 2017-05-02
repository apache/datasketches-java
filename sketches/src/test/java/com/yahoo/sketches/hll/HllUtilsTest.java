/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 */
public class HllUtilsTest {

  @Test
  public void testInvPow2ComputesEmptyBuckets() throws Exception
  {
    Assert.assertEquals(
        20.0,
        HllUtils.computeInvPow2Sum(20, new ArrayBucketIterator(new int[]{}, new byte[]{}))
    );
  }

  @Test
  public void testInvPow2AggregatesBuckets() throws Exception {
    Assert.assertEquals(
        19.0 + Math.pow(2.0, -1.0 * 3),
        HllUtils.computeInvPow2Sum(20, new ArrayBucketIterator(new int[]{49}, new byte[]{3}))
    );
  }

  private static class ArrayBucketIterator implements BucketIterator {
    private final int[] keys;
    private final byte[] vals;

    private int i = -1;

    public ArrayBucketIterator(int[] keys, byte[] vals) {
      this.keys = keys;
      this.vals = vals;
    }

    @Override
    public boolean next() {
      return ++i < keys.length;
    }

    @Override
    public boolean nextAll() {
      return ++i < keys.length;
    }

    @Override
    public int getKey() {
      return keys[i];
    }

    @Override
    public byte getValue() {
      return vals[i];
    }
  }
}
