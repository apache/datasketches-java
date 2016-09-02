/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class FieldsTest
{
  @DataProvider(name = "updatableFields")
  public Object[][] getFields()
  {
    Preamble preamble = Preamble.fromLogK(10);

    return new Object[][]{
        {new OnHeapFields(preamble)},
        { new OnHeapHashFields(preamble, 16, 0x2<<preamble.getLogConfigK(), new DenseFieldsFactory()) },
        { new OnHeapCompressedFields(preamble) }
    };
  }

  @Test(dataProvider = "updatableFields")
  public void testUpdateBucketBreadthFirst(Fields fields)
  {
    final AtomicReference<Integer[]> callbackArgs = new AtomicReference<>(null);
    Fields.UpdateCallback cb = new Fields.UpdateCallback()
    {
      @Override
      public void bucketUpdated(int bucket, byte oldVal, byte newVal)
      {
        Assert.assertNull(callbackArgs.get());
        callbackArgs.set(new Integer[]{bucket, (int) oldVal, (int) newVal});
      }
    };

    Random rand = new Random(1234L);
    int numBuckets = fields.getPreamble().getConfigK();

    List<Integer> indexes = new ArrayList<>(numBuckets);
    int[] vals = new int[numBuckets];
    for (int i = 0; i < numBuckets; ++i) {
      indexes.add(i);
    }
    for (int val = 1; val < 64; ++val) {
      Collections.shuffle(indexes, rand);

      for (int i = 0; i < numBuckets; ++i) {
        int index = indexes.get(i);
        fields = fields.updateBucket(index, (byte) val, cb);
        int oldVal = vals[index];
        vals[index] = Math.max(oldVal, val);

        if (vals[index] == val) {
          // We got a new value, so the callback should have been called
          ensureEquals(callbackArgs.get(), new Integer[]{index, oldVal, val});
          callbackArgs.set(null);
        } else {
          // No new value, so nothing should have been updated.
          Assert.assertNull(callbackArgs.get());
        }

        BucketIterator iter = fields.getBucketIterator();
        while (iter.next()) {
          int bucketId = iter.getKey();
          if (iter.getValue() != vals[bucketId]) {
            Assert.fail(String.format("bucket[%s]: %d != %d", bucketId, iter.getValue(), vals[bucketId]));
          }
        }
      }
    }
  }

  @Test(dataProvider = "updatableFields")
  public void testUpdateBucketDepthFirst(Fields fields)
  {
    final AtomicReference<Integer[]> callbackArgs = new AtomicReference<>(null);
    Fields.UpdateCallback cb = new Fields.UpdateCallback()
    {
      @Override
      public void bucketUpdated(int bucket, byte oldVal, byte newVal)
      {
        Assert.assertNull(callbackArgs.get());
        callbackArgs.set(new Integer[]{bucket, (int) oldVal, (int) newVal});
      }
    };

    Random rand = new Random(1234L);
    int numBuckets = fields.getPreamble().getConfigK();

    List<Byte> valsToInsert = new ArrayList<>(64);
    int[] actualVals = new int[numBuckets];
    for (byte i = 1; i < 64; ++i) {
      valsToInsert.add(i);
    }
    for (int bucket = 0; bucket < numBuckets; ++bucket) {
      Collections.shuffle(valsToInsert, rand);

      for (Byte val : valsToInsert) {
        if (bucket == 351 && val == 25) {
          println(""+bucket);
        }
        fields = fields.updateBucket(bucket, val, cb);
        int oldVal = actualVals[bucket];
        actualVals[bucket] = Math.max(oldVal, val);

        if (actualVals[bucket] == val) {
          // We got a new value, so the callback should have been called
          ensureEquals(callbackArgs.get(), new Integer[]{bucket, oldVal, (int) val});
          callbackArgs.set(null);
        } else {
          // No new value, so nothing should have been updated.
          Assert.assertNull(callbackArgs.get());
        }

        BucketIterator iter = fields.getBucketIterator();
        while (iter.next()) {
          int bucketId = iter.getKey();
          if (iter.getValue() != actualVals[bucketId]) {
            Assert.fail(String.format("bucket[%s]: %d != %d", bucketId, iter.getValue(), actualVals[bucketId]));
          }
        }
      }
    }
  }

  private static void ensureEquals(Integer[] callbackArgs, Integer[] expected)
  {
    if (!Arrays.equals(expected, callbackArgs)) {
      Assert.fail(
          String.format(
              "Expected array %s, got array %s", Arrays.toString(expected), Arrays.toString(callbackArgs)
          )
      );
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print 
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
