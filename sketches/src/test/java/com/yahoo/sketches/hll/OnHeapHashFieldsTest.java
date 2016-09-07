/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import java.util.LinkedHashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 */
public class OnHeapHashFieldsTest
{
  private static final Fields.UpdateCallback cb = new Fields.UpdateCallback()
  {
    @Override
    public void bucketUpdated(int bucket, byte oldVal, byte newVal)
    {

    }
  };

  OnHeapHashFields fields;
  private Preamble preamble;

  @BeforeMethod
  public void setUp() throws Exception
  {
    preamble = Preamble.fromLogK(10);
    fields = new OnHeapHashFields(preamble, 16, 0x2<<preamble.getLogConfigK(), new DenseFieldsFactory());
  }

  @Test
  public void testGetPreamble() throws Exception
  {
    Assert.assertSame(fields.getPreamble(), preamble);
  }

  @Test
  public void testUpdateBucket() throws Exception
  {
    Set<Integer> expectedKeys = new LinkedHashSet<>();
    for (int i = 0; i < 10; ++i) {
      fields.updateBucket(i, (byte) (i+1), cb);
      expectedKeys.add(i);
    }

    BucketIterator bucketIter = fields.getBucketIterator();
    while(bucketIter.next()) {
      Assert.assertTrue(expectedKeys.remove(bucketIter.getKey()));
      Assert.assertEquals(bucketIter.getValue(), bucketIter.getKey() + 1);
    }
    Assert.assertTrue(expectedKeys.isEmpty(), "expectedKeys wasn't empty, all keys should have been removed.");
  }

  @Test
  public void testUpdateBucketCallsCallback() throws Exception
  {
    TestUpdateCallback cb2 = new TestUpdateCallback();
    cb2.setExpectedBucket(2);

    TestUpdateCallback.assertVals(cb2, 0, 0, 0);

    fields.updateBucket(2, (byte) 2, cb2);
    TestUpdateCallback.assertVals(cb2, 1, 0, 2);

    fields.updateBucket(2, (byte) 4, cb2);
    TestUpdateCallback.assertVals(cb2, 2, 2, 4);

    fields.updateBucket(2, (byte) 1, cb2);
    TestUpdateCallback.assertVals(cb2, 2, 2, 4);

    fields.updateBucket(2, (byte) 9, cb2);
    TestUpdateCallback.assertVals(cb2, 3, 4, 9);
  }

  @Test
  public void testIntoByteArray() throws Exception
  {
    byte[] stored = new byte[fields.numBytesToSerialize()];
    byte[] expected = new byte[stored.length];
    Memory expectedMem = new NativeMemory(expected);
    expectedMem.putByte(0, (byte) 0x1);
    for (int i = 1; i < 65; i+=4) {
      expectedMem.putInt(i, -1);
    }

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(2, (byte) 27, cb);
    // 9 is a magic number based on the hashing.  If the hashing were to change,
    // then it should also be expected to change
    expectedMem.putInt(9, HashUtils.pairOfKeyAndVal(2, (byte) 27));

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(892, (byte) 10, cb);
    // 49 is a magic number based on the hashing.  If the hashing were to change,
    // then it should also be expected to change.
    expectedMem.putInt(49, HashUtils.pairOfKeyAndVal(892, (byte) 10));

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);


    boolean exceptionThrown = false;
    try {
      fields.intoByteArray(new byte[stored.length - 1], 0);
    } catch (SketchesArgumentException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "Expected exception about length of array to be thrown.");
  }

  @Test
  public void testNumBytesToSerialize() throws Exception
  {
    Assert.assertEquals(fields.numBytesToSerialize(), 1 + (16 * 4));


    for (int i = 0; i < 11; ++i) {
      fields.updateBucket(i, (byte) 1, cb);
      Assert.assertEquals(fields.numBytesToSerialize(), 1 + (16 * 4), String.valueOf(i));
    }

    fields.updateBucket(1023, (byte) 98, cb);
    Assert.assertEquals(fields.numBytesToSerialize(), 1 + (32 * 4));
  }

  @Test
  public void testToCompact() throws Exception
  {
    Assert.assertSame(fields.toCompact().getClass(), OnHeapImmutableCompactFields.class);
  }
}
