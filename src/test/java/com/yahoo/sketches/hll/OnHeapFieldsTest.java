package com.yahoo.sketches.hll;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 */
public class OnHeapFieldsTest
{
  private static final NoopUpdateCallback cb = new NoopUpdateCallback();

  OnHeapFields fields;
  private Preamble preamble;

  @BeforeMethod
  public void setUp() throws Exception
  {
    preamble = Preamble.createSharedPreamble(10);
    fields = new OnHeapFields(preamble);
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
    TestUpdateCallback cb = new TestUpdateCallback();
    cb.setExpectedBucket(2);

    TestUpdateCallback.assertVals(cb, 0, 0, 0);

    fields.updateBucket(2, (byte) 2, cb);
    TestUpdateCallback.assertVals(cb, 1, 2, 0);

    fields.updateBucket(2, (byte) 4, cb);
    TestUpdateCallback.assertVals(cb, 2, 4, 2);

    fields.updateBucket(2, (byte) 1, cb);
    TestUpdateCallback.assertVals(cb, 2, 4, 2);

    fields.updateBucket(2, (byte) 9, cb);
    TestUpdateCallback.assertVals(cb, 3, 9, 4);
  }

  @Test
  public void testIntoByteArray() throws Exception
  {
    byte[] stored = new byte[fields.numBytesToSerialize()];
    byte[] expected = new byte[stored.length];
    expected[0] = 0x0;

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(2, (byte) 27, cb);
    expected[3] = 27;

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(892, (byte) 10, cb);
    expected[893] = 10;

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);
  }

  @Test
  public void testNumBytesToSerialize() throws Exception
  {
    Assert.assertEquals(fields.numBytesToSerialize(), 1025);
  }

  @Test
  public void testToCompact() throws Exception
  {
    Assert.assertSame(fields.toCompact(), fields);
  }
}