package com.yahoo.sketches.hll;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.memory.NativeMemory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 */
public class OnHeapCompressedFieldsTest
{
  OnHeapCompressedFields fields;
  private Preamble preamble;

  @BeforeMethod
  public void setUp() throws Exception
  {
    preamble = Preamble.fromLogK(10);
    fields = new OnHeapCompressedFields(preamble);
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
    for (int i = 0; i < 20; ++i) {
      fields.updateBucket(i, (byte) (i+1), Fields.NOOP_CB);
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
    TestUpdateCallback.assertVals(cb, 1, 0, 2);

    fields.updateBucket(2, (byte) 4, cb);
    TestUpdateCallback.assertVals(cb, 2, 2, 4);

    fields.updateBucket(2, (byte) 1, cb);
    TestUpdateCallback.assertVals(cb, 2, 2, 4);

    fields.updateBucket(2, (byte) 27, cb);
    TestUpdateCallback.assertVals(cb, 3, 4, 27);
  }

  @Test
  public void testIntoByteArray() throws Exception
  {
    byte[] stored = new byte[fields.numBytesToSerialize()];
    byte[] expected = new byte[stored.length];
    expected[0] = 0x3;
    NativeMemory expectedMem = new NativeMemory(expected);
    expectedMem.putInt(2, fields.getPreamble().getConfigK());
    for(int i = 518; i < expected.length; ++i) {
      expected[i] = -1;
    }

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(2, (byte) 27, Fields.NOOP_CB);
    expectedMem.putInt(2, expectedMem.getInt(2) - 1);
    expected[7] = (byte) 0xf0;
    expectedMem.putInt(
        1 + 5 + (preamble.getConfigK() >>> 1) + 8,
        HashUtils.pairOfKeyAndVal(2, (byte) 27)
    );

    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(892, (byte) 0xa, Fields.NOOP_CB);
    expectedMem.putInt(2, expectedMem.getInt(2) - 1);
    expected[452] = (byte) 0xa0;
    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(892, (byte) 0x7, Fields.NOOP_CB);
    expected[452] = (byte) 0xa0;
    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(892, (byte) 0xb, Fields.NOOP_CB);
    expected[452] = (byte) 0xb0;
    fields.intoByteArray(stored, 0);
    Assert.assertEquals(stored, expected);

    fields.updateBucket(893, (byte) 0xa, Fields.NOOP_CB);
    expectedMem.putInt(2, expectedMem.getInt(2) - 1);
    expected[452] = (byte) 0xba;
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
    Assert.assertEquals(fields.numBytesToSerialize(), 1 + 5 + 512 + 64);
  }

  @Test
  public void testToCompact() throws Exception
  {
    Assert.assertSame(fields.toCompact(), fields);
  }
}
