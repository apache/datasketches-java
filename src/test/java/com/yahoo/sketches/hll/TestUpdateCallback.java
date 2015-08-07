package com.yahoo.sketches.hll;

import org.testng.Assert;

/**
 */
public class TestUpdateCallback implements Fields.UpdateCallback
{
  public static void assertVals(TestUpdateCallback cb, int count, int newVal, int oldVal) {
    Assert.assertEquals(cb.getCount(), count, "count is off");
    Assert.assertEquals(cb.getOldVal(), (byte) oldVal, "oldVal is off");
    Assert.assertEquals(cb.getNewVal(), (byte) newVal, "newVal is off");
  }


  int count = 0;

  int expectedBucket;
  byte oldVal;
  byte newVal;

  @Override
  public void bucketUpdated(int bucket, byte oldVal, byte newVal)
  {
    Assert.assertEquals(bucket, expectedBucket);
    this.oldVal = oldVal;
    this.newVal = newVal;
    ++count;
  }

  public byte getOldVal()
  {
    return oldVal;
  }

  public byte getNewVal()
  {
    return newVal;
  }

  public int getCount()
  {
    return count;
  }

  public int getExpectedBucket()
  {
    return expectedBucket;
  }

  public void setExpectedBucket(int expectedBucket)
  {
    this.expectedBucket = expectedBucket;
  }
}
