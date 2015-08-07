package com.yahoo.sketches.hll;

/**
 */
public class NoopUpdateCallback implements Fields.UpdateCallback
{
  @Override
  public void bucketUpdated(int bucket, byte oldVal, byte newVal)
  {

  }
}
