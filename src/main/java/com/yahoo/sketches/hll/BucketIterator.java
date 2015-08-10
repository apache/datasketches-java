package com.yahoo.sketches.hll;

public interface BucketIterator
{
  boolean next();

  int getKey();

  byte getValue();

}
