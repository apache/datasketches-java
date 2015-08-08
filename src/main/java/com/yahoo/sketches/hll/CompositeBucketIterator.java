package com.yahoo.sketches.hll;

/**
 */
public class CompositeBucketIterator implements BucketIterator
{
  private final BucketIterator[] iters;

  private int index = 0;

  public CompositeBucketIterator(BucketIterator... iters) {
    this.iters = iters;
  }

  @Override
  public boolean next()
  {
    while (index < iters.length && !iters[index].next()) {
      iters[index] = null; // give up the reference
      ++index;
    }
    return index < iters.length;
  }

  @Override
  public int getKey()
  {
    return iters[index].getKey();
  }

  @Override
  public byte getValue()
  {
    return iters[index].getValue();
  }
}
