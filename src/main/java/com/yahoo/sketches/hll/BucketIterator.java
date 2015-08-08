package com.yahoo.sketches.hll;

/**
 * A BucketIterator is an iterator over bucket values in an Hll Fields object.
 *
 * The general pattern for usage is you call next() and if that returns true, you will get values from
 * getKey() and getValue().  If next() returned false, that means that iteration is complete; getKey() and
 * getValue() are undefined.
 */
public interface BucketIterator
{
  /**
   * Should be called before each step of iteration (as well as immediately after initialization)
   *
   * @return true if there is a a bucket to return, false if iteration is complete
   */
  boolean next();

  /**
   * Gets the index of the current bucket
   *
   * @return the index of the current bucket
   */
  int getKey();

  /**
   * Gets the value of the current bucket
   *
   * @return the value of the current bucket
   */
  byte getValue();

}
