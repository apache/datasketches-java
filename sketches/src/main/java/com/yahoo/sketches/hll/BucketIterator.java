/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * A BucketIterator is an iterator over bucket values in an Hll Fields object.
 *
 * <p>The general pattern for usage is you call next() and if that returns true, you will get
 * values from getKey() and getValue().  If next() returned false, that means that iteration is
 * complete; getKey() and getValue() become undefined.</p>
 */
public interface BucketIterator {
  /**
   * Should be called before each step of iteration (as well as immediately after initialization)
   *
   * @return true if there is a valid bucket to return, false if iteration is complete
   */
  boolean next();

  /**
   * Should be called before each step of iteration (as well as immediately after initialization)
   *
   * @return true if there is a bucket to return, false if iteration is complete
   */
  boolean nextAll();

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
