/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

/**
 * This is used to iterate over the retained hash values of the Theta sketch.
 * @author Lee Rhodes
 */
public interface HashIterator {

  /**
   * Gets the hash value
   * @return the hash value
   */
  long get();

  /**
   * Returns true at the next hash value in sequence.
   * If false, the iteration is done.
   * @return true at the next hash value in sequence.
   */
  boolean next();
}
