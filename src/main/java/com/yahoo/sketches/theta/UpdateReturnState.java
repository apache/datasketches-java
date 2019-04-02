/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

/**
 * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
 *
 * @author Lee Rhodes
 */
public enum UpdateReturnState {

  /**
   * Indicates that the value was accepted into the sketch and the retained count was incremented.
   */
  InsertedCountIncremented, //all UpdateSketches

  /**
   * Indicates that the value was accepted into the sketch and the retained count was not incremented.
   */
  InsertedCountNotIncremented, //used by enhancedHashInsert for Alpha

  /**
   * Indicates that the value was inserted into the local concurrent buffer,
   * but has not yet been propagated to the concurrent shared sketch.
   */
  ConcurrentBufferInserted, //used by ConcurrentHeapThetaBuffer

  /**
   * Indicates that the value has been propagated to the concurrent shared sketch.
   * This does not reflect the action taken by the shared sketch.
   */
  ConcurrentPropagated,  //used by ConcurrentHeapThetaBuffer

  /**
   * Indicates that the value was rejected as a duplicate.
   */
  RejectedDuplicate, //all UpdateSketches hashUpdate(), enhancedHashInsert

  /**
   * Indicates that the value was rejected because it was null or empty.
   */
  RejectedNullOrEmpty, //UpdateSketch.update(arr[])

  /**
   * Indicates that the value was rejected because the hash value was negative, zero or
   * greater than theta.
   */
  RejectedOverTheta; //all UpdateSketches.hashUpdate()

}
