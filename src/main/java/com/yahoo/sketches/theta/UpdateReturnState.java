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
   * Indicates a normal sketch update response and the action that was taken by the sketch.
   */
  InsertedCountIncremented, //all UpdateSketches

  /**
   * Indicates a normal sketch update response and the action that was taken by the sketch.
   */
  InsertedCountNotIncremented, //used by enhancedHashInsert for Alpha

  /**
   * Indicates a normal sketch update response and the action that was taken by the sketch.
   */
  RejectedDuplicate, //all UpdateSketches hashUpdate(), enhancedHashInsert

  /**
   * Indicates a normal sketch update response and the action that was taken by the sketch.
   */
  RejectedNullOrEmpty, //UpdateSketch.update(arr[])

  /**
   * Indicates a normal sketch update response and the action that was taken by the sketch.
   */
  RejectedOverTheta; //all UpdateSketches.hashUpdate()
}
