/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

/**
 * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
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
   * Used by Direct Update Sketches.
   * This indicates that the Sketch has run out of capacity in Direct Memory and must
   * be moved and reallocated larger memory before continuing. This should only occur in 
   * Direct Memory cases where the allocated memory has intentionally been undersized.
   */
  RejectedFull, //used by DQSS.hashUpdate(), DirectUnion.hashUpdate() (internal only)
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