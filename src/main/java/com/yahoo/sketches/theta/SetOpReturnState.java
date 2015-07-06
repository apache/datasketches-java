/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

public enum SetOpReturnState {
  
  /**
   * Indicates a successful SetOperation response
   */
  Success,
  
  /**
   * This indicates that the SetOperation has run out of capacity in Direct Memory and must
   * be moved and reallocated larger memory before continuing. This should only occur in 
   * Direct Memory cases where the allocated memory has intentionally been undersized.
   */
  SetOpRejectedFull;
}