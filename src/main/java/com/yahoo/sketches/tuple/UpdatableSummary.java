/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Interface for updating user-defined Summary
 * @param <U> type of update value
 */
public interface UpdatableSummary<U> extends Summary {

  /**
   * This is to provide a method of updating summaries
   * @param value update value
   */
  public void update(U value);

}
