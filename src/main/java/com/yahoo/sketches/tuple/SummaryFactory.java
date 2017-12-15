/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * Interface for user-defined SummaryFactory
 * @param <S> type of Summary
 */
public interface SummaryFactory<S extends Summary> {

  /**
   * @return new instance of Summary
   */
  public S newSummary();

}
