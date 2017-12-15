/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.sketches.tuple.DoubleSummary.Mode;

/**
 * Methods for producing unions and intersections of two objects of type DoubleSummary.
 */
public final class DoubleSummarySetOperations implements SummarySetOperations<DoubleSummary> {

  private final Mode summaryMode_;

  /**
   * Creates an instance with default mode.
   */
  public DoubleSummarySetOperations() {
    summaryMode_ = DoubleSummary.Mode.Sum;
  }

  /**
   * Creates an instance given a DoubleSummary update mode.
   * @param summaryMode DoubleSummary update mode.
   */
  public DoubleSummarySetOperations(final Mode summaryMode) {
    summaryMode_ = summaryMode;
  }

  @Override
  public DoubleSummary union(final DoubleSummary a, final DoubleSummary b) {
    final DoubleSummary result = new DoubleSummary(summaryMode_);
    if (a != null) { result.update(a.getValue()); }
    if (b != null) { result.update(b.getValue()); }
    return result;
  }

  @Override
  public DoubleSummary intersection(final DoubleSummary a, final DoubleSummary b) {
    return union(a, b);
  }
}
