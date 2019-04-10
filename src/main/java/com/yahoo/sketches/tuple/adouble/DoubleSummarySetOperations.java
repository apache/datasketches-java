/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.adouble;

import com.yahoo.sketches.tuple.SummarySetOperations;
import com.yahoo.sketches.tuple.adouble.DoubleSummary.Mode;

/**
 * Methods for defining how unions and intersections of two objects of type DoubleSummary
 * are performed. These methods are not called directly by a user.
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
    result.update(a.getValue());
    result.update(b.getValue());
    return result;
  }

  @Override
  public DoubleSummary intersection(final DoubleSummary a, final DoubleSummary b) {
    return union(a, b);
  }
}
