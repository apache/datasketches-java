/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import com.yahoo.sketches.tuple.SummarySetOperations;

/**
 * @author Lee Rhodes
 */
public class StringsSummarySetOperations implements SummarySetOperations<StringsSummary> {

  @Override
  public StringsSummary union(final StringsSummary a, final StringsSummary b) {
    return a.copy();
  }

  @Override
  public StringsSummary intersection(final StringsSummary a, final StringsSummary b) {
    return a.copy();
  }

}
