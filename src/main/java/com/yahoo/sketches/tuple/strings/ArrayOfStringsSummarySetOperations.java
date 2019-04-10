/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import com.yahoo.sketches.tuple.SummarySetOperations;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummarySetOperations implements SummarySetOperations<ArrayOfStringsSummary> {

  @Override
  public ArrayOfStringsSummary union(final ArrayOfStringsSummary a, final ArrayOfStringsSummary b) {
    return a.copy();
  }

  @Override
  public ArrayOfStringsSummary intersection(final ArrayOfStringsSummary a, final ArrayOfStringsSummary b) {
    return a.copy();
  }

}
