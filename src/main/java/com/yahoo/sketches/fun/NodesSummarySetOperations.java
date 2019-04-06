/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import com.yahoo.sketches.tuple.SummarySetOperations;

/**
 * @author Lee Rhodes
 */
public class NodesSummarySetOperations implements SummarySetOperations<NodesSummary> {

  @Override
  public NodesSummary union(final NodesSummary a, final NodesSummary b) {
    return a.copy();
  }

  @Override
  public NodesSummary intersection(final NodesSummary a, final NodesSummary b) {
    return a.copy();
  }

}
