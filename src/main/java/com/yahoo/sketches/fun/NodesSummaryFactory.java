/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import com.yahoo.sketches.tuple.SummaryFactory;

/**
 * @author Lee Rhodes
 */
public class NodesSummaryFactory implements SummaryFactory<NodesSummary> {

  @Override
  public NodesSummary newSummary() {
    return new NodesSummary();
  }

}
