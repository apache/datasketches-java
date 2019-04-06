/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.SummaryDeserializer;

/**
 * @author Lee Rhodes
 */
public class NodesSummaryDeserializer implements SummaryDeserializer<NodesSummary> {

  @Override
  public DeserializeResult<NodesSummary> heapifySummary(final Memory mem) {
    return NodesSummary.fromMemory(mem);
  }

}
