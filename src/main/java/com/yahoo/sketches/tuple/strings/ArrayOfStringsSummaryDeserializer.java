/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.SummaryDeserializer;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummaryDeserializer implements SummaryDeserializer<ArrayOfStringsSummary> {

  @Override
  public DeserializeResult<ArrayOfStringsSummary> heapifySummary(final Memory mem) {
    return ArrayOfStringsSummary.fromMemory(mem);
  }

}
