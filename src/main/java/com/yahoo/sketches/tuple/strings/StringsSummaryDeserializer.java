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
public class StringsSummaryDeserializer implements SummaryDeserializer<StringsSummary> {

  @Override
  public DeserializeResult<StringsSummary> heapifySummary(final Memory mem) {
    return StringsSummary.fromMemory(mem);
  }

}
