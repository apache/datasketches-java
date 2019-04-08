package com.yahoo.sketches.tuple.adouble;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.SummaryDeserializer;

public class DoubleSummaryDeserializer implements SummaryDeserializer<DoubleSummary> {

  @Override
  public DeserializeResult<DoubleSummary> heapifySummary(final Memory mem) {
    return DoubleSummary.fromMemory(mem);
  }

}
