package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;

public class DoubleSummaryDeserializer implements SummaryDeserializer<DoubleSummary> {

  @Override
  public DeserializeResult<DoubleSummary> heapifySummary(Memory mem) {
    return DoubleSummary.fromMemory(mem);
  }

}
