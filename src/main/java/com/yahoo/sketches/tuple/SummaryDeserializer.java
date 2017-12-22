package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;

/**
 * Interface for deserializing user-defined Summary
 * @param <S> type of Summary
 */
public interface SummaryDeserializer<S extends Summary> {

  /**
   * This is to create an instance of a Summary given a serialized representation.
   * The user may assume that the start of the given Memory is the correct place to start
   * deserializing. However, the user must be able to determine the number of bytes required to
   * deserialize the summary as the capacity of the given Memory may
   * include multiple such summaries and may be much larger than required for a single summary.
   * @param mem Memory object with serialized representation of a Summary
   * @return DeserializedResult object, which contains a Summary object and number of bytes read 
   * from the Memory
   */
  public DeserializeResult<S> heapifySummary(Memory mem);

}
