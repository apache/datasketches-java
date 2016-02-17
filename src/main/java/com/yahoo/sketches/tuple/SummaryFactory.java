/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import com.yahoo.sketches.memory.Memory;

/**
 * Interface for user-defined SummaryFactory
 * @param <S> type of Summary
 */
public interface SummaryFactory<S extends Summary> {

  /**
   * @return new instance of Summary
   */
  public S newSummary();

  /**
   * This is to obtain methods of producing unions and intersections of two Summary objects
   * @return SummarySetOperations
   */
  public SummarySetOperations<S> getSummarySetOperations();

  /**
   * This is to create an instance of a Summary given a serialized representation
   * @param mem Memory object with serialized representation of a Summary
   * @return DeserializedResult object, which contains a Summary object and number of bytes read from the Memory
   */
  public DeserializeResult<S> summaryFromMemory(Memory mem);

  /**
   * This is to serialize an instance to a byte array.
   * For deserialization there must be a static method
   * DeserializeResult<T> fromMemory(Memory mem)
   * @return serialized representation of the SummaryFactory
   */
  public byte[] toByteArray();
}
