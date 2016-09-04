/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.tuple.DoubleSummary.Mode;

/**
 * Factory for DoubleSummary. It supports three modes of operation of DoubleSummary:
 * Sum, Min and Max.
 */
public final class DoubleSummaryFactory implements SummaryFactory<DoubleSummary> {

  private final DoubleSummary.Mode summaryMode_;

  /**
   * Creates an instance of DoubleSummaryFactory with default mode
   */
  public DoubleSummaryFactory() {
    summaryMode_ = DoubleSummary.Mode.Sum;
  }

  /**
   * Creates an instance of DoubleSummaryFactory with a given mode
   * @param summaryMode summary mode
   */
  public DoubleSummaryFactory(final DoubleSummary.Mode summaryMode) {
    summaryMode_ = summaryMode;
  }

  @Override
  public DoubleSummary newSummary() {
    return new DoubleSummary(summaryMode_);
  }

  @Override
  public DoubleSummarySetOperations getSummarySetOperations() {
    return new DoubleSummarySetOperations(summaryMode_);
  }

  private static final int SERIALIZED_SIZE_BYTES = 1;
  private static final int MODE_BYTE = 0;

  @Override
  public byte[] toByteArray() {
    final byte[] bytes = new byte[SERIALIZED_SIZE_BYTES];
    final Memory mem = new NativeMemory(bytes);
    mem.putByte(MODE_BYTE, (byte) summaryMode_.ordinal());
    return bytes;
  }

  /**
   * Creates an instance of the DoubleSummaryFactory given a serialized representation
   * @param mem Memory object with serialized DoubleSummaryFactory
   * @return DeserializedResult object, which contains a DoubleSummaryFactory object and number of 
   * bytes read from the Memory
   */
  public static DeserializeResult<DoubleSummaryFactory> fromMemory(final Memory mem) {
    return new DeserializeResult<DoubleSummaryFactory>(
        new DoubleSummaryFactory(Mode.values()[mem.getByte(MODE_BYTE)]), SERIALIZED_SIZE_BYTES);
  }

  @Override
  public DeserializeResult<DoubleSummary> summaryFromMemory(final Memory mem) {
    return DoubleSummary.fromMemory(mem);
  }

}
