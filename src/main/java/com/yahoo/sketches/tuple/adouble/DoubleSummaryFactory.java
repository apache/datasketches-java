/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.adouble;

import com.yahoo.sketches.tuple.SummaryFactory;

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

  //private static final int SERIALIZED_SIZE_BYTES = 1;
  //private static final int MODE_BYTE = 0;

  /*
   * This is deprecated and exists here just to test compatibility with previous serialization format.
   * In the current serial version of sketches factories are not serialized.
   * Creates an instance of the DoubleSummaryFactory given a serialized representation
   * @param mem Memory object with serialized DoubleSummaryFactory
   * @return DeserializedResult object, which contains a DoubleSummaryFactory object and number of
   * bytes read from the Memory
   */
  //  public static DeserializeResult<DoubleSummaryFactory> fromMemory(final Memory mem) {
  //    return new DeserializeResult<DoubleSummaryFactory>(
  //        new DoubleSummaryFactory(Mode.values()[mem.getByte(MODE_BYTE)]), SERIALIZED_SIZE_BYTES);
  //  }

}
