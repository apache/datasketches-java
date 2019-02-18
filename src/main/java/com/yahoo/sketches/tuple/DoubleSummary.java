/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ByteArrayUtil;

/**
 * Summary for generic tuple sketches of type Double.
 * This summary keeps a double value. On update a predefined operation is performed depending on
 * the mode.
 * Three modes are supported: Sum, Min and Max. The default mode is Sum.
 */
public final class DoubleSummary implements UpdatableSummary<Double> {

  /**
   * The aggregation modes for this Summary
   */
  public static enum Mode {
    /**
     * The aggregation mode is the summation function
     */
    Sum,
    /**
     * The aggregation mode is the minimum function
     */
    Min,
    /**
     * The aggregation mode is the maximum function
     */
    Max
  }

  private double value_;
  private final Mode mode_;

  /**
   * Creates an instance of DoubleSummary with zero starting value and default mode (Sum)
   */
  public DoubleSummary() {
    this(0, Mode.Sum);
  }

  /**
   * Creates an instance of DoubleSummary with zero starting value and a given mode (Sum)
   * @param mode update mode
   */
  public DoubleSummary(final Mode mode) {
    mode_ = mode;
    switch (mode) {
      case Sum:
        value_ = 0;
        break;
      case Min:
        value_ = Double.POSITIVE_INFINITY;
        break;
      case Max:
        value_ = Double.NEGATIVE_INFINITY;
        break;
        //default: //This cannot happen and cannot be tested
    }
  }

  /**
   * Creates an instance of DoubleSummary with a given starting value and mode
   * @param value starting value
   * @param mode update mode
   */
  public DoubleSummary(final double value, final Mode mode) {
    value_ = value;
    mode_ = mode;
  }

  @Override
  public void update(final Double value) {
    switch (mode_) {
    case Sum:
      value_ += value.doubleValue();
      break;
    case Min:
      if (value < value_) { value_ = value; }
      break;
    case Max:
      if (value > value_) { value_ = value; }
      break;
      //default: //This cannot happen and cannot be tested
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public DoubleSummary copy() {
    return new DoubleSummary(value_, mode_);
  }

  /**
   * @return current value of the DoubleSummary
   */
  public double getValue() {
    return value_;
  }

  private static final int SERIALIZED_SIZE_BYTES = 9;
  private static final int VALUE_DOUBLE = 0;
  private static final int MODE_BYTE = 8;

  @Override
  public byte[] toByteArray() {
    final byte[] bytes = new byte[SERIALIZED_SIZE_BYTES];
    ByteArrayUtil.putDoubleLE(bytes, VALUE_DOUBLE, value_);
    bytes[MODE_BYTE] = (byte) mode_.ordinal();
    return bytes;
  }

  /**
   * Creates an instance of the DoubleSummary given a serialized representation
   * @param mem Memory object with serialized DoubleSummary
   * @return DeserializedResult object, which contains a DoubleSummary object and number of bytes
   * read from the Memory
   */
  public static DeserializeResult<DoubleSummary> fromMemory(final Memory mem) {
    return new DeserializeResult<DoubleSummary>(new DoubleSummary(mem.getDouble(VALUE_DOUBLE),
        Mode.values()[mem.getByte(MODE_BYTE)]), SERIALIZED_SIZE_BYTES);
  }

}
