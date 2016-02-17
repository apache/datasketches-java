/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * This summary keeps a double value. On update a predefined operation is performed depending on the mode.
 * Three modes are supported: Sum, Min and Max. The default mode is Sum.
 */
public class DoubleSummary implements UpdatableSummary<Double> {

  public static enum Mode { Sum, Min, Max }

  private double value_;
  private Mode mode_;

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
  public DoubleSummary(Mode mode) {
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
    }
  }

  /**
   * Creates an instance of DoubleSummary with a given starting value and mode
   * @param value starting value
   * @param mode update mode
   */
  public DoubleSummary(double value, Mode mode) {
    value_ = value;
    mode_ = mode;
  }

  @Override
  public void update(Double value) {
    switch(mode_) {
    case Sum:
      value_ += value.doubleValue();
      break;
    case Min:
      if (value < value_) value_ = value;
      break;
    case Max:
      if (value > value_) value_ = value;
      break;
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
    byte[] bytes = new byte[SERIALIZED_SIZE_BYTES];
    Memory mem = new NativeMemory(bytes);
    mem.putDouble(VALUE_DOUBLE, value_);
    mem.putByte(MODE_BYTE, (byte) mode_.ordinal());
    return bytes;
  }

  /**
   * Creates an instance of the DoubleSummary given a serialized representation
   * @param mem Memory object with serialized DoubleSummary
   * @return DeserializedResult object, which contains a DoubleSummary object and number of bytes read from the Memory
   */
  public static DeserializeResult<DoubleSummary> fromMemory(Memory mem) {
    return new DeserializeResult<DoubleSummary>(new DoubleSummary(mem.getDouble(VALUE_DOUBLE), Mode.values()[mem.getByte(MODE_BYTE)]), SERIALIZED_SIZE_BYTES);
  }

}
