/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple.adouble;

import org.apache.datasketches.ByteArrayUtil;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.DeserializeResult;
import org.apache.datasketches.tuple.UpdatableSummary;

/**
 * Summary for generic tuple sketches of type Double.
 * This summary keeps a double value. On update a predefined operation is performed depending on
 * the mode.
 * Supported modes: Sum, Min, Max, AlwaysOne, Increment. The default mode is Sum.
 */
public final class DoubleSummary implements UpdatableSummary<Double> {
  private double value_;
  private final Mode mode_;

  /**
   * The aggregation modes for this Summary
   */
  public enum Mode {

    /**
     * The aggregation mode is the summation function.
     * <p>New retained value = previous retained value + incoming value</p>
     */
    Sum,

    /**
     * The aggregation mode is the minimum function.
     * <p>New retained value = min(previous retained value, incoming value)</p>
     */
    Min,

    /**
     * The aggregation mode is the maximum function.
     * <p>New retained value = max(previous retained value, incoming value)</p>
     */
    Max,

    /**
     * The aggregation mode is always one.
     * <p>New retained value = 1.0</p>
     */
    AlwaysOne
  }

  /**
   * Creates an instance of DoubleSummary with a given starting value and mode
   * @param value starting value
   * @param mode update mode
   */
  private DoubleSummary(final double value, final Mode mode) {
    value_ = value;
    mode_ = mode;
  }

  /**
   * Creates an instance of DoubleSummary with a given mode.
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
      case AlwaysOne:
        value_ = 1.0;
        break;
    }
  }

  @Override
  public DoubleSummary update(final Double value) {
    switch (mode_) {
    case Sum:
      value_ += value;
      break;
    case Min:
      if (value < value_) { value_ = value; }
      break;
    case Max:
      if (value > value_) { value_ = value; }
      break;
    case AlwaysOne:
      value_ = 1.0;
      break;
    }
    return this;
  }

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
  private static final int VALUE_INDEX = 0;
  private static final int MODE_BYTE_INDEX = 8;

  @Override
  public byte[] toByteArray() {
    final byte[] bytes = new byte[SERIALIZED_SIZE_BYTES];
    ByteArrayUtil.putDoubleLE(bytes, VALUE_INDEX, value_);
    bytes[MODE_BYTE_INDEX] = (byte) mode_.ordinal();
    return bytes;
  }

  /**
   * Creates an instance of the DoubleSummary given a serialized representation
   * @param mem Memory object with serialized DoubleSummary
   * @return DeserializedResult object, which contains a DoubleSummary object and number of bytes
   * read from the Memory
   */
  public static DeserializeResult<DoubleSummary> fromMemory(final Memory mem) {
    return new DeserializeResult<>(new DoubleSummary(mem.getDouble(VALUE_INDEX),
        Mode.values()[mem.getByte(MODE_BYTE_INDEX)]), SERIALIZED_SIZE_BYTES);
  }

}
