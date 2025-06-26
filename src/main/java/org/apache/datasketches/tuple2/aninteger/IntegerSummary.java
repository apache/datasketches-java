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

package org.apache.datasketches.tuple2.aninteger;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.tuple2.DeserializeResult;
import org.apache.datasketches.tuple2.UpdatableSummary;

/**
 * Summary for generic tuple sketches of type Integer.
 * This summary keeps an Integer value. On update a predefined operation is performed depending on
 * the mode.
 * Supported modes: Sum, Min, Max, AlwaysOne, Increment. The default mode is Sum.
 */
public class IntegerSummary implements UpdatableSummary<Integer> {
  private int value_;
  private final Mode mode_;

  /**
   * The aggregation modes for this Summary
   */
  public enum Mode {

    /**
     * The aggregation mode is the summation function.
     *
     * <p>New retained value = previous retained value + incoming value</p>
     */
    Sum,

    /**
     * The aggregation mode is the minimum function.
     *
     * <p>New retained value = min(previous retained value, incoming value)</p>
     */
    Min,

    /**
     * The aggregation mode is the maximum function.
     *
     * <p>New retained value = max(previous retained value, incoming value)</p>
     */
    Max,

    /**
     * The aggregation mode is always one.
     *
     * <p>New retained value = 1</p>
     */
    AlwaysOne
  }

  /**
   * Creates an instance of IntegerSummary with a given starting value and mode.
   * @param value starting value
   * @param mode update mode
   */
  private IntegerSummary(final int value, final Mode mode) {
    value_ = value;
    mode_ = mode;
  }

  /**
   * Creates an instance of IntegerSummary with a given mode.
   * @param mode update mode. This should not be called by a user.
   */
  public IntegerSummary(final Mode mode) {
    mode_ = mode;
    switch (mode) {
      case Sum:
        value_ = 0;
        break;
      case Min:
        value_ = Integer.MAX_VALUE;
        break;
      case Max:
        value_ = Integer.MIN_VALUE;
        break;
      case AlwaysOne:
        value_ = 1;
        break;
    }
  }

  @Override
  public IntegerSummary update(final Integer value) {
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
      value_ = 1;
      break;
    }
    return this;
  }

  @Override
  public IntegerSummary copy() {
    return new IntegerSummary(value_, mode_);
  }

  /**
   * Returns the current value of the IntegerSummary
   * @return current value of the IntegerSummary
   */
  public int getValue() {
    return value_;
  }

  private static final int SERIALIZED_SIZE_BYTES = 5;
  private static final int VALUE_INDEX = 0;
  private static final int MODE_BYTE_INDEX = 4;

  @Override
  public byte[] toByteArray() {
    final byte[] bytes = new byte[SERIALIZED_SIZE_BYTES];
    ByteArrayUtil.putIntLE(bytes, VALUE_INDEX, value_);
    bytes[MODE_BYTE_INDEX] = (byte) mode_.ordinal();
    return bytes;
  }

  /**
   * Creates an instance of the IntegerSummary given a serialized representation
   * @param seg MemorySegment object with serialized IntegerSummary
   * @return DeserializedResult object, which contains a IntegerSummary object and number of bytes
   * read from the MemorySegment
   */
  public static DeserializeResult<IntegerSummary> fromMemorySegment(final MemorySegment seg) {
    return new DeserializeResult<>(new IntegerSummary(seg.get(JAVA_INT_UNALIGNED, VALUE_INDEX),
        Mode.values()[seg.get(JAVA_BYTE, MODE_BYTE_INDEX)]), SERIALIZED_SIZE_BYTES);
  }

}
