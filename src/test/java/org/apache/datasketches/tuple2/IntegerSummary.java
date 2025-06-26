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

package org.apache.datasketches.tuple2;

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ByteArrayUtil;

/**
 * Summary for generic tuple sketches of type Integer.
 * This summary keeps an Integer value.
 */
public class IntegerSummary implements UpdatableSummary<Integer> {
  private int value_;

  /**
   * Creates an instance of IntegerSummary with a given starting value.
   * @param value starting value
   */
  public IntegerSummary(final int value) {
    value_ = value;
  }

  @Override
  public IntegerSummary update(final Integer value) {
    value_ += value;
    return this;
  }

  @Override
  public IntegerSummary copy() {
    return new IntegerSummary(value_);
  }

  /**
   * @return current value of the IntegerSummary
   */
  public int getValue() {
    return value_;
  }

  private static final int SERIALIZED_SIZE_BYTES = 4;
  private static final int VALUE_INDEX = 0;

  @Override
  public byte[] toByteArray() {
    final byte[] bytes = new byte[SERIALIZED_SIZE_BYTES];
    ByteArrayUtil.putIntLE(bytes, VALUE_INDEX, value_);
    return bytes;
  }

  /**
   * Creates an instance of the IntegerSummary given a serialized representation
   * @param seg MemorySegment object with serialized IntegerSummary
   * @return DeserializedResult object, which contains a IntegerSummary object and number of bytes
   * read from the MemorySegment
   */
  public static DeserializeResult<IntegerSummary> fromMemorySegment(final MemorySegment seg) {
    return new DeserializeResult<>(new IntegerSummary(seg.get(JAVA_INT_UNALIGNED, VALUE_INDEX)), SERIALIZED_SIZE_BYTES);
  }

}
