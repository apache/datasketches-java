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

package org.apache.datasketches.quantiles;

import java.util.Arrays;

/**
 * @author Jon Malkin
 */
final class DoublesArrayAccessor extends DoublesBufferAccessor {
  private int numItems_;
  private double[] buffer_;

  private DoublesArrayAccessor(final double[] buffer) {
    numItems_ = buffer.length;
    buffer_ = buffer;
  }

  static DoublesArrayAccessor wrap(final double[] buffer) {
    return new DoublesArrayAccessor(buffer);
  }

  static DoublesArrayAccessor initialize(final int numItems) {
    return new DoublesArrayAccessor(new double[numItems]);
  }

  @Override
  double get(final int index) {
    assert index >= 0 && index < numItems_;
    return buffer_[index];
  }

  @Override
  double set(final int index, final double value) {
    assert index >= 0 && index < numItems_;

    final double retVal = buffer_[index];
    buffer_[index] = value;
    return retVal;
  }

  @Override
  int numItems() {
    return numItems_;
  }

  @Override
  double[] getArray(final int fromIdx, final int numItems) {
    return Arrays.copyOfRange(buffer_, fromIdx, fromIdx + numItems);
  }

  @Override
  void putArray(final double[] srcArray, final int srcIndex,
                       final int dstIndex, final int numItems) {
    System.arraycopy(srcArray, srcIndex, buffer_, dstIndex, numItems);
  }

}
