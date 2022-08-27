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

package org.apache.datasketches;

public interface QuantilesAPI {

  /**
   * Gets the user configured parameter k, which controls the accuracy of the sketch
   * and its memory space usage.
   * @return the user configured parameter k, which controls the accuracy of the sketch
   * and its memory space usage.
   */
  int getK();

  /**
   * Gets the length of the input stream.
   * @return the length of the input stream.
   */
  long getN();

  /**
   * Gets the number of values (or items) retained by the sketch.
   * @return the number of values (or items) retained by the sketch
   */
  int getNumRetained();

  /**
   * Returns true if this sketch's data structure is backed by Memory or WritableMemory.
   * @return true if this sketch's data structure is backed by Memory or WritableMemory.
   */
  boolean hasMemory();

  /**
   * Returns true if this sketch's data structure is off-heap (a.k.a., Direct or Native memory).
   * @return true if this sketch's data structure is off-heap (a.k.a., Direct or Native memory).
   */
  boolean isDirect();

  /**
   * Returns true if this sketch is empty.
   * @return true if this sketch is empty.
   */
  boolean isEmpty();

  /**
   * Returns true if this sketch is in estimation mode.
   * @return true if this sketch is in estimation mode.
   */
  boolean isEstimationMode();

  /**
   * Returns true if this sketch is read only.
   * @return true if this sketch is read only.
   */
  boolean isReadOnly();

  /**
   * Resets this sketch to the empty state.
   * If the sketch is <i>read only</i> this does nothing.
   */
  void reset();

  /**
   * Returns a summary of the key parameters of the sketch.
   * @return a summary of the key parameters of the sketch.
   */
  @Override
  String toString();

}

