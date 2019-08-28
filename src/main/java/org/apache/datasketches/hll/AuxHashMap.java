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

package org.apache.datasketches.hll;

import org.apache.datasketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
interface AuxHashMap {

  AuxHashMap copy();

  int getAuxCount();

  int[] getAuxIntArr();

  int getCompactSizeBytes();

  PairIterator getIterator();

  int getLgAuxArrInts();

  int getUpdatableSizeBytes();

  boolean isMemory();

  boolean isOffHeap();

  /**
   * Adds the slotNo and value to the aux array.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo.
   * @throws SketchesStateException if this slotNo already exists in the aux array.
   */
  void mustAdd(int slotNo, int value);

  /**
   * Returns value given slotNo. If this fails an exception is thrown.
   * @param slotNo the index from the HLL array
   * @return value the HLL value at the slotNo
   * @throws SketchesStateException if valid slotNo and value is not found.
   */
  int mustFindValueFor(int slotNo);

  /**
   * Replaces the entry at slotNo with the given value.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo
   * @throws SketchesStateException if a valid slotNo, value is not found.
   */
  void mustReplace(int slotNo, int value);

}
