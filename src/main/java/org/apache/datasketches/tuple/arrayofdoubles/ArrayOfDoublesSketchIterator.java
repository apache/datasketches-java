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

package org.apache.datasketches.tuple.arrayofdoubles;

/**
 * Interface for iterating over tuple sketches of type ArrayOfDoubles
 */
public interface ArrayOfDoublesSketchIterator {
  /**
   * Advancing the iterator and checking existence of the next entry
   * is combined here for efficiency. This results in an undefined
   * state of the iterator before the first call of this method.
   * @return true if the next element exists
   */
  public boolean next();

  /**
   * Gets a key from the current entry in the sketch, which is a hash
   * of the original key passed to update(). The original keys are not
   * retained. Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return hash key from the current entry
   */
  public long getKey();

  /**
   * Gets an array of values from the current entry in the sketch.
   * Don't call this before calling next() for the first time
   * or after getting false from next().
   * @return array of double values for the current entry (may or may not be a copy)
   */
  public double[] getValues();
}
