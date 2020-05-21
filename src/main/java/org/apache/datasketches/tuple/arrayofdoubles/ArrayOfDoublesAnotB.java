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

import org.apache.datasketches.memory.WritableMemory;

/**
 * Computes a set difference of two tuple sketches of type ArrayOfDoubles
 */
public abstract class ArrayOfDoublesAnotB {

  ArrayOfDoublesAnotB() {}

  /**
   * Perform A-and-not-B set operation on the two given sketches.
   * A null sketch is interpreted as an empty sketch.
   * This is not an accumulating update. Calling update() more than once
   * without calling getResult() will discard the result of previous update()
   * 
   * @param a The incoming sketch for the first argument
   * @param b The incoming sketch for the second argument
   */  
  public abstract void update(ArrayOfDoublesSketch a, ArrayOfDoublesSketch b);

  /**
   * Gets the result of this operation in the form of a ArrayOfDoublesCompactSketch
   * @return compact sketch representing the result of the operation
   */
  public abstract ArrayOfDoublesCompactSketch getResult();

  /**
   * Gets the result of this operation in the form of a ArrayOfDoublesCompactSketch
   * @param mem memory for the result (can be null)
   * @return compact sketch representing the result of the operation (off-heap if memory is 
   * provided)
   */
  public abstract ArrayOfDoublesCompactSketch getResult(WritableMemory mem);

}
