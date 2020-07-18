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

package org.apache.datasketches.theta;

/**
 * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
 *
 * @author Lee Rhodes
 */
public enum UpdateReturnState {

  /**
   * The hash was accepted into the sketch and the retained count was incremented.
   */
  InsertedCountIncremented, //all UpdateSketches

  /**
   * The hash was accepted into the sketch, the retained count was incremented.
   * The current cache was out of room and resized larger based on the Resize Factor.
   */
  InsertedCountIncrementedResized, //used by HeapQuickSelectSketch

  /**
   * The hash was accepted into the sketch, the retained count was incremented.
   * The current cache was out of room and at maximum size, so the cache was rebuilt.
   */
  InsertedCountIncrementedRebuilt, //used by HeapQuickSelectSketch

  /**
   * The hash was accepted into the sketch and the retained count was not incremented.
   */
  InsertedCountNotIncremented, //used by enhancedHashInsert for Alpha

  /**
   * The hash was inserted into the local concurrent buffer,
   * but has not yet been propagated to the concurrent shared sketch.
   */
  ConcurrentBufferInserted, //used by ConcurrentHeapThetaBuffer

  /**
   * The hash has been propagated to the concurrent shared sketch.
   * This does not reflect the action taken by the shared sketch.
   */
  ConcurrentPropagated,  //used by ConcurrentHeapThetaBuffer

  /**
   * The hash was rejected as a duplicate.
   */
  RejectedDuplicate, //all UpdateSketches hashUpdate(), enhancedHashInsert

  /**
   * The hash was rejected because it was null or empty.
   */
  RejectedNullOrEmpty, //UpdateSketch.update(arr[])

  /**
   * The hash was rejected because the value was negative, zero or
   * greater than theta.
   */
  RejectedOverTheta; //all UpdateSketches.hashUpdate()

}
