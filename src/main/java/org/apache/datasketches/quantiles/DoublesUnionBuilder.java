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

import org.apache.datasketches.memory.WritableMemory;

/**
 * For building a new DoublesSketch Union operation.
 *
 * @author Lee Rhodes
 */
public class DoublesUnionBuilder {
  private int bMaxK = PreambleUtil.DEFAULT_K;

  /**
   * Constructor for a new DoublesUnionBuilder. The default configuration is
   * <ul>
   * <li>k: 128. This produces a normalized rank error of about 1.7%</li>
   * <li>Memory: null</li>
   * </ul>
   */
  public DoublesUnionBuilder() {}

  /**
   * Sets the parameter <i>masK</i> that determines the maximum size of the sketch that
   * results from a union and its accuracy.
   * @param maxK determines the accuracy and size of the union and is a maximum value.
   * The effective <i>k</i> can be smaller due to unions with smaller <i>k</i> sketches.
   * It is recommended that <i>maxK</i> be a power of 2 to enable unioning of sketches with
   * different values of <i>k</i>.
   * @return this builder
   */
  public DoublesUnionBuilder setMaxK(final int maxK) {
    Util.checkK(maxK);
    bMaxK = maxK;
    return this;
  }

  /**
   * Gets the current configured value of <i>maxK</i>
   * @return the current configured value of <i>maxK</i>
   */
  public int getMaxK() {
    return bMaxK;
  }

  /**
   * Returns a new empty Union object with the current configuration of this Builder.
   * @return a Union object
   */
  public DoublesUnion build() {
    return DoublesUnionImpl.heapInstance(bMaxK);
  }

  /**
   * Returns a new empty Union object with the current configuration of this Builder
   * and the specified backing destination Memory store.
   * @param dstMem the destination memory
   * @return a Union object
   */
  public DoublesUnion build(final WritableMemory dstMem) {
    return DoublesUnionImpl.directInstance(bMaxK, dstMem);
  }

}
