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

package org.apache.datasketches.quantilescommon;

import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * This defines the methods required to compute the partition limits.
 */
public interface SketchPartitionLimits {

  /**
   * Gets the maximum number of partitions this sketch will support based on the configured size <i>K</i>
   * and the number of retained values of this sketch.
   * @return the maximum number of partitions this sketch will support.
   */
  default int getMaxPartitions() {
    return getNumRetained() / 2;
  }

  /**
   * Gets the minimum partition size in items this sketch will support based on the configured size <i>K</i> of this
   * sketch and the number of retained values of this sketch.
   * @return the minimum partition size in items this sketch will support.
   */
  default long getMinPartitionSizeItems() {
    final long totalN = getN();
    if (totalN <= 0) { throw new SketchesArgumentException(EMPTY_MSG); }
    return totalN / getMaxPartitions();
  }

  /**
   * Gets the length of the input stream offered to the sketch..
   * @return the length of the input stream offered to the sketch.
   */
  long getN();

  /**
   * Gets the number of quantiles retained by the sketch.
   * @return the number of quantiles retained by the sketch
   */
  int getNumRetained();

}
