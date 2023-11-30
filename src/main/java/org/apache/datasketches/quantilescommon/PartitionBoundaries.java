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

/**
 * This defines a set of results computed from the getParitionBoundaries() function and
 * encapsulates the basic methods needed to construct actual partitions based on generic items.
 */
public interface PartitionBoundaries {

  /**
   * Gets the length of the input stream offered to the underlying sketch.
   * @return the length of the input stream offered to the underlying sketch.
   */
  long getN();

  /**
   * Gets an ordered array of natural ranks of the associated array of partition boundaries utilizing
   * a specified search criterion. Natural ranks are integral values on the interval [1, N]
   * @return an array of natural ranks.
   */
  long[] getNaturalRanks();

  /**
   * Gets an ordered array of normalized ranks of the associated array of partition boundaries utilizing
   * a specified search criterion. Normalized ranks are double values on the interval [0.0, 1.0].
   * @return an array of normalized ranks.
   */
  double[] getNormalizedRanks();

  /**
   * Gets the number of items to be included for each partition as an array.
   * The count at index 0 is 0.  The number of items included in the first partition, defined by the boundaries at
   * index 0 and index 1, is at index 1 in this array, etc.
   * @return the number of items to be included for each partition as an array.
   */
  long[] getNumDeltaItems();

  /**
   * Gets the number of partitions
   * @return the number of partitions
   */
  int getNumPartitions();

  /**
   * Gets the search criteria specified for the source sketch
   * @return The search criteria specified for the source sketch
   */
  QuantileSearchCriteria getSearchCriteria();
}
