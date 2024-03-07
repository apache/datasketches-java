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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import org.apache.datasketches.common.SketchesStateException;

/**
 * Implements PartitionBoundaries
 */
final public class GenericPartitionBoundaries<T> implements PartitionBoundaries {
  private long totalN; //totalN of source sketch
  private T[] boundaries;       //quantiles at the boundaries
  private long[] natRanks;      //natural ranks at the boundaries
  private double[] normRanks;   //normalized ranks at the boundaries
  private T maxItem;            //of the source sketch
  private T minItem;            //of the source sketch
  private QuantileSearchCriteria searchCrit; //of the source sketch query to getPartitionBoundaries.
  //computed
  private long[] numDeltaItems; //num of items in each part
  private int numPartitions;    //num of partitions

  public GenericPartitionBoundaries(
      final long totalN,
      final T[] boundaries,
      final long[] natRanks,
      final double[] normRanks,
      final T maxItem,
      final T minItem,
      final QuantileSearchCriteria searchCrit) {
    this.totalN = totalN;
    this.boundaries = boundaries; //SpotBugs EI_EXPOSE_REP2 copying from sketch class to this "friend" class.
    this.natRanks = natRanks;     // "
    this.normRanks = normRanks;   // "
    this.maxItem = maxItem;
    this.minItem = minItem;
    this.searchCrit = searchCrit;
    //check and compute
    final int len = boundaries.length;
    if (len < 2) { throw new SketchesStateException("Source sketch is empty"); }
    numDeltaItems = new long[len];
    numDeltaItems[0] = 0; // index 0 is always 0
    for (int i = 1; i < len; i++) {
      final int addOne = ( (i == 1 && (this.searchCrit == INCLUSIVE))
       || ((i == (len - 1)) && this.searchCrit == EXCLUSIVE) ) ? 1 : 0;
      numDeltaItems[i] = natRanks[i] - natRanks[i - 1] + addOne;
    }
    this.numPartitions = len - 1;
  }

  @Override
  public long getN() { return totalN; }

  /**
   * Gets an ordered array of boundaries that sequentially define the upper and lower boundaries of partitions.
   * These partitions are to be constructed by an external process. Each boundary is essentially a reference and
   * should uniquely identify an item or a set of identical items from the original stream of data fed to the
   * originating sketch.
   *
   * <p>Assume boundaries array has size N + 1. Let the indicies be sequentially numbered from 0 to N.
   * The number of partitions is always one less than the size of the boundaries array.
   * Let the the partitions be sequentially numbered from 1 to N.
   *
   * <p>If these results were computed using QuantileSearchCriteria.INCLUSIVE then these sequential boundaries
   * are to be interpreted as follows:
   * <ul>
   * <li>Partition 1: include all items &ge; index 0 and &le; index 1.</li>
   * <li>Partition 2: include all items &gt; index 1 and &le; index 2.</li>
   * <li>Partition N: include all items &gt; index N-1 and &le; index N.</li>
   * </ul>
   *
   * <p>If these results were computed using QuantileSearchCriteria.EXCLUSIVE then these sequential boundaries
   * are to be interpreted as follows:
   * <ul>
   * <li>Partition 1: include all items &ge; index 0 and &lt; index 1.</li>
   * <li>Partition 2: include all items &ge; index 1 and &lt; index 2.</li>
   * <li>Partition N: include all items &ge; index N-1 and &le; index N.</li>
   * </ul>
   *
   * @return an array of boundaries that sequentially define the upper and lower boundaries of partitions.
   */
  public T[] getBoundaries() { return boundaries.clone(); }

  @Override
  public long[] getNaturalRanks() { return natRanks.clone(); }

  @Override
  public double[] getNormalizedRanks() { return normRanks.clone(); }

  @Override
  public long[] getNumDeltaItems() { return numDeltaItems.clone(); }

  @Override
  public int getNumPartitions() { return numPartitions; }

  /**
   * Returns the maximum item of the stream. This may be distinct from the largest item retained by the
   * sketch algorithm.
   *
   * @return the maximum item of the stream
   * @throws IllegalArgumentException if sketch is empty.
   */
  public T getMaxItem() { return maxItem; }

  /**
   * Returns the minimum item of the stream. This may be distinct from the smallest item retained by the
   * sketch algorithm.
   *
   * @return the minimum item of the stream
   * @throws IllegalArgumentException if sketch is empty.
   */
  public T getMinItem() { return minItem; }

  @Override
  public QuantileSearchCriteria getSearchCriteria() { return searchCrit; }

}
