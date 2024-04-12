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

import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

/**
 * This enables the special functions for performing efficient partitioning of massive data.
 */
public interface PartitioningFeature<T> {

  /**
   * This method returns an instance of
   * {@link GenericPartitionBoundaries GenericPartitionBoundaries} which provides
   * sufficient information for the user to create the given number of equally sized partitions, where "equally sized"
   * refers to an approximately equal number of items per partition.
   *
   * <p>This method is equivalent to
   * {@link #getPartitionBoundariesFromNumParts(int, QuantileSearchCriteria)
   * getPartitionBoundariesFromNumParts(numEquallySizedParts, INCLUSIVE)}.
   * </p>
   *
   * @param numEquallySizedParts an integer that specifies the number of equally sized partitions between
   * {@link GenericPartitionBoundaries#getMinItem() getMinItem()} and
   * {@link GenericPartitionBoundaries#getMaxItem() getMaxItem()}.
   * This must be a positive integer less than
   * {@link SketchPartitionLimits#getMaxPartitions() getMaxPartitions()}
   * <ul>
   * <li>A 1 will return: minItem, maxItem.</li>
   * <li>A 2 will return: minItem, median quantile, maxItem.</li>
   * <li>Etc.</li>
   * </ul>
   *
   * @return an instance of {@link GenericPartitionBoundaries GenericPartitionBoundaries}.
   * @throws SketchesArgumentException if sketch is empty.
   * @throws SketchesArgumentException if <i>numEquallySized</i> is greater than
   * {@link SketchPartitionLimits#getMaxPartitions() getMaxPartitions()}
   */
  default GenericPartitionBoundaries<T> getPartitionBoundariesFromNumParts(int numEquallySizedParts) {
    return getPartitionBoundariesFromNumParts(numEquallySizedParts, INCLUSIVE);
  }

  /**
   * This method returns an instance of
   * {@link GenericPartitionBoundaries GenericPartitionBoundaries} which provides
   * sufficient information for the user to create the given number of equally sized partitions, where "equally sized"
   * refers to an approximately equal number of items per partition.
   *
   * @param numEquallySizedParts an integer that specifies the number of equally sized partitions between
   * {@link GenericPartitionBoundaries#getMinItem() getMinItem()} and
   * {@link GenericPartitionBoundaries#getMaxItem() getMaxItem()}.
   * This must be a positive integer less than
   * {@link SketchPartitionLimits#getMaxPartitions() getMaxPartitions()}
   * <ul>
   * <li>A 1 will return: minItem, maxItem.</li>
   * <li>A 2 will return: minItem, median quantile, maxItem.</li>
   * <li>Etc.</li>
   * </ul>
   *
   * @param searchCrit
   * If INCLUSIVE, all the returned quantiles are the upper boundaries of the equally sized partitions
   * with the exception of the lowest returned quantile, which is the lowest boundary of the lowest ranked partition.
   * If EXCLUSIVE, all the returned quantiles are the lower boundaries of the equally sized partitions
   * with the exception of the highest returned quantile, which is the upper boundary of the highest ranked partition.
   *
   * @return an instance of {@link GenericPartitionBoundaries GenericPartitionBoundaries}.
   * @throws SketchesArgumentException if sketch is empty.
   * @throws SketchesArgumentException if <i>numEquallySized</i> is greater than
   * {@link SketchPartitionLimits#getMaxPartitions() getMaxPartitions()}
   */
  GenericPartitionBoundaries<T> getPartitionBoundariesFromNumParts(
      int numEquallySizedParts, QuantileSearchCriteria searchCrit);

  /**
   * This method returns an instance of
   * {@link GenericPartitionBoundaries GenericPartitionBoundaries} which provides
   * sufficient information for the user to create the given number of equally sized partitions, where "equally sized"
   * refers to an approximately equal number of items per partition.
   *
   * <p>This method is equivalent to
   * {@link #getPartitionBoundariesFromPartSize(long, QuantileSearchCriteria)
   * getPartitionBoundariesFromPartSize(nominalPartSizeItems, INCLUSIVE)}.
   * </p>
   *
   * @param nominalPartSizeItems an integer that specifies the nominal size, in items, of each target partition.
   * This must be a positive integer greater than
   * {@link SketchPartitionLimits#getMinPartitionSizeItems() getMinPartitionSizeItems()}
   *
   * @return an instance of {@link GenericPartitionBoundaries GenericPartitionBoundaries}.
   * @throws SketchesArgumentException if sketch is empty.
   * @throws SketchesArgumentException if <i>nominalPartSizeItems</i> is less than
   * {@link SketchPartitionLimits#getMinPartitionSizeItems() getMinPartitionSizeItems()}
   */
  default GenericPartitionBoundaries<T> getPartitionBoundariesFromPartSize(long nominalPartSizeItems) {
    return getPartitionBoundariesFromPartSize(nominalPartSizeItems, INCLUSIVE);
  }

  /**
   * This method returns an instance of
   * {@link GenericPartitionBoundaries GenericPartitionBoundaries} which provides
   * sufficient information for the user to create the given number of equally sized partitions, where "equally sized"
   * refers to an approximately equal number of items per partition.
   *
   * @param nominalPartSizeItems an integer that specifies the nominal size, in items, of each target partition.
   * This must be a positive integer greater than
   * {@link SketchPartitionLimits#getMinPartitionSizeItems() getMinPartitionSizeItems()}.
   *
   * @param searchCrit
   * If INCLUSIVE, all the returned quantiles are the upper boundaries of the equally sized partitions
   * with the exception of the lowest returned quantile, which is the lowest boundary of the lowest ranked partition.
   * If EXCLUSIVE, all the returned quantiles are the lower boundaries of the equally sized partitions
   * with the exception of the highest returned quantile, which is the upper boundary of the highest ranked partition.
   *
   * @return an instance of {@link GenericPartitionBoundaries GenericPartitionBoundaries}.
   * @throws SketchesArgumentException if sketch is empty.
   * @throws SketchesArgumentException if <i>nominalPartSizeItems</i> is less than
   * {@link SketchPartitionLimits#getMinPartitionSizeItems() getMinPartitionSizeItems()}
   */
  GenericPartitionBoundaries<T> getPartitionBoundariesFromPartSize(
      long nominalPartSizeItems, QuantileSearchCriteria searchCrit);

}
