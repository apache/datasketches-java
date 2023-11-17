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

package org.apache.datasketches.partitions;

import static java.lang.Math.ceil;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesAPI.EMPTY_MSG;

import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.GenericPartitionBoundaries;
import org.apache.datasketches.quantilescommon.PartitioningFeature;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesGenericAPI;
import org.apache.datasketches.quantilescommon.Stack;

/**
 * A partitioning process that can partition very large data sets into thousands
 * of partitions of approximately the same size.
 * @param <T> the data type
 * @param <S> the quantiles sketch that implements both QuantilesGenericAPI and PartitioningFeature.
 */
//@SuppressWarnings("unused")
public class Partitioner<T, S extends QuantilesGenericAPI<T> & PartitioningFeature<T>> {
  private static final QuantileSearchCriteria defaultCriteria = INCLUSIVE;
  private final long tgtPartitionSize;
  private final int maxPartsPerSk;
  private final SketchFillRequest<T, S> fillReq;
  private final QuantileSearchCriteria criteria;
  private final Stack<StackElement<T>> stack = new Stack<>();

  //computed once at the beginning
  private int numLevels;
  private int partitionsPerSk;
  //output
  private final List<PartitionBoundsRow<T>> finalPartitionList = new ArrayList<>();

  /**
   * This constructor assumes a QuantileSearchCriteria of INCLUSIVE.
   * @param tgtPartitionSize the target size of the resulting partitions in number of items.
   * @param maxPartsPerPass The maximum number of partitions to request from the sketch. The smaller this number is
   * the smaller the variance will be of the resulting partitions, but this will increase the number of passes of the
   * source data set.
   * @param fillReq The is an implementation of the SketchFillRequest call-back supplied by the user and implements
   * the SketchFillRequest interface.
   */
  public Partitioner(
      final long tgtPartitionSize,
      final int maxPartsPerPass,
      final SketchFillRequest<T,S> fillReq) {
    this(tgtPartitionSize, maxPartsPerPass, fillReq, defaultCriteria);
  }

  /**
   * This constructor includes the QuantileSearchCriteria criteria as a parameter.
   * @param tgtPartitionSize the target size of the resulting partitions in number of items.
   * @param maxPartsPerSk The maximum number of partitions to request from the sketch. The smaller this number is
   * the smaller the variance will be of the resulting partitions, but this will increase the number of passes of the
   * source data set.
   * @param fillReq The is an implementation of the SketchFillRequest call-back supplied by the user.
   * @param criteria This is the desired QuantileSearchCriteria to be used.
   */
  public Partitioner(
      final long tgtPartitionSize,
      final int maxPartsPerSk,
      final SketchFillRequest<T,S> fillReq,
      final QuantileSearchCriteria criteria) {
    this.tgtPartitionSize = tgtPartitionSize;
    this.maxPartsPerSk = maxPartsPerSk;
    this.fillReq = fillReq;
    this.criteria = criteria;
  }

  /**
   * This initiates the partitioning process
   * @param sk A sketch of the entire data set.
   * @return the final partitioning list
   */
  public List<PartitionBoundsRow<T>> partition(final S sk) {
    if (sk.isEmpty()) { throw new SketchesArgumentException(EMPTY_MSG); }
    final long inputN = sk.getN();
    final double guessNumParts = max(1.0, ceil((double)inputN / tgtPartitionSize));
    this.numLevels = (int)max(1, ceil(log(guessNumParts) / log(maxPartsPerSk)));
    final int partsPerSk = (int)round(pow(guessNumParts, 1.0 / numLevels));
    this.partitionsPerSk = min(partsPerSk, maxPartsPerSk);
    final GenericPartitionBoundaries<T> gpb = sk.getPartitionBoundaries(partitionsPerSk, criteria);
    final StackElement<T> se = new StackElement<>(gpb, 0, "1");
    stack.push(se);
    partitionSearch(stack);
    return finalPartitionList;
  }

  private void partitionSearch(final Stack<StackElement<T>> stack) {
    if (stack.isEmpty()) {
      return;
    }
    final StackElement<T> se = stack.peek();
    final GenericPartitionBoundaries<T> gpb = se.gpb;
    final int numParts = gpb.getNumPartitions();

    if (stack.size() == numLevels) { //at max level
      while (++se.part <= numParts) { //add rows to final partition list
        final PartitionBoundsRow<T> row = new PartitionBoundsRow<>(se);
        finalPartitionList.add(row);
      }
      stack.pop();
      partitionSearch(stack);
    }
    else { //not at max level
      if (++se.part <= numParts) {
        final PartitionBoundsRow<T> row = new PartitionBoundsRow<>(se);
        final S sk = fillReq.getRange(row.lowerBound, row.upperBound, row.rule);
        final GenericPartitionBoundaries<T> gpb2 = sk.getPartitionBoundaries(this.partitionsPerSk, criteria);
        final int level = stack.size() + 1;
        final String partId = se.levelPartId + "." + se.part + "," + level;
        final StackElement<T> se2 = new StackElement<>(gpb2, 0, partId);
        stack.push(se2);
        partitionSearch(stack);
      }
      //done with all parts at this level
      if (stack.isEmpty()) {
        return;
      }
      stack.pop();
      partitionSearch(stack);
    }
  }

  /**
   * Holds data for a Stack element
   */
  public static class StackElement<T> {
    public final GenericPartitionBoundaries<T> gpb;
    public int part;
    public String levelPartId;

    public StackElement(final GenericPartitionBoundaries<T> gpb, final int part, final String levelPartId) {
      this.gpb = gpb;
      this.part = part;
      this.levelPartId = levelPartId;
    }
  }

  /**
   * Defines a row for List of PartitionBounds.
   */
  public static class PartitionBoundsRow<T> {
    public int part;
    public String levelPartId;
    public long approxNumDeltaItems;
    public BoundsRule rule;
    public T lowerBound;
    public T upperBound;

    public PartitionBoundsRow(final StackElement<T> se) {
      final GenericPartitionBoundaries<T> gpb = se.gpb;
      this.part = se.part;
      this.levelPartId = se.levelPartId + "." + part;
      final QuantileSearchCriteria searchCrit = gpb.getSearchCriteria();
      final T[] boundaries = gpb.getBoundaries();
      final int numParts = gpb.getNumPartitions();
      if (searchCrit == INCLUSIVE) {
        if (part == 1) {
          lowerBound = gpb.getMinItem();
          upperBound = boundaries[part];
          rule = BoundsRule.INCLUDE_BOTH;
        } else {
          lowerBound = boundaries[part - 1];
          upperBound = boundaries[part];
          rule = BoundsRule.INCLUDE_UPPER;
        }
      } else { //EXCLUSIVE
        if (part == numParts) {
          lowerBound = boundaries[part - 1];
          upperBound = gpb.getMaxItem();
          rule = BoundsRule.INCLUDE_BOTH;
        } else {
          lowerBound = boundaries[part - 1];
          upperBound = boundaries[part];
          rule = BoundsRule.INCLUDE_LOWER;
        }
      }
      approxNumDeltaItems = gpb.getNumDeltaItems()[part];
    }
  }

}
