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

import static org.apache.datasketches.partitions.BoundsRule.INCLUDE_BOTH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import java.util.List;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.partitions.Partitioner.PartitionBoundsRow;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.testng.annotations.Test;

/**
 * This classic quantiles sketch partitioner example application uses Strings formatted as numbers.
 * The length of each string is the number of characters required to display the decimal digits of <i>N</i>,
 * the number of elements of the entire set of data to be partitioned.
 * As a result, there is a lot of overhead in string processing.
 * Nevertheless, real applications of the approach outlined here, would have a lot of IO overhead that this simple
 * test example does not have.
 */
@SuppressWarnings("unused")
public class ClassicPartitionsTest {

  /**
   * Launch the partitioner as an application with the following arguments as strings:
   * <ul>
   * <li>arg[0]: int k, the size of the sketch</li>
   * <li>arg[1]: long totalN, the total size, in elements, of the data set to parse.</li>
   * <li>arg[2]: long tgtPartitionSize, the target number of elements per resulting partition.</li>
   * <li>arg[3]: int maxPartsPerSk, the maximum number of partitions to be handled by any one sketch</li>
   * </ul>
   * @param args input arguments as defined above.
   */
  public void main(String[] args) {
    final int k, maxPartsPerSk;
    final long totalN, tgtPartitionSize;
    try {
      k = Integer.parseInt(args[0].trim());
      totalN = Long.parseLong(args[1].trim());
      tgtPartitionSize = Long.parseLong(args[2].trim());
      maxPartsPerSk = Integer.parseInt(args[3].trim());
    } catch (NumberFormatException e) { throw new SketchesArgumentException(e.toString()); }
    classicPartitioner(k, totalN, tgtPartitionSize, maxPartsPerSk);
  }

  //@Test //launch from TestNG
  public void checkClassicPartitioner() {
    final int k = 1 << 15;
    final long totalN = 1000_000_000L; //artificially set low so it will execute fast
    final long tgtPartitionSize = 3_000_000L;
    final int maxPartsPerSk = 100;
    classicPartitioner(k, totalN, tgtPartitionSize, maxPartsPerSk);
  }

  /**
   * Programmatic call to classic Partitioner
   * @param k the size of the sketch.
   * @param totalN the total size, in elements, of the data set to parse.
   * @param tgtPartitionSize the target number of elements per resulting partition.
   * @param maxPartsPerSk the maximum number of partitions to be handled by any one sketch.
   */
  public void classicPartitioner(
      final int k,
      final long totalN,
      final long tgtPartitionSize,
      final int maxPartsPerSk) {

    final long startTime_mS = System.currentTimeMillis();
    final ItemsSketchFillRequestLongAsString fillReq = new ItemsSketchFillRequestLongAsString(k, totalN);
    final ItemsSketch<String> sk = fillReq.getRange(1L, totalN, INCLUDE_BOTH);
    final long endFillInitialSketchTime_mS = System.currentTimeMillis();
    final Partitioner<String, ItemsSketch<String>> partitioner = new Partitioner<>(
        tgtPartitionSize,
        maxPartsPerSk,
        fillReq,
        INCLUSIVE);
    final List<PartitionBoundsRow<String>> list = partitioner.partition(sk);
    final long endTime_mS = System.currentTimeMillis();
    final long fillInitialSketchTime_mS = endFillInitialSketchTime_mS - startTime_mS;
    final long partitioningTime_mS = endTime_mS - endFillInitialSketchTime_mS;
    final long totalTime_mS = endTime_mS - startTime_mS;
    PartitionResults.output(
        "Classic",
        list,
        k,
        totalN,
        tgtPartitionSize,
        maxPartsPerSk,
        fillInitialSketchTime_mS,
        partitioningTime_mS,
        totalTime_mS);
  }

}
