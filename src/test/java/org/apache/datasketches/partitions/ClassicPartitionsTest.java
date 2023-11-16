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

import static org.apache.datasketches.common.Util.milliSecToString;
import static org.apache.datasketches.partitions.BoundsRule.INCLUDE_BOTH;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import java.util.List;

import org.apache.datasketches.partitions.Partitioner;
import org.apache.datasketches.partitions.Partitioner.PartitionBoundsRow;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class ClassicPartitionsTest {
  private final int k = 1 << 15;
  private final long totalN = 100_000_000L;
  private final long tgtPartitionSize = (long)3e6;
  private final int maxPartsPerSk = 100;

  //@Test
  public void checkClassicPartitioner() {
    println("Classic ItemsSketch Partitions Test");
    printf("Sketch K             :%,20d\n", k);
    printf("Total N              :%,20d\n", totalN);
    printf("Tgt Partition Size   :%,20d\n", tgtPartitionSize);
    printf("Max Parts Per Sketch :%20d\n", maxPartsPerSk);

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
    outputList(list);

    final long endTime_mS = System.currentTimeMillis();
    final long fillInitialSketchTime_mS = endFillInitialSketchTime_mS - startTime_mS;
    final long partitioningTime_mS = endTime_mS - endFillInitialSketchTime_mS;
    final long totalTime_mS = endTime_mS - startTime_mS;
    println("");
    println("FillInitialSketchTime: " + milliSecToString(fillInitialSketchTime_mS));
    println("PartioningTime       : " + milliSecToString(partitioningTime_mS));
    println("Total Time           : " + milliSecToString(totalTime_mS));
  }

  private static final String[] hdr  =
    { "Level.Part", "Partition", "LowerBound", "UpperBound", "ApproxNumItems", "Include Rule" };
  private static final String hdrFmt = "%15s %10s %15s %15s %15s %15s\n";
  private static final String dFmt   = "%15s %10d %15s %15s %15d %15s\n";

  void outputList(final List<PartitionBoundsRow<String>> list) {
    printf(hdrFmt, (Object[]) hdr);
    final int numParts = list.size();
    final double meanPartSize = (double)totalN / numParts;
    double size = 0;
    double sumSizes = 0;
    double sumAbsRelErr = 0;
    double sumSqErr = 0;
    for (int i = 0; i < numParts; i++) {
      final PartitionBoundsRow<String> row = list.get(i);
      printf(dFmt, row.levelPartId , (i + 1), row.lowerBound, row.upperBound, row.approxNumDeltaItems, row.rule.name());
      size = row.approxNumDeltaItems;
      sumSizes += size;
      sumAbsRelErr += Math.abs(size / meanPartSize - 1.0);
      final double absErr = size - meanPartSize;
      sumSqErr += absErr * absErr;
    }
    final double meanAbsRelErr = sumAbsRelErr / numParts;
    final double meanSqErr = sumSqErr / numParts; //intermediate value
    final double normMeanSqErr = meanSqErr / (meanPartSize * meanPartSize); //intermediate value
    final double rmsRelErr = Math.sqrt(normMeanSqErr); //a.k.a. Normalized RMS Error or NRMSE

    printf("Total ApproxNumItems :%,20d\n",(long)sumSizes);
    printf("Mean Partition Size  :%,20.1f\n",meanPartSize);
    printf("Mean Abs Rel Error   :%20.3f%%\n",meanAbsRelErr * 100);
    printf("Norm RMS Error       :%20.3f%%\n",rmsRelErr * 100);
  }

  private final static boolean enablePrinting = true;

  /**
   * @param o the Object to print
   */
  private static final void print(final Object o) {
    if (enablePrinting) { System.out.print(o.toString()); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ...args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

}
