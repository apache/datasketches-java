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

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.lang.Math.sqrt;
import static org.apache.datasketches.common.Util.milliSecToString;

import java.util.List;

import org.apache.datasketches.partitions.Partitioner.PartitionBoundsRow;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * Output partitioning results to console.
 */
public class PartitionResults {
  private final static String LS = System.getProperty("line.separator");
  private static final String[] hdr  =
    { "Level.Part", "Partition", "LowerBound", "UpperBound", "ApproxNumItems", "Include Rule" };
  private static final String hdrFmt = "%15s %10s %15s %15s %15s %15s\n";
  private static final String dFmt   = "%15s %10d %15s %15s %15d %15s\n";

  public static void output(
      final String sketchType,
      final List<PartitionBoundsRow<String>> list,
      final int k,
      final QuantileSearchCriteria searchCrit,
      final long totalN,
      final long tgtPartitionSize,
      final int maxPartsPerSk,
      final long fillInitialSketchTime_mS,
      final long partitioningTime_mS,
      final long totalTime_mS) {
    printf(hdrFmt, (Object[]) hdr);
    final int numParts = list.size();
    final double meanPartSize = (double)totalN / numParts;
    double size = 0;
    double sumSizes = 0;
    double sumAbsRelErr = 0;
    double sumSqErr = 0;
    double maxAbsErr = 0;
    for (int i = 0; i < numParts; i++) {
      final PartitionBoundsRow<String> row = list.get(i);
      printf(dFmt, row.levelPartId , (i + 1), row.lowerBound, row.upperBound, row.approxNumDeltaItems, row.rule.name());
      size = row.approxNumDeltaItems;
      sumSizes += size;
      sumAbsRelErr += abs(size / meanPartSize - 1.0);
      final double absErr = abs(size - meanPartSize);
      sumSqErr += absErr * absErr;
      maxAbsErr= max(absErr, maxAbsErr);
    }
    final double meanAbsRelErr = sumAbsRelErr / numParts;
    final double meanSqErr = sumSqErr / numParts; //intermediate value
    final double normMeanSqErr = meanSqErr / (meanPartSize * meanPartSize); //intermediate value
    final double rmsRelErr = sqrt(normMeanSqErr); //a.k.a. Normalized RMS Error or NRMSE
    final double maxAbsErrFraction = maxAbsErr / meanPartSize;

    println(LS + sketchType +" ItemsSketch Partitions Test");
    println(LS + "INPUT:");
    printf("Sketch K              :%,20d\n", k);
    printf("Search Criteria       :%20s\n", searchCrit.name());
    printf("Total N               :%,20d\n", totalN);
    printf("Tgt Partition Size    :%,20d\n", tgtPartitionSize);
    printf("Max Parts Per Sketch  :%20d\n", maxPartsPerSk);

    println(LS + "STATISTICS:");
    printf( "Total ApproxNumItems  :%,20d\n", (long)sumSizes);
    printf( "Mean Partition Size   :%,20.1f\n", meanPartSize);
    printf( "Mean Abs Rel Error    :%20.3f%%\n", meanAbsRelErr * 100);
    printf( "Norm RMS Error        :%20.3f%%\n", rmsRelErr * 100);
    printf( "Max Abs Error Percent :%20.3f%%\n", maxAbsErrFraction * 100);

    println(LS + "TIMINGS:");
    println("FillInitialSketchTime : " + milliSecToString(fillInitialSketchTime_mS));
    println("PartioningTime        : " + milliSecToString(partitioningTime_mS));
    println("Total Time            : " + milliSecToString(totalTime_mS) + LS);
  }

  private final static boolean enablePrinting = true;

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
