/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fdt;

import static com.yahoo.sketches.HashOperations.hashSearchOrInsert;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary.stringHash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;

/**
 * @author Lee Rhodes
 */
public class PostProcessor {
  private final FdtSketch sketch;
  private final int arrSize;
  private final int lgArrSize;
  private int groupCount;
  private int totCount;
  private boolean mapValid;

  private final long[] hashArr;
  private final String[] priKeyArr;
  private final int[] counterArr;

  /**
   * Construct with the given FdtSketch
   * @param sketch the given sketch to analyze.
   */
  public PostProcessor(final FdtSketch sketch) {
    this.sketch = sketch;
    final int numEntries = sketch.getRetainedEntries();
    arrSize = ceilingPowerOf2((int)(numEntries / 0.75));
    lgArrSize = Integer.numberOfTrailingZeros(arrSize);
    hashArr = new long[arrSize];
    priKeyArr = new String[arrSize];
    counterArr = new int[arrSize];
    mapValid = false;
  }

  /**
   * Returns the number of groups in the final sketch.
   * @return the number of groups in the final sketch.
   */
  public int getGroupCount() {
    return groupCount;
  }

  /**
   * Returns the total count of occurrences for the retained groups in the sketch.
   * @return the total count of occurrences for the retained groups in the sketch.
   */
  public int getTotalCount() {
    return totCount;
  }

  /**
   * Return the most frequent primary dimensions based on the distinct count of the combinations
   * of the non-primary dimensions.
   * @param priKeyIndices the indices of the primary dimensions
   * @param numStdDev the number of standard deviations for the error bounds, this value is an
   * integer and must be one of 1, 2, or 3.
   * @param limit the maximum number of rows to return. If &le; 0, all rows will be returned.
   * @return the most frequent primary dimensions based on the distinct count of the combinations
   * of the non-primary dimensions.
   */
  public List<Group<String>> getGroupList(final int[] priKeyIndices, final int numStdDev,
      final int limit) {
    if (!mapValid) { populateMap(priKeyIndices); }
    return populateList(numStdDev, limit);
  }

  /**
   * Scan each entry in the sketch. Count the number of duplicate occurrences of each
   * primary key in a hash map. The number of primary keys in the map is the group count.
   * @param priKeyIndices identifies the primary key indices
   */
  private void populateMap(final int[] priKeyIndices) {
    final SketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    Arrays.fill(hashArr, 0L);
    Arrays.fill(priKeyArr, null);
    Arrays.fill(counterArr, 0);
    totCount = 0;
    groupCount = 0;

    while (it.next()) {
      final String[] arr = it.getSummary().getValue();
      final String priKey = getPrimaryKey(arr, priKeyIndices);
      final long hash = stringHash(priKey);
      final int index = hashSearchOrInsert(hashArr, lgArrSize, hash);
      if (index < 0) { //was empty, hash inserted
        final int idx = -(index + 1); //actual index
        counterArr[idx] = 1;
        totCount++;
        groupCount++;
        priKeyArr[idx] = priKey;
      } else { //found, duplicate
        counterArr[index]++; //increment
        totCount++;
      }
    }
    mapValid = true;
  }

  private List<Group<String>> populateList(final int numStdDev, final int limit) {
    final List<Group<String>> list = new ArrayList<>();
    for (int i = 0; i < arrSize; i++) {
      if (hashArr[i] != 0) {
        final String priKey = priKeyArr[i];
        final int count = counterArr[i];
        final double est = sketch.getEstimate(count);
        final double ub = sketch.getUpperBound(numStdDev, count);
        final double lb = sketch.getLowerBound(numStdDev, count);
        final double thresh = (double) count / sketch.getRetainedEntries();
        final double rse = sketch.getUpperBound(1, count) / est;
        final Group<String> row = new Group<>(priKey, count, est, ub, lb, thresh, rse);
        list.add(row);
      }
    }
    list.sort(null);
    final int totLen = list.size();

    final List<Group<String>> returnList;
    if ((limit > 0) && (limit < totLen)) {
      returnList = list.subList(0, limit);
    } else {
      returnList = list;
    }
    return returnList;
  }

  //also used by test
  static String getPrimaryKey(final String[] arr, final int[] priKeyIndices) {
    assert priKeyIndices.length < arr.length;
    final StringBuilder sb = new StringBuilder();
    final int keys = priKeyIndices.length;
    for (int i = 0; i < keys; i++) {
      final int idx = priKeyIndices[i];
      sb.append(arr[idx]);
      if ((i + 1) < keys) { sb.append(","); }
    }
    return sb.toString();
  }

}
