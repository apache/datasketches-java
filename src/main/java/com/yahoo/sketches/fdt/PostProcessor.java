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
 * This processes the contents of a FDT sketch to extract the
 * primary keys with the most frequent unique combinations of the non-primary dimensions.
 *
 * @author Lee Rhodes
 */
public class PostProcessor {
  private final FdtSketch sketch;
  private final char sep;
  private int groupCount;
  private Group group; //uninitialized

  //simple hash-map
  private boolean mapValid;
  private final int mapArrSize;
  private final long[] hashArr;
  private final String[] priKeyArr;
  private final int[] counterArr;

  /**
   * Construct with a populated FdtSketch
   * @param sketch the given sketch to query.
   * @param group the Group
   * @param sep the separator character
   */
  public PostProcessor(final FdtSketch sketch, final Group group, final char sep) {
    this.sketch = sketch;
    this.sep = sep;
    final int numEntries = sketch.getRetainedEntries();
    mapArrSize = ceilingPowerOf2((int)(numEntries / 0.75));
    hashArr = new long[mapArrSize];
    priKeyArr = new String[mapArrSize];
    counterArr = new int[mapArrSize];
    mapValid = false;
    this.group = group;
  }

  /**
   * Returns the number of groups in the final sketch.
   * @return the number of groups in the final sketch.
   */
  public int getGroupCount() {
    return groupCount;
  }

  /**
   * Return the most frequent Groups associated with Primary Keys based on the size of the groups.
   * @param priKeyIndices the indices of the primary dimensions
   * @param numStdDev the number of standard deviations for the error bounds, this value is an
   * integer and must be one of 1, 2, or 3.
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param limit the maximum number of rows to return. If &le; 0, all rows will be returned.
   * @return the most frequent Groups associated with Primary Keys based on the size of the groups.
   */
  public List<Group> getGroupList(final int[] priKeyIndices, final int numStdDev,
      final int limit) {
    //allows subsequent queries with different priKeyIndices without rebuilding the map
    if (!mapValid) { populateMap(priKeyIndices); }
    return populateList(numStdDev, limit);
  }

  /**
   * Scan each entry in the sketch. Count the number of duplicate occurrences of each
   * primary key in a hash map.
   * @param priKeyIndices identifies the primary key indices
   */
  private void populateMap(final int[] priKeyIndices) {
    final SketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    Arrays.fill(hashArr, 0L);
    Arrays.fill(priKeyArr, null);
    Arrays.fill(counterArr, 0);
    groupCount = 0;
    final int lgMapArrSize = Integer.numberOfTrailingZeros(mapArrSize);

    while (it.next()) {
      final String[] arr = it.getSummary().getValue();
      final String priKey = getPrimaryKey(arr, priKeyIndices, sep);
      final long hash = stringHash(priKey);
      final int index = hashSearchOrInsert(hashArr, lgMapArrSize, hash);
      if (index < 0) { //was empty, hash inserted
        final int idx = -(index + 1); //actual index
        counterArr[idx] = 1;
        groupCount++;
        priKeyArr[idx] = priKey;
      } else { //found, duplicate
        counterArr[index]++; //increment
      }
    }
    mapValid = true;
  }

  /**
   * Create the list of groups along with the error statistics
   * @param numStdDev number of standard deviations
   * @param limit the maximum size of the list to return
   * @return the list of groups along with the error statistics
   */
  private List<Group> populateList(final int numStdDev, final int limit) {
    final List<Group> list = new ArrayList<>();
    for (int i = 0; i < mapArrSize; i++) {
      if (hashArr[i] != 0) {
        final String priKey = priKeyArr[i];
        final int count = counterArr[i];
        final double est = sketch.getEstimate(count);
        final double ub = sketch.getUpperBound(numStdDev, count);
        final double lb = sketch.getLowerBound(numStdDev, count);
        final double thresh = (double) count / sketch.getRetainedEntries();
        final double rse = (sketch.getUpperBound(1, count) / est) - 1.0;
        final Group gp = group.copy();
        gp.init(priKey, count, est, ub, lb, thresh, rse);
        list.add(gp);
      }
    }
    list.sort(null); //Comparable implemented in Group
    final int totLen = list.size();

    final List<Group> returnList;
    if ((limit > 0) && (limit < totLen)) {
      returnList = list.subList(0, limit);
    } else {
      returnList = list;
    }
    return returnList;
  }

  /**
   * Extract simple string Primary Key defined by the <i>priKeyIndices</i> from the given tuple.
   * @param tuple the given tuple containing the Primary Key
   * @param priKeyIndices the indices indicating the ordering and selection of dimensions defining
   * the Primary Key
   * @param sep the separator character
   * @return a simple string Primary Key defined by the <i>priKeyIndices</i> from the given tuple.
   */
  //also used by test
  private static String getPrimaryKey(final String[] tuple, final int[] priKeyIndices,
      final char sep) {
    assert priKeyIndices.length < tuple.length;
    final StringBuilder sb = new StringBuilder();
    final int keys = priKeyIndices.length;
    for (int i = 0; i < keys; i++) {
      final int idx = priKeyIndices[i];
      sb.append(tuple[idx]);
      if ((i + 1) < keys) { sb.append(sep); }
    }
    return sb.toString();
  }

}
