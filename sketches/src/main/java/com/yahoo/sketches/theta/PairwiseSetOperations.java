/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

public class PairwiseSetOperations {

  /**
   * This implements a stateless, pair-wise intersection on Sketches that are already compact and
   * ordered. This will work with sketches that are either on-heap or off-heap.
   *
   * @param skA The first sketch argument. Must be compact, ordered and not null.
   * @param skB The second sketch argument. Must be compact, ordered and not null.
   * @return the result of the intersection as a heap, compact, ordered sketch.
   */
  public static Sketch intersect(Sketch skA, Sketch skB) {
    if (!skA.isCompact() || !skA.isOrdered() || !skB.isCompact() || !skB.isOrdered()) {
      throw new SketchesArgumentException("Require compact, ordered sketch, got: "
          + skA.getClass().getSimpleName() + ", " + skB.getClass().getSimpleName());
    }
    short seedHashA = skA.getSeedHash();
    short seedHashB = skB.getSeedHash();
    Util.checkSeedHashes(seedHashA, seedHashB);

    long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    int indexA = 0;
    int indexB = 0;
    int count = 0;

    long[] cacheA = skA.getCache();
    long[] cacheB = skB.getCache();

    long[] outCache = new long[Math.min(cacheA.length, cacheB.length)];

    while ((indexA < cacheA.length) && (indexB < cacheB.length)) {
      long hashA = cacheA[indexA];
      long hashB = cacheB[indexB];

      if (hashA >= thetaLong || hashB >= thetaLong) {
        break;
      }

      if (hashA == hashB) {
        outCache[count++] = hashA;
        ++indexA;
        ++indexB;
      } else if (hashA < hashB) {
        ++indexA;
      } else {
        ++indexB;
      }
    }

    boolean empty = skA.isEmpty() || skB.isEmpty(); //empty rule is OR

    return new HeapCompactOrderedSketch(
        Arrays.copyOf(outCache, count), empty, seedHashA, count, thetaLong);
  }



  /**
   * This implements a stateless, pair-wise <i>A</i> AND NOT <i>B</i> operation on Sketches that are
   * already compact and ordered. This will work with sketches that are either on-heap or off-heap.
   *
   * @param skA The first sketch argument. Must be compact, ordered and not null.
   * @param skB The second sketch argument. Must be compact, ordered and not null.
   * @return the result of the <i>A</i> AND NOT <i>B</i> as a heap, compact, ordered sketch.
   */
  public static Sketch aNotB(Sketch skA, Sketch skB) {
    if (!skA.isCompact() || !skA.isOrdered() || !skB.isCompact() || !skB.isOrdered()) {
      throw new SketchesArgumentException("Require compact, ordered sketch, got: "
          + skA.getClass().getSimpleName() + ", " + skB.getClass().getSimpleName());
    }
    short seedHashA = skA.getSeedHash();
    short seedHashB = skB.getSeedHash();
    Util.checkSeedHashes(seedHashA, seedHashB);

    long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    int indexA = 0;
    int indexB = 0;
    int count = 0;

    long[] cacheA = skA.getCache();
    long[] cacheB = skB.getCache();

    long[] outCache = new long[cacheA.length];

    while (indexA < cacheA.length) {
      long hashA = cacheA[indexA];
      long hashB = (indexB >= cacheB.length) ? thetaLong : cacheB[indexB];

      if (hashA >= thetaLong) {
        break;
      }

      if (hashA == hashB) {
        ++indexA;
        ++indexB;
      } else if (hashA < hashB) {
        outCache[count++] = hashA;
        ++indexA;
      } else {
        ++indexB;
      }
    }

    boolean empty = skA.isEmpty();

    return new HeapCompactOrderedSketch(
        Arrays.copyOf(outCache, count), empty, seedHashA, count, thetaLong);
  }

  /**
   * This implements a stateless, pair-wise union on Sketches that are already compact and
   * ordered. This will work with sketches that are either on-heap or off-heap.
   *
   * @param skA The first sketch argument. Must be compact, ordered and not null.
   * @param skB The second sketch argument. Must be compact, ordered and not null.
   * @return the result of the union as a heap, compact, ordered sketch.
   */
  public static Sketch union(Sketch skA, Sketch skB) {
    if (!skA.isCompact() || !skA.isOrdered() || !skB.isCompact() || !skB.isOrdered()) {
      throw new SketchesArgumentException("Require compact, ordered sketch, got: "
          + skA.getClass().getSimpleName() + ", " + skB.getClass().getSimpleName());
    }
    short seedHashA = skA.getSeedHash();
    short seedHashB = skB.getSeedHash();
    Util.checkSeedHashes(seedHashA, seedHashB);

    long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    int indexA = 0;
    int indexB = 0;
    int count = 0;

    long[] cacheA = skA.getCache();
    long[] cacheB = skB.getCache();

    long[] outCache = new long[cacheA.length + cacheB.length];

    while ((indexA < cacheA.length) || (indexB < cacheB.length)) {
      long hashA = (indexA >= cacheA.length) ? thetaLong : cacheA[indexA];
      long hashB = (indexB >= cacheB.length) ? thetaLong : cacheB[indexB];

      if (hashA >= thetaLong && hashB >= thetaLong) {
        break;
      }

      if (hashA == hashB) {
        outCache[count++] = hashA;
        ++indexA;
        ++indexB;
      } else if (hashA < hashB) {
        outCache[count++] = hashA;
        ++indexA;
      } else {
        outCache[count++] = hashB;
        ++indexB;
      }
    }

    boolean empty = skA.isEmpty() || skB.isEmpty(); //empty rule is OR

    return new HeapCompactOrderedSketch(
        Arrays.copyOf(outCache, count), empty, seedHashA, count, thetaLong);
  }

}
