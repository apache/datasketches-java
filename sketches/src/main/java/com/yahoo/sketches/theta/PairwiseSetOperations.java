/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * Set Operations where the arguments are presented in pairs as in <i>C = Op(A,B)</i>. These are
 * stateless operations and the result is returned immediately. These operations are designed for
 * high performance and only accept ordered, CompactSketches that may be either Heap-based or
 * Direct.  The returned results are always in the form of a Heap-based, ordered CompactSketch.
 *
 * @author Lee Rhodes
 */
public class PairwiseSetOperations {

  /**
   * This implements a stateless, pair-wise intersection operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   *
   * @param skA The first ordered, CompactSketch argument that must not be null.
   * @param skB The second ordered, CompactSketch argument that must not be null.
   * @return the result as a Heap-based, ordered CompactSketch.
   */
  public static CompactSketch intersect(final CompactSketch skA, final CompactSketch skB) {
    final short seedHash = checkOrderedAndSeedHash(skA, skB);

    final long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    int indexA = 0;
    int indexB = 0;
    int outCount = 0;

    final long[] cacheA = skA.getCache();
    final long[] cacheB = skB.getCache();

    final long[] outCache = new long[Math.min(cacheA.length, cacheB.length)];

    while ((indexA < cacheA.length) && (indexB < cacheB.length)) {
      final long hashA = cacheA[indexA];
      final long hashB = cacheB[indexB];

      if (hashA >= thetaLong || hashB >= thetaLong) {
        break;
      }

      if (hashA == hashB) {
        outCache[outCount++] = hashA;
        ++indexA;
        ++indexB;
      } else if (hashA < hashB) {
        ++indexA;
      } else {
        ++indexB;
      }
    }

    final boolean empty = skA.isEmpty() || skB.isEmpty(); //Empty rule is OR

    return new HeapCompactOrderedSketch(
        Arrays.copyOf(outCache, outCount), empty, seedHash, outCount, thetaLong);
  }

  /**
   * This implements a stateless, pair-wise <i>A</i> AND NOT <i>B</i> operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   *
   * @param skA The first ordered, CompactSketch argument that must not be null.
   * @param skB The second ordered, CompactSketch argument that must not be null.
   * @return the result as a Heap-based, ordered CompactSketch.
   */
  public static CompactSketch aNotB(final CompactSketch skA, final CompactSketch skB) {
    final short seedHash = checkOrderedAndSeedHash(skA, skB);

    final long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    int indexA = 0;
    int indexB = 0;
    int outCount = 0;

    final long[] cacheA = skA.getCache();
    final long[] cacheB = skB.getCache();

    final long[] outCache = new long[cacheA.length];

    while (indexA < cacheA.length) {
      final long hashA = cacheA[indexA];
      final long hashB = (indexB >= cacheB.length) ? thetaLong : cacheB[indexB];

      if (hashA >= thetaLong) {
        break;
      }

      if (hashA == hashB) {
        ++indexA;
        ++indexB;
      } else if (hashA < hashB) {
        outCache[outCount++] = hashA;
        ++indexA;
      } else {
        ++indexB;
      }
    }

    final boolean empty = skA.isEmpty(); //Empty rule is whatever A is

    return new HeapCompactOrderedSketch(
        Arrays.copyOf(outCache, outCount), empty, seedHash, outCount, thetaLong);
  }

  /**
   * This implements a stateless, pair-wise union operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   *
   * @param skA The first ordered, CompactSketch argument that must not be null.
   * @param skB The second ordered, CompactSketch argument that must not be null.
   * @return the result as a Heap-based, ordered CompactSketch.
   */
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB) {
    final short seedHash = checkOrderedAndSeedHash(skA, skB);

    final long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    int indexA = 0;
    int indexB = 0;
    int outCount = 0;

    final long[] cacheA = skA.getCache();
    final long[] cacheB = skB.getCache();

    final long[] outCache = new long[cacheA.length + cacheB.length];

    while ((indexA < cacheA.length) || (indexB < cacheB.length)) {
      final long hashA = (indexA >= cacheA.length) ? thetaLong : cacheA[indexA];
      final long hashB = (indexB >= cacheB.length) ? thetaLong : cacheB[indexB];

      if (hashA >= thetaLong && hashB >= thetaLong) {
        break;
      }

      if (hashA == hashB) {
        outCache[outCount++] = hashA;
        ++indexA;
        ++indexB;
      } else if (hashA < hashB) {
        outCache[outCount++] = hashA;
        ++indexA;
      } else {
        outCache[outCount++] = hashB;
        ++indexB;
      }
    }

    final boolean empty = skA.isEmpty() && skB.isEmpty(); //Empty rule is AND

    return new HeapCompactOrderedSketch(
        Arrays.copyOf(outCache, outCount), empty, seedHash, outCount, thetaLong);
  }

  private static final short checkOrderedAndSeedHash(
      final CompactSketch skA, final CompactSketch skB) {
    if (!skA.isOrdered() || !skB.isOrdered()) {
      throw new SketchesArgumentException("Sketch must be ordered, got: "
          + skA.getClass().getSimpleName() + ", " + skB.getClass().getSimpleName());
    }
    final short seedHashA = skA.getSeedHash();
    final short seedHashB = skB.getSeedHash();
    Util.checkSeedHashes(seedHashA, seedHashB);
    return seedHashA;
  }
}
