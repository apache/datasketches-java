/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import java.util.Arrays;

import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesException;
import com.yahoo.sketches.Util;

/**
 * Set Operations where the arguments are presented in pairs as in <i>C = Op(A,B)</i>. These are
 * stateless operations and the result is returned immediately. These operations are designed for
 * high performance and only accept ordered, CompactSketches, which may be either Heap-based or
 * Direct.  The returned results are always in the form of an ordered CompactSketch.
 *
 * @author Lee Rhodes
 */
public class PairwiseSetOperations {

  /**
   * This implements a stateless, pair-wise intersection operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   * If both inputs are null a null is returned.
   * If one is null an empty sketch is returned.
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument.
   * @return the result as an ordered CompactSketch.
   */
  public static CompactSketch intersect(final CompactSketch skA, final CompactSketch skB) {
    if ((skA == null) && (skB == null)) { return null; } //no way to construct the seedHash

    if (skA == null) {
      return HeapCompactOrderedSketch
          .compact(new long[0], true, skB.getSeedHash(), 0, skB.getThetaLong());
    }
    if (skB == null) {
      return HeapCompactOrderedSketch
          .compact(new long[0], true, skA.getSeedHash(), 0, skA.getThetaLong());
    }

    //Both sketches are valid, check seedHashes and ordered
    final short seedHash = Util.checkSeedHashes(skA.getSeedHash(), skB.getSeedHash());
    if (!skB.isOrdered()) {
      throw new SketchesArgumentException("skB must be ordered!");
    }
    if (!skA.isOrdered()) {
      throw new SketchesArgumentException("skA must be ordered!");
    }

    //Full Intersection
    final boolean emptyA = skA.isEmpty();
    final boolean emptyB = skB.isEmpty();
    final boolean emptyRule = emptyA || emptyB; //Empty rule is OR

    final long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule

    if (emptyRule) { //even if emptyRule = true, theta can be < 1.0
      return HeapCompactOrderedSketch
          .compact(new long[0], emptyRule, seedHash, 0, thetaLong);
    }

    //Both sketches are non-empty
    final long[] cacheA = (skA.isDirect()) ? skA.getCache() : skA.getCache().clone();
    final long[] cacheB = (skB.isDirect()) ? skB.getCache() : skB.getCache().clone();
    final int aLen = cacheA.length;
    final int bLen = cacheB.length;

    final long[] outCache = new long[Math.min(aLen, bLen)];

    int indexA = 0;
    int indexB = 0;
    int outCount = 0;

    while ((indexA < aLen) && (indexB < bLen)) {
      final long hashA = cacheA[indexA];
      final long hashB = cacheB[indexB];

      if ((hashA >= thetaLong) || (hashB >= thetaLong)) {
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

    return HeapCompactOrderedSketch
        .compact(Arrays.copyOf(outCache, outCount), emptyRule, seedHash, outCount, thetaLong);
  }

  /**
   * This implements a stateless, pair-wise <i>A</i> AND NOT <i>B</i> operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   * If both inputs are null a null is returned.  If skA is null an empty sketch is returned.
   * If skB is null or empty skA is returned.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument.
   * @return the result as an ordered CompactSketch.
   */ //see HeapAnotB.compute() for return rule table
  public static CompactSketch aNotB(final CompactSketch skA, final CompactSketch skB) {
    if ((skA == null) && (skB == null)) { return null; } //no way to construct the seedHash

    if (skA == null) {
      if (!skB.isOrdered()) {
        throw new SketchesException("skB must be ordered!");
      }
      //return rule {ThB, 0, T}
      return HeapCompactOrderedSketch
          .compact(new long[0], true, skB.getSeedHash(), 0, skB.getThetaLong());
    }
    if (skB == null) {
      if (!skA.isOrdered()) {
        throw new SketchesException("skA must be ordered!");
      }
      return skA; //return rule {ThA, |A|, E(a)}
    }

    //Both sketches are valid check seedHashes and ordered
    final short seedHash = Util.checkSeedHashes(skA.getSeedHash(), skB.getSeedHash());
    if (!skB.isOrdered()) {
      throw new SketchesArgumentException("skB must be ordered!");
    }
    if (!skA.isOrdered()) {
      throw new SketchesArgumentException("skA must be ordered!");
    }

    final boolean emptyA = skA.isEmpty();
    final boolean emptyB = skB.isEmpty();
    final boolean bothEmpty = emptyA && emptyB;

    final long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule
    final boolean emptyRule = emptyA; //Empty rule is whatever A is

    if (emptyA || bothEmpty) { //return rule {minT, 0, T}
      return HeapCompactOrderedSketch
          .compact(new long[0], emptyRule, seedHash, 0, thetaLong);
    }

    final long[] cacheA = (skA.isDirect()) ? skA.getCache() : skA.getCache().clone();

    if (emptyB) { //return rule {minT, |A| < minT , E(a)}
      final int curCount = HashOperations.count(cacheA, thetaLong);
      final long[] cache = CompactSketch.compactCache(cacheA, curCount, thetaLong, true);
      return HeapCompactOrderedSketch
          .compact(cache, emptyRule, seedHash, curCount, thetaLong);
    }

    //Both are non-empty
    final long[] cacheB = (skB.isDirect()) ? skB.getCache() : skB.getCache().clone();

    final int aLen = cacheA.length;
    final int bLen = cacheB.length;

    final long[] outCache = new long[aLen];

    int indexA = 0;
    int indexB = 0;
    int indexOut = 0;
    long hashA = cacheA[indexA];
    long hashB = cacheB[indexB];

    while ((indexA < aLen) || (indexB < bLen)) {
      if (hashA == hashB) {
        if (hashA < thetaLong) {
          //reject
          hashA = (++indexA < aLen) ? cacheA[indexA] : thetaLong;
          hashB = (++indexB < bLen) ? cacheB[indexB] : thetaLong;
          continue;
        }
        break;
      }
      else if (hashA < hashB) {
        if (hashA < thetaLong) {
          outCache[indexOut++] = hashA; //keep
          hashA = (++indexA < aLen) ? cacheA[indexA] : thetaLong;
          continue;
        }
        break;
      }
      else { //hashA > hashB
        if (hashB < thetaLong) {
          //reject
          hashB = (++indexB < bLen) ? cacheB[indexB] : thetaLong;
          continue;
        }
        break;
      }
    }

    final int outLen = indexOut;

    return HeapCompactOrderedSketch
        .compact(Arrays.copyOf(outCache, outLen), emptyA, seedHash, outLen, thetaLong);
  }


  /**
   * This implements a stateless, pair-wise union operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   * If both inputs are null a null is returned.
   * If one is null the other is returned, which can be either Heap-based or Direct.
   * This is equivalent to union(skA, skB, k) where k is the default of 4096.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument
   * @return the result as an ordered CompactSketch.
   */
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB) {
    return union(skA, skB, Util.DEFAULT_NOMINAL_ENTRIES);
  }

  /**
   * This implements a stateless, pair-wise union operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   * If both inputs are null a null is returned.
   * If one is null the other is returned, which can be either Heap-based or Direct.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument
   * @param k The upper bound of the number of entries to be retained by the sketch
   * @return the result as an ordered CompactSketch.
   */
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB, final int k) {
    if ((skA == null) && (skB == null)) { return null; } //no way to construct the seedHash

    if (skA == null) {
      if (!skB.isOrdered()) { //must be ordered
        throw new SketchesException("skB must be ordered!");
      }
      if (skB.getRetainedEntries(true) > k) { //guarantees cutback to k
        final long[] cacheB = (skB.isDirect()) ? skB.getCache() : skB.getCache().clone();
        final long thetaLong = cacheB[k];
        final long[] arrB = Arrays.copyOf(cacheB, k);
        return HeapCompactOrderedSketch
            .compact(arrB, skB.isEmpty(), skB.getSeedHash(), k, thetaLong);
      }
      return skB;
    }

    if (skB == null) {
      if (!skA.isOrdered()) { //must be ordered
        throw new SketchesException("skA must be ordered!");
      }
      if (skA.getRetainedEntries(true) > k) { //guarantees cutback to k
        final long[] cacheA = (skA.isDirect()) ? skA.getCache() : skA.getCache().clone();
        final long thetaLong = cacheA[k];
        final long[] arrA = Arrays.copyOf(cacheA, k);
        return HeapCompactOrderedSketch
            .compact(arrA, skA.isEmpty(), skA.getSeedHash(), k, thetaLong);
      }
      return skA;
    }

    //Both sketches are valid check seedHashes and ordered
    final short seedHash = Util.checkSeedHashes(skA.getSeedHash(), skB.getSeedHash());
    if (!skB.isOrdered()) {
      throw new SketchesArgumentException("skB must be ordered!");
    }
    if (!skA.isOrdered()) {
      throw new SketchesArgumentException("skA must be ordered!");
    }

    final boolean emptyA = skA.isEmpty();
    final boolean emptyB = skB.isEmpty();
    final boolean bothEmptyRule = emptyA && emptyB; //Empty rule is AND

    if (bothEmptyRule) {
      return (skA.getThetaLong() < skB.getThetaLong()) ? skA : skB;
    }

    long thetaLong = Math.min(skA.getThetaLong(), skB.getThetaLong()); //Theta rule

    // Attempting to shortcut this if one of the arguments is "empty" turns out to be complex.
    // The theta of an empty sketch could be < 1.0 and will empact the other sketch.

    //Full Union operation
    final long[] cacheA = (skA.isDirect()) ? skA.getCache() : skA.getCache().clone();
    final long[] cacheB = (skB.isDirect()) ? skB.getCache() : skB.getCache().clone();
    final int aLen = cacheA.length;
    final int bLen = cacheB.length;

    final long[] outCache = new long[aLen + bLen];

    int indexA = 0;
    int indexB = 0;
    int indexOut = 0;
    long hashA = (aLen == 0) ? thetaLong : cacheA[indexA];
    long hashB = (bLen == 0) ? thetaLong : cacheB[indexB];

    while ((indexA < aLen) || (indexB < bLen)) {
      if (hashA == hashB) {
        if (hashA < thetaLong) {
          if (indexOut >= k) {
            thetaLong = hashA;
            break;
          }
          outCache[indexOut++] = hashA;
          hashA = (++indexA < aLen) ? cacheA[indexA] : thetaLong;
          hashB = (++indexB < bLen) ? cacheB[indexB] : thetaLong;
          continue;
        }
        break;
      }
      else if (hashA < hashB) {
        if (hashA < thetaLong) {
          if (indexOut >= k) {
            thetaLong = hashA;
            break;
          }
          outCache[indexOut++] = hashA;
          hashA = (++indexA < aLen) ? cacheA[indexA] : thetaLong;
          continue;
        }
        break;
      }
      else { //hashA > hashB
        if (hashB < thetaLong) {
          if (indexOut >= k) {
            thetaLong = hashB;
            break;
          }
          outCache[indexOut++] = hashB;
          hashB = (++indexB < bLen) ? cacheB[indexB] : thetaLong;
          continue;
        }
        break;
      }
    }

    final int outLen = indexOut;

    return HeapCompactOrderedSketch
        .compact(Arrays.copyOf(outCache, outLen), bothEmptyRule, seedHash, outLen, thetaLong);
  }

}
