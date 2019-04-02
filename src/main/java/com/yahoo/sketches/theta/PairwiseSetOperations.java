/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.SetOperation.createCompactSketch;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * Set Operations where the arguments are presented in pairs as in <i>C = Op(A,B)</i>. These are
 * stateless operations and the result is returned immediately.
 *
 * <p>These operations are designed for convenience and accept Sketches that may be either
 * Heap-based or Direct.
 *
 * @author Lee Rhodes
 */
public class PairwiseSetOperations {

  /**
   * This implements a stateless, pair-wise <i>Intersect</i> operation on sketches
   * that are either Heap-based or Direct.
   * If both inputs are null a null is returned.
   *
   * @param skA The first Sketch argument.
   * @param skB The second Sketch argument.
   * @return the result as an ordered CompactSketch on the heap.
   */
  public static CompactSketch intersect(final Sketch skA, final Sketch skB) {
    if ((skA == null) && (skB == null)) { return null; }
    final short seedHash = (skA == null) ? skB.getSeedHash() : skA.getSeedHash();
    final Intersection inter = new IntersectionImpl(seedHash);
    return inter.intersect(skA, skB, true, null);
  }

  /**
   * This implements a stateless, pair-wise <i>A AND NOT B</i> operation on Sketches
   * that are either Heap-based or Direct.
   * If both inputs are null a null is returned.
   *
   * @param skA The first Sketch argument.
   * @param skB The second Sketch argument.
   * @return the result as an ordered CompactSketch on the heap.
   */
  public static CompactSketch aNotB(final Sketch skA, final Sketch skB) {
    if ((skA == null) && (skB == null)) { return null; }
    final short seedHash = (skA == null) ? skB.getSeedHash() : skA.getSeedHash();
    final HeapAnotB anotb = new HeapAnotB(seedHash);
    return anotb.aNotB(skA, skB, true, null);
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
   * CompactSketches that are either Heap-based or Direct. The returned sketch will be cutback to
   * k if required, similar to the regular Union operation. If a cutback is required, the returned
   * sketch will always be on the heap.
   * If both inputs are null a null is returned. If either sketch is empty its Theta is ignored.
   * If one is null the other is returned, which may be either Direct or heap-based if a cutback
   * is required.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument
   * @param k The upper bound of the number of entries to be retained by the sketch
   * @return the result as an ordered CompactSketch.
   */
  @SuppressWarnings("null")
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB, final int k) {
    //Handle all corner cases with null or empty arguments
    //For backward compatibility, we must allow input empties with Theta < 1.0.
    final int swA, swB;
    if (skA == null) { swA = 1; } else { checkOrdered(skA); swA = skA.isEmpty() ? 2 : 3; }
    if (skB == null) { swB = 1; } else { checkOrdered(skB); swB = skB.isEmpty() ? 2 : 3; }
    final int sw = (swA << 2) | swB;
    switch (sw) {
      case 5: {  //skA == null;  skB == null; return null. Cannot determine seedhash.
        return null;
      }
      case 6: {  //skA == null;  skB == empty; return empty
        final long thetaLong = skB.getThetaLong(); //lgtm [java/dereferenced-value-may-be-null]
        return (thetaLong == Long.MAX_VALUE) ? skB
          : HeapCompactOrderedSketch.compact(new long[0], true, skB.getSeedHash(), 0, Long.MAX_VALUE);
      }
      case 7: {  //skA == null;  skB == valid; return skB
        return maybeCutback(skB, k);
      }
      case 9: {  //skA == empty; skB == null; return empty
        final long thetaLong = skA.getThetaLong(); //lgtm [java/dereferenced-value-may-be-null]
        return (thetaLong == Long.MAX_VALUE) ? skA
          : HeapCompactOrderedSketch.compact(new long[0], true, skA.getSeedHash(), 0, Long.MAX_VALUE);
      }
      case 10: { //skA == empty; skB == empty; return empty
        final short seedHash = seedHashesCheck(skA, skB);
        long thetaLong = skA.getThetaLong(); //lgtm [java/dereferenced-value-may-be-null]
        if (thetaLong == Long.MAX_VALUE) { return skA; }
        thetaLong = skB.getThetaLong(); //lgtm [java/dereferenced-value-may-be-null]
        if (thetaLong == Long.MAX_VALUE) { return skB; }
        return HeapCompactOrderedSketch.compact(new long[0], true, seedHash, 0, Long.MAX_VALUE);
      }
      case 11: { //skA == empty; skB == valid; return skB
        seedHashesCheck(skA, skB);
        return maybeCutback(skB, k);
      }
      case 13: { //skA == valid; skB == null; return skA
        return maybeCutback(skA, k);
      }
      case 14: { //skA == valid; skB == empty; return skA
        seedHashesCheck(skA, skB);
        return maybeCutback(skA, k);
      }
      case 15: { //skA == valid; skB == valid; perform full union
        seedHashesCheck(skA, skB);
        break;
      }
      //default: cannot happen
    }

    //Both sketches are valid with matching seedhashes and ordered
    //Full Union operation:
    final long thetaLongA = skA.getThetaLong(); //lgtm [java/dereferenced-value-may-be-null]
    final long thetaLongB = skB.getThetaLong(); //lgtm [java/dereferenced-value-may-be-null]
    long thetaLong = Math.min(thetaLongA, thetaLongB); //Theta rule
    final long[] cacheA = (skA.hasMemory()) ? skA.getCache() : skA.getCache().clone();
    final long[] cacheB = (skB.hasMemory()) ? skB.getCache() : skB.getCache().clone();
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

    int curCount = indexOut;
    final long[] outArr;
    if (indexOut > k) {
      outArr = Arrays.copyOf(outCache, k); //cutback to k
      curCount = k;
    } else {
      outArr = Arrays.copyOf(outCache, curCount); //copy only valid items
    }
    return createCompactSketch(outArr, false, skA.getSeedHash(), curCount, thetaLong, true, null);
  }

  private static CompactSketch maybeCutback(final CompactSketch csk, final int k) {
    final boolean empty = csk.isEmpty();
    int curCount = csk.getRetainedEntries(true);
    long thetaLong = csk.getThetaLong();
    if (curCount > k) { //cutback to k
      final long[] cache = (csk.hasMemory()) ? csk.getCache() : csk.getCache().clone();
      thetaLong = cache[k];
      final long[] arr = Arrays.copyOf(cache, k);
      curCount = k;
      return createCompactSketch(arr, empty, csk.getSeedHash(), curCount, thetaLong, true, null);
    }
    return csk;
  }

  private static void checkOrdered(final CompactSketch csk) {
    if (!csk.isOrdered()) {
      throw new SketchesArgumentException("Given sketch must be ordered.");
    }
  }

  private static short seedHashesCheck(final Sketch skA, final Sketch skB) {
    final short seedHashA = skA.getSeedHash(); //lgtm [java/dereferenced-value-may-be-null]
    final short seedHashB = skB.getSeedHash(); //lgtm [java/dereferenced-value-may-be-null]
    return Util.checkSeedHashes(seedHashA, seedHashB);
  }

}
