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

package org.apache.datasketches.theta;

import static org.apache.datasketches.theta.SetOperation.createCompactSketch;

import java.util.Arrays;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;

/**
 * Set Operations where the arguments are presented in pairs as in <i>C = Op(A,B)</i>. These are
 * stateless operations and the result is returned immediately.
 *
 * <p>These operations are designed for convenience and accept Sketches that may be either
 * Heap-based or Direct.
 *
 * @author Lee Rhodes
 * @deprecated  This class has been deprecated as equivalent functionality has been added to the
 * SetOperation classes: Union, Intersection and AnotB.
 */
@Deprecated
public class PairwiseSetOperations {

  /**
   * This implements a stateless, pair-wise <i>Intersect</i> operation on sketches
   * that are either Heap-based or Direct.
   * If either inputs are null or empty an EmptyCompactSketch is returned.
   *
   * @param skA The first Sketch argument.
   * @param skB The second Sketch argument.
   * @return the result as an ordered CompactSketch on the heap.
   * @deprecated Use {@link Intersection#intersect(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  public static CompactSketch intersect(final Sketch skA, final Sketch skB) {
    if (((skA == null) || (skA instanceof EmptyCompactSketch))
        || ((skB == null) || (skB instanceof EmptyCompactSketch))) {
      return EmptyCompactSketch.getInstance();
    }
    final short seedHash = skA.getSeedHash();
    final Intersection inter = new IntersectionImpl(seedHash);
    return inter.intersect(skA, skB, true, null);
  }

  /**
   * This implements a stateless, pair-wise <i>A AND NOT B</i> operation on Sketches
   * that are either Heap-based or Direct.
   * If both inputs are null an EmptyCompactSketch is returned.
   *
   * @param skA The first Sketch argument.
   * @param skB The second Sketch argument.
   * @return the result as an ordered CompactSketch on the heap.
   * @deprecated Use {@link AnotB#aNotB(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  public static CompactSketch aNotB(final Sketch skA, final Sketch skB) {
    if (((skA == null) || (skA instanceof EmptyCompactSketch))
        && ((skB == null) || (skB instanceof EmptyCompactSketch))) {
      return EmptyCompactSketch.getInstance();
    }
    final short seedHash = ((skA == null) || (skA instanceof EmptyCompactSketch))
        ? skB.getSeedHash() : skA.getSeedHash(); // lgtm [java/dereferenced-value-may-be-null]
    final AnotBimpl anotb = new AnotBimpl(seedHash);
    return anotb.aNotB(skA, skB, true, null);
  }

  /**
   * This implements a stateless, pair-wise union operation on ordered,
   * CompactSketches that are either Heap-based or Direct.
   * Having the input sketches be compact and ordered enables extremely fast union operation.
   * If both inputs are null an EmptyCompactSketch is returned.
   * If one is null the other is returned, which can be either Heap-based or Direct.
   * This is equivalent to union(skA, skB, k) where k is the default of 4096.
   *
   * @param skA The first ordered, CompactSketch argument.
   * @param skB The second ordered, CompactSketch argument
   * @return the result as an ordered CompactSketch.
   * @deprecated Please use {@link Union#union(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
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
   * @deprecated Please use {@link Union#union(Sketch, Sketch)} instead, which has more
   * complete seed handling.
   */
  @Deprecated
  @SuppressWarnings("null")
  public static CompactSketch union(final CompactSketch skA, final CompactSketch skB, final int k) {
    //Handle all corner cases with null or empty arguments
    //For backward compatibility, we must allow input empties with Theta < 1.0.
    final int swA, swB;
    swA = ((skA == null) || (skA instanceof EmptyCompactSketch)) ? 0 : 2;
    swB = ((skB == null) || (skB instanceof EmptyCompactSketch)) ? 0 : 1;
    final int sw = swA | swB;
    switch (sw) {
      case 0: { //skA == null/ECS;  skB == null/ECS; return EmptyCompactSketch.
        return EmptyCompactSketch.getInstance();
      }
      case 1: { //skA == null/ECS;  skB == valid; return skB
        checkOrdered(skB);
        return maybeCutback(skB, k);
      }
      case 2: { //skA == valid; skB == null/ECS; return skA
        checkOrdered(skA);
        return maybeCutback(skA, k);
      }
      case 3: { //skA == valid; skB == valid; perform full union
        checkOrdered(skA);
        checkOrdered(skB);
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
    if (indexOut > k) { //unlikely
      outArr = Arrays.copyOf(outCache, k); //cutback to k, just in case
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
