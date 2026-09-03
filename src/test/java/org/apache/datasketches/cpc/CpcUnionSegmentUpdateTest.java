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

package org.apache.datasketches.cpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import java.util.Random;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

/**
 * update(MemorySegment) decodes into the union's bit matrix where it can and uncompresses a
 * sketch where it cannot, so it must give the same result as update(heapify(seg)) in every case.
 */
public class CpcUnionSegmentUpdateTest {

  private static byte[] sketchBytes(final int lgK, final long from, final long to) {
    final CpcSketch sk = new CpcSketch(lgK);
    for (long i = from; i < to; i++) { sk.update(i); }
    return sk.toByteArray();
  }

  /** Feeds the same images to both paths and requires byte-identical results. */
  private static void assertSamePath(final String what, final int unionLgK, final byte[][] images) {
    final CpcUnion viaSegment = new CpcUnion(unionLgK);
    final CpcUnion viaSketch = new CpcUnion(unionLgK);
    for (final byte[] image : images) {
      viaSegment.update(MemorySegment.ofArray(image));
      viaSketch.update(CpcSketch.heapify(MemorySegment.ofArray(image)));
    }
    final CpcSketch a = viaSegment.getResult();
    final CpcSketch b = viaSketch.getResult();
    assertEquals(a.getEstimate(), b.getEstimate(), 0.0, what + " estimate");
    assertEquals(a.getLgK(), b.getLgK(), what + " lgK");
    assertEquals(a.toByteArray(), b.toByteArray(), what + " serialized image");
  }

  /** Every flavor of source, against a union that is still empty. */
  @Test
  public void checkEachFlavorIntoFreshUnion() {
    for (int lgK = 4; lgK <= 14; lgK++) {
      final int k = 1 << lgK;
      //counts chosen to land in EMPTY, SPARSE, HYBRID, PINNED and SLIDING
      final int[] counts = {0, 1, 10, k / 16, k / 4, k, 2 * k, 3 * k, 6 * k, 20 * k};
      for (final int n : counts) {
        assertSamePath("lgK=" + lgK + " n=" + n, lgK,
            new byte[][] {sketchBytes(lgK, 0, n)});
      }
    }
  }

  /** Sequences, so the union is exercised in every state a source can arrive into. */
  @Test
  public void checkSequencesAcrossUnionStates() {
    for (final int lgK : new int[] {4, 8, 11, 12}) {
      final int k = 1 << lgK;
      final int[][] sequences = {
          {1, 1, 1},                       //stays sparse
          {1, 10, k},                      //sparse then graduates
          {k, 1},                          //matrix first, then a sparse source
          {k / 16, k, 3 * k, 20 * k},      //ascending through the flavors
          {20 * k, 3 * k, k, k / 16},      //descending
          {0, k, 0, 3 * k},                //empties interleaved
      };
      for (final int[] seq : sequences) {
        final byte[][] images = new byte[seq.length][];
        long from = 0;
        for (int i = 0; i < seq.length; i++) {
          images[i] = sketchBytes(lgK, from, from + seq[i]);
          from += seq[i] / 2; //overlap, so the merges do real work
        }
        assertSamePath("lgK=" + lgK + " seq=" + java.util.Arrays.toString(seq), lgK, images);
      }
    }
  }

  /** A source with a smaller lgK than the union forces the union to be reduced first. */
  @Test
  public void checkSourceWithSmallerLgK() {
    for (final int unionLgK : new int[] {10, 12, 14}) {
      for (final int srcLgK : new int[] {4, 8, 9}) {
        if (srcLgK >= unionLgK) { continue; }
        final int srcK = 1 << srcLgK;
        for (final int n : new int[] {1, srcK / 4, srcK, 3 * srcK, 20 * srcK}) {
          //seed the union at its own lgK first, then merge the smaller source
          assertSamePath("unionLgK=" + unionLgK + " srcLgK=" + srcLgK + " n=" + n, unionLgK,
              new byte[][] {sketchBytes(unionLgK, 0, 1 << unionLgK), sketchBytes(srcLgK, 0, n)});
        }
      }
    }
  }

  /** Randomly fed sketches, which is where the column permutations get exercised hardest. */
  @Test
  public void checkRandomInput() {
    final Random rnd = new Random(9876543L);
    for (final int lgK : new int[] {8, 11, 12}) {
      for (int trial = 0; trial < 6; trial++) {
        final byte[][] images = new byte[5][];
        for (int i = 0; i < images.length; i++) {
          final CpcSketch sk = new CpcSketch(lgK);
          final int n = 1 + rnd.nextInt(40 << lgK);
          for (int j = 0; j < n; j++) { sk.update(rnd.nextLong()); }
          images[i] = sk.toByteArray();
        }
        assertSamePath("random lgK=" + lgK + " trial=" + trial, lgK, images);
      }
    }
  }

  /** A mismatched seed must be rejected, exactly as the sketch path rejects it. */
  @Test
  public void checkSeedMismatchIsRejected() {
    final byte[] image = sketchBytes(10, 0, 1000);
    final CpcUnion u = new CpcUnion(10, Util.DEFAULT_UPDATE_SEED + 1);
    try {
      u.update(MemorySegment.ofArray(image));
      fail("expected a seed hash mismatch");
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }

  /** Interleaving both entry points on one union must behave like using either alone. */
  @Test
  public void checkInterleavedEntryPoints() {
    final int lgK = 11;
    final int k = 1 << lgK;
    final byte[][] images = new byte[6][];
    for (int i = 0; i < images.length; i++) {
      images[i] = sketchBytes(lgK, (long) i * k, ((long) i * k) + (2 * k));
    }

    final CpcUnion mixed = new CpcUnion(lgK);
    final CpcUnion sketchOnly = new CpcUnion(lgK);
    for (int i = 0; i < images.length; i++) {
      if ((i % 2) == 0) {
        mixed.update(MemorySegment.ofArray(images[i]));
      } else {
        mixed.update(CpcSketch.heapify(MemorySegment.ofArray(images[i])));
      }
      sketchOnly.update(CpcSketch.heapify(MemorySegment.ofArray(images[i])));
    }
    assertEquals(mixed.getResult().toByteArray(), sketchOnly.getResult().toByteArray());
  }
}
