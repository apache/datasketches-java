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

package org.apache.datasketches.tuple;

import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;
import static org.apache.datasketches.common.Util.computeSeedHash;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;
import org.apache.datasketches.fdt.FdtSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryDeserializer;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.adouble.DoubleSummarySetOperations;
import org.apache.datasketches.tuple.adouble.DoubleTupleSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.datasketches.tuple.strings.ArrayOfStringsTupleSketch;
import org.testng.annotations.Test;

/**
 * Checks TupleSketches and their set operations created with a custom update seed.
 */
@SuppressWarnings("deprecation")
public class TupleCustomSeedTest {
  private static final long SEED = 123L;
  private static final short SEED_HASH = computeSeedHash(SEED);
  private static final short DEFAULT_SEED_HASH = computeSeedHash(DEFAULT_UPDATE_SEED);
  private static final int SEED_HASH_SHORT = 6;

  private final DoubleSummaryFactory factory = new DoubleSummaryFactory(DoubleSummary.Mode.Sum);
  private final DoubleSummarySetOperations dsso = new DoubleSummarySetOperations();
  private final DoubleSummaryDeserializer deser = new DoubleSummaryDeserializer();

  private UpdatableTupleSketch<Double, DoubleSummary> tupleSketch(final long seed, final int start, final int n) {
    final UpdatableTupleSketch<Double, DoubleSummary> sk =
        new UpdatableTupleSketchBuilder<>(factory).setSeed(seed).build();
    for (int i = start; i < (start + n); i++) { sk.update(i, 1.0); }
    return sk;
  }

  private static UpdatableThetaSketch thetaSketch(final long seed, final int start, final int n) {
    final UpdatableThetaSketch sk = UpdatableThetaSketch.builder().setSeed(seed).build();
    for (int i = start; i < (start + n); i++) { sk.update(i); }
    return sk;
  }

  private static short seedHashOf(final byte[] bytes) {
    return ByteArrayUtil.getShortLE(bytes, SEED_HASH_SHORT);
  }

  @Test
  public void checkSeedHashIsCarried() {
    final UpdatableTupleSketch<Double, DoubleSummary> sk = tupleSketch(SEED, 0, 10);
    assertEquals(sk.getSeedHash(), SEED_HASH);
    assertEquals(sk.copy().getSeedHash(), SEED_HASH);
    assertEquals(sk.compact().getSeedHash(), SEED_HASH);
    assertEquals(seedHashOf(sk.compact().toByteArray()), SEED_HASH);
    assertEquals(tupleSketch(DEFAULT_UPDATE_SEED, 0, 10).getSeedHash(), DEFAULT_SEED_HASH);
  }

  @Test
  public void checkDefaultSeedHashMatchesCpp() {
    final byte[] bytes = tupleSketch(DEFAULT_UPDATE_SEED, 0, 10).compact().toByteArray();
    assertEquals(seedHashOf(bytes), (short) 37836);
  }

  @Test
  public void checkSeedChangesHashes() {
    assertNotEquals(hashOf(tupleSketch(SEED, 0, 1)), hashOf(tupleSketch(DEFAULT_UPDATE_SEED, 0, 1)));
  }

  @Test
  public void checkHeapifyCompact() {
    final UpdatableTupleSketch<Double, DoubleSummary> sk = tupleSketch(SEED, 0, 100);
    final MemorySegment seg = MemorySegment.ofArray(sk.compact().toByteArray());
    expectThrows(SketchesArgumentException.class, () -> TupleSketch.heapifySketch(seg, deser));
    final TupleSketch<DoubleSummary> sk2 = TupleSketch.heapifySketch(seg, deser, SEED);
    assertEquals(sk2.getEstimate(), sk.getEstimate());
    assertEquals(sk2.getSeedHash(), SEED_HASH);
    //default seed image read with a custom seed is rejected
    final MemorySegment segDef = MemorySegment.ofArray(tupleSketch(DEFAULT_UPDATE_SEED, 0, 100).compact().toByteArray());
    expectThrows(SketchesArgumentException.class, () -> TupleSketch.heapifySketch(segDef, deser, SEED));
  }

  @Test
  public void checkHeapifyEmptyCompact() {
    final byte[] bytes = tupleSketch(SEED, 0, 0).compact().toByteArray();
    assertEquals(seedHashOf(bytes), (short) 0); //empty image carries zero, as in theta
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    assertTrue(TupleSketch.heapifySketch(seg, deser).isEmpty());
    final TupleSketch<DoubleSummary> sk = TupleSketch.heapifySketch(seg, deser, SEED);
    assertTrue(sk.isEmpty());
    assertEquals(sk.getSeedHash(), SEED_HASH);
  }

  @Test
  public void checkUpdatableSerialization() {
    final UpdatableTupleSketch<Double, DoubleSummary> sk = tupleSketch(SEED, 0, 10);
    expectThrows(SketchesStateException.class, () -> sk.toByteArray());
    //the updatable form has no seed hash, so it is read as the default seed
    final byte[] bytes = tupleSketch(DEFAULT_UPDATE_SEED, 0, 10).toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(bytes);
    assertEquals(TupleSketch.heapifySketch(seg, deser).getRetainedEntries(), 10);
    expectThrows(SketchesArgumentException.class, () -> TupleSketch.heapifySketch(seg, deser, SEED));
  }

  @Test
  public void checkUpdatableSerializationComparesFullSeed() {
    //a custom seed whose 16-bit seed hash equals that of the default seed must still be refused
    final short defaultSeedHash = computeSeedHash(DEFAULT_UPDATE_SEED);
    long seed = 1;
    while ((seed == DEFAULT_UPDATE_SEED) || (computeSeedHash(seed) != defaultSeedHash)) { seed++; }
    final UpdatableTupleSketch<Double, DoubleSummary> sk = tupleSketch(seed, 0, 10);
    assertEquals(sk.getSeedHash(), defaultSeedHash);
    expectThrows(SketchesStateException.class, () -> sk.toByteArray());
    expectThrows(SketchesStateException.class, () -> sk.copy().toByteArray());
    expectThrows(SketchesStateException.class,
        () -> new DoubleTupleSketch(12, 3, 1f, DoubleSummary.Mode.Sum, SEED).toByteArray());
    assertEquals(sk.compact().toByteArray().length, tupleSketch(DEFAULT_UPDATE_SEED, 0, 10).compact().toByteArray().length);
  }

  @Test
  public void checkUnion() {
    final TupleUnion<DoubleSummary> union = new TupleUnion<>(4096, dsso, SEED);
    union.union(tupleSketch(SEED, 0, 10));
    union.union(thetaSketch(SEED, 5, 10), factory.newSummary());
    expectThrows(SketchesArgumentException.class, () -> union.union(tupleSketch(DEFAULT_UPDATE_SEED, 0, 10)));
    expectThrows(SketchesArgumentException.class,
        () -> union.union(thetaSketch(DEFAULT_UPDATE_SEED, 0, 10), factory.newSummary()));
    union.union(tupleSketch(DEFAULT_UPDATE_SEED, 0, 0)); //empty is ignored
    final CompactTupleSketch<DoubleSummary> result = union.getResult();
    assertEquals(result.getRetainedEntries(), 15);
    assertEquals(result.getSeedHash(), SEED_HASH);
    //the default union rejects custom seed sketches
    final TupleUnion<DoubleSummary> defUnion = new TupleUnion<>(dsso);
    expectThrows(SketchesArgumentException.class, () -> defUnion.union(tupleSketch(SEED, 0, 10)));
  }

  @Test
  public void checkIntersection() {
    final TupleIntersection<DoubleSummary> inter = new TupleIntersection<>(dsso, SEED);
    inter.intersect(tupleSketch(SEED, 0, 10));
    inter.intersect(thetaSketch(SEED, 5, 10), factory.newSummary());
    expectThrows(SketchesArgumentException.class, () -> inter.intersect(tupleSketch(DEFAULT_UPDATE_SEED, 0, 10)));
    final CompactTupleSketch<DoubleSummary> result = inter.getResult();
    assertEquals(result.getRetainedEntries(), 5);
    assertEquals(result.getSeedHash(), SEED_HASH);
    final TupleIntersection<DoubleSummary> defInter = new TupleIntersection<>(dsso);
    expectThrows(SketchesArgumentException.class, () -> defInter.intersect(tupleSketch(SEED, 0, 10)));
  }

  @Test
  public void checkAnotB() {
    final TupleAnotB<DoubleSummary> anotb = new TupleAnotB<>(SEED);
    anotb.setA(tupleSketch(SEED, 0, 10));
    anotb.notB(tupleSketch(SEED, 0, 2));
    anotb.notB(thetaSketch(SEED, 2, 2));
    expectThrows(SketchesArgumentException.class, () -> anotb.notB(tupleSketch(DEFAULT_UPDATE_SEED, 0, 10)));
    final CompactTupleSketch<DoubleSummary> result = anotb.getResult(true);
    assertEquals(result.getRetainedEntries(), 6);
    assertEquals(result.getSeedHash(), SEED_HASH);
    final TupleAnotB<DoubleSummary> defAnotB = new TupleAnotB<>();
    expectThrows(SketchesArgumentException.class, () -> defAnotB.setA(tupleSketch(SEED, 0, 10)));
  }

  @Test
  public void checkStatelessAnotB() {
    final CompactTupleSketch<DoubleSummary> r1 =
        TupleAnotB.aNotB(tupleSketch(SEED, 0, 10), tupleSketch(SEED, 0, 4), SEED);
    assertEquals(r1.getRetainedEntries(), 6);
    assertEquals(r1.getSeedHash(), SEED_HASH);
    final CompactTupleSketch<DoubleSummary> r2 =
        TupleAnotB.aNotB(tupleSketch(SEED, 0, 10), thetaSketch(SEED, 0, 4), SEED);
    assertEquals(r2.getRetainedEntries(), 6);
    assertEquals(r2.getSeedHash(), SEED_HASH);
    expectThrows(SketchesArgumentException.class,
        () -> TupleAnotB.aNotB(tupleSketch(SEED, 0, 10), tupleSketch(SEED, 0, 4)));
    expectThrows(SketchesArgumentException.class,
        () -> TupleAnotB.aNotB(tupleSketch(SEED, 0, 10), thetaSketch(DEFAULT_UPDATE_SEED, 0, 4), SEED));
  }

  @Test
  public void checkJaccard() {
    final double[] j = JaccardSimilarity.jaccard(tupleSketch(SEED, 0, 10), tupleSketch(SEED, 5, 10), dsso, SEED);
    assertEquals(j[1], 5.0 / 15.0, 1e-9);
    final double[] jt = JaccardSimilarity.jaccard(tupleSketch(SEED, 0, 10), thetaSketch(SEED, 5, 10),
        factory.newSummary(), dsso, SEED);
    assertEquals(jt[1], 5.0 / 15.0, 1e-9);
    assertTrue(JaccardSimilarity.exactlyEqual(tupleSketch(SEED, 0, 10), tupleSketch(SEED, 0, 10), dsso, SEED));
    expectThrows(SketchesArgumentException.class,
        () -> JaccardSimilarity.jaccard(tupleSketch(SEED, 0, 10), tupleSketch(SEED, 5, 10), dsso));
  }

  @Test
  public void checkFilter() {
    final Filter<DoubleSummary> filter = new Filter<>(s -> s.getValue() > 0);
    assertEquals(filter.filter(tupleSketch(SEED, 0, 10)).getSeedHash(), SEED_HASH);
  }

  @Test
  public void checkDerivedSketches() {
    final DoubleTupleSketch dsk = new DoubleTupleSketch(12, ResizeFactor.X8.lg(), 1f, DoubleSummary.Mode.Sum, SEED);
    dsk.update(1, 1.0);
    assertEquals(dsk.compact().getSeedHash(), SEED_HASH);

    final IntegerTupleSketch isk = new IntegerTupleSketch(12, ResizeFactor.X8.lg(), 1f, IntegerSummary.Mode.Sum, SEED);
    isk.update(1, 1);
    assertEquals(isk.compact().getSeedHash(), SEED_HASH);

    final ArrayOfStringsTupleSketch ask = new ArrayOfStringsTupleSketch(12, ResizeFactor.X8, 1f, SEED);
    ask.update(new String[] {"a"}, new String[] {"b"});
    assertEquals(ask.compact().getSeedHash(), SEED_HASH);
    assertEquals(ask.copy().compact().getSeedHash(), SEED_HASH);

    final FdtSketch fsk = new FdtSketch(12, SEED);
    fsk.update(new String[] {"a", "b"});
    assertEquals(fsk.compact().getSeedHash(), SEED_HASH);
    assertEquals(fsk.copy().compact().getSeedHash(), SEED_HASH);

    final ArrayOfStringsTupleSketch askDef = new ArrayOfStringsTupleSketch();
    askDef.update(new String[] {"a"}, new String[] {"b"});
    assertNotEquals(hashOf(ask), hashOf(askDef));
  }

  private static long hashOf(final TupleSketch<?> sk) {
    final TupleSketchIterator<?> it = sk.iterator();
    while (it.next()) { return it.getHash(); }
    return 0;
  }

}
