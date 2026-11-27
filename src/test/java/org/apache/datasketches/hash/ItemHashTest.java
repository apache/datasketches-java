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

package org.apache.datasketches.hash;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdateReturnState;
import org.apache.datasketches.tuple.UpdatableTupleSketch;
import org.apache.datasketches.tuple.UpdatableTupleSketchBuilder;
import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.testng.annotations.Test;

public class ItemHashTest {
  private static final long SEED = 12345L;
  private static final int N = 20_000;

  @Test
  public void singleLongMatchesOneElementArray() {
    final long[] samples = {0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x0123_4567_89ab_cdefL};
    for (final long x : samples) {
      assertEquals(MurmurHash3.hash(x, SEED), MurmurHash3.hash(new long[] {x}, SEED));
    }
  }

  @Test
  public void canonicalizesDoublesAndSkipsEmptyItems() {
    assertEquals(ItemHash.of(-0.0, SEED).h0(), ItemHash.of(0.0, SEED).h0());
    assertEquals(ItemHash.of(Double.longBitsToDouble(0x7ff8_0000_0000_0001L), SEED).h0(), ItemHash.of(Double.NaN, SEED).h0());
    assertNull(ItemHash.of((String) null, SEED));
    assertNull(ItemHash.of("", SEED));
    assertNull(ItemHash.of(new byte[0], SEED));
    assertNull(ItemHash.of(new char[0], SEED));
    assertNull(ItemHash.of(new int[0], SEED));
    assertNull(ItemHash.of(new long[0], SEED));
    assertNull(ItemHash.of(ByteBuffer.allocate(0), SEED));
    assertNull(ItemHash.of(MemorySegment.ofArray(new byte[0]), SEED));
    assertEquals(ItemHash.of(MemorySegment.ofArray("abc".getBytes(UTF_8)), SEED).h0(), ItemHash.of("abc", SEED).h0());
  }

  @Test
  public void thetaMatchesItemUpdates() {
    final UpdatableThetaSketch byItem = UpdatableThetaSketch.builder().setNominalEntries(1024).setSeed(SEED).build();
    final UpdatableThetaSketch byHash = UpdatableThetaSketch.builder().setNominalEntries(1024).setSeed(SEED).build();
    feed(byItem::update, byItem::update, byItem::update, byItem::update, byItem::update, byItem::update, byItem::update,
        byItem::update);
    feedHashes(SEED, byHash::update);
    assertEquals(byHash.compact().toByteArray(), byItem.compact().toByteArray());
    assertEquals(byHash.update((ItemHash) null), UpdateReturnState.RejectedNullOrEmpty);
  }

  @Test
  public void tupleMatchesItemUpdates() {
    final UpdatableTupleSketchBuilder<Double, DoubleSummary> builder =
        new UpdatableTupleSketchBuilder<Double, DoubleSummary>(new DoubleSummaryFactory(DoubleSummary.Mode.Sum))
            .setNominalEntries(1024).setSeed(SEED);
    final UpdatableTupleSketch<Double, DoubleSummary> byItem = builder.build();
    final UpdatableTupleSketch<Double, DoubleSummary> byHash = builder.build();
    feed(v -> byItem.update(v, 1.0), v -> byItem.update(v, 1.0), v -> byItem.update(v, 1.0), v -> byItem.update(v, 1.0),
        v -> byItem.update(v, 1.0), null, v -> byItem.update(v, 1.0), v -> byItem.update(v, 1.0));
    feedHashesNoChars(SEED, h -> byHash.update(h, 1.0));
    assertEquals(byHash.compact().toByteArray(), byItem.compact().toByteArray());
  }

  @Test
  public void arrayOfDoublesMatchesItemUpdates() {
    final ArrayOfDoublesUpdatableSketchBuilder builder =
        new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(1024).setSeed(SEED);
    final ArrayOfDoublesUpdatableSketch byItem = builder.build();
    final ArrayOfDoublesUpdatableSketch byHash = builder.build();
    final double[] one = {1.0};
    feed(v -> byItem.update(v, one), v -> byItem.update(v, one), v -> byItem.update(v, one), v -> byItem.update(v, one),
        v -> byItem.update(v, one), null, v -> byItem.update(v, one), v -> byItem.update(v, one));
    feedHashesNoChars(SEED, h -> byHash.update(h, one));
    assertEquals(byHash.compact().toByteArray(), byItem.compact().toByteArray());
  }

  @Test
  public void cpcMatchesItemUpdates() {
    final CpcSketch byItem = new CpcSketch(11, SEED);
    final CpcSketch byHash = new CpcSketch(11, SEED);
    feed(byItem::update, byItem::update, byItem::update, byItem::update, byItem::update, byItem::update, byItem::update,
        byItem::update);
    feedHashes(SEED, byHash::update);
    assertEquals(byHash.toByteArray(), byItem.toByteArray());
  }

  @Test
  public void hllMatchesItemUpdates() {
    final HllSketch byItem = new HllSketch(11);
    final HllSketch byHash = new HllSketch(11);
    feed(byItem::update, byItem::update, byItem::update, byItem::update, byItem::update, byItem::update, byItem::update,
        byItem::update);
    feedHashes(Util.DEFAULT_UPDATE_SEED, byHash::update);
    assertEquals(byHash.toCompactByteArray(), byItem.toCompactByteArray());
  }

  @Test
  public void oneHashFeedsEverySketch() {
    final long seed = Util.DEFAULT_UPDATE_SEED;
    final UpdatableThetaSketch theta = UpdatableThetaSketch.builder().build();
    final CpcSketch cpc = new CpcSketch(11);
    final HllSketch hll = new HllSketch(11);
    final UpdatableThetaSketch thetaRef = UpdatableThetaSketch.builder().build();
    final CpcSketch cpcRef = new CpcSketch(11);
    final HllSketch hllRef = new HllSketch(11);
    for (int i = 0; i < N; i++) {
      final String id = "user-" + i;
      final ItemHash h = ItemHash.of(id, seed);
      theta.update(h);
      cpc.update(h);
      hll.update(h);
      thetaRef.update(id);
      cpcRef.update(id);
      hllRef.update(id);
    }
    assertEquals(theta.compact().toByteArray(), thetaRef.compact().toByteArray());
    assertEquals(cpc.toByteArray(), cpcRef.toByteArray());
    assertEquals(hll.toCompactByteArray(), hllRef.toCompactByteArray());
  }

  @Test
  public void rejectsHashFromAnotherSeed() {
    final ItemHash h = ItemHash.of(1L, SEED + 1);
    assertThrows(SketchesArgumentException.class, () -> UpdatableThetaSketch.builder().setSeed(SEED).build().update(h));
    assertThrows(SketchesArgumentException.class, () -> new CpcSketch(11, SEED).update(h));
    assertThrows(SketchesArgumentException.class, () -> new HllSketch(11).update(h));
    assertThrows(SketchesArgumentException.class, () -> new ArrayOfDoublesUpdatableSketchBuilder().setSeed(SEED).build()
        .update(h, new double[] {1.0}));
  }

  private static void feed(final Consumer<Long> longs, final Consumer<Double> doubles, final Consumer<String> strings,
      final Consumer<byte[]> bytes, final Consumer<ByteBuffer> buffers, final Consumer<char[]> chars,
      final Consumer<int[]> ints, final Consumer<long[]> longArrays) {
    for (int i = 0; i < N; i++) {
      switch (i % 8) {
        case 0: longs.accept((long) i); break;
        case 1: doubles.accept(i % 16 == 1 ? -0.0 : i + 0.5); break;
        case 2: strings.accept(i % 16 == 2 ? "" : "s" + i); break;
        case 3: bytes.accept(i % 16 == 3 ? null : ("b" + i).getBytes(UTF_8)); break;
        case 4: buffers.accept(ByteBuffer.wrap(("y" + i).getBytes(UTF_8))); break;
        case 5: if (chars != null) { chars.accept(("c" + i).toCharArray()); } break;
        case 6: ints.accept(new int[] {i, -i}); break;
        default: longArrays.accept(new long[] {i, i * 31L}); break;
      }
    }
  }

  private static void feedHashes(final long seed, final Consumer<ItemHash> sink) {
    feed(v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)),
        v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)),
        v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)),
        v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)));
  }

  private static void feedHashesNoChars(final long seed, final Consumer<ItemHash> sink) {
    feed(v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)),
        v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)),
        v -> sink.accept(ItemHash.of(v, seed)), null,
        v -> sink.accept(ItemHash.of(v, seed)), v -> sink.accept(ItemHash.of(v, seed)));
  }
}
