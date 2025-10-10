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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * Empty essentially means that the sketch has never seen data.
 *
 * @author Lee Rhodes
 */
public class EmptyTest {

  @Test
  public void checkEmpty() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch sk2 = UpdatableThetaSketch.builder().build();
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();

    final int u = 100;
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.intersect(sk1);
    inter.intersect(sk2);

    final CompactThetaSketch csk = inter.getResult();
    //The intersection of two disjoint, exact-mode sketches is empty, T == 1.0.
    println(csk.toString());
    assertTrue(csk.isEmpty());

    final ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk2 = aNotB.aNotB(csk, sk1);
    //The ThetaAnotB of an empty, T == 1.0 sketch with another exact-mode sketch is empty, T == 1.0
    assertTrue(csk2.isEmpty());
  }

  @Test
  public void checkNotEmpty() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().build();
    final UpdatableThetaSketch sk2 = UpdatableThetaSketch.builder().build();
    final ThetaIntersection inter = ThetaSetOperation.builder().buildIntersection();

    final int u = 10000; //estimating
    for (int i = 0; i < u; i++) { //disjoint
      sk1.update(i);
      sk2.update(i + u);
    }
    inter.intersect(sk1);
    inter.intersect(sk2);

    final CompactThetaSketch csk = inter.getResult();
    println(csk.toString());
    //The intersection of two disjoint, est-mode sketches is not-empty, T < 1.0.
    assertFalse(csk.isEmpty());

    ThetaAnotB aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk2 = aNotB.aNotB(csk, sk1); //empty, T < 1.0; with est-mode sketch
    println(csk2.toString());
    //The ThetaAnotB of an empty, T < 1.0 sketch with another exact-mode sketch is not-empty.
    assertFalse(csk2.isEmpty());

    final UpdatableThetaSketch sk3 = UpdatableThetaSketch.builder().build();
    aNotB = ThetaSetOperation.builder().buildANotB();
    final CompactThetaSketch csk3 = aNotB.aNotB(sk3, sk1); //empty, T == 1.0; with est-mode sketch
    println(csk3.toString());
    //the ThetaAnotB of an empty, T == 1.0 sketch with another est-mode sketch is empty, T < 1.0
    assertTrue(csk3.isEmpty());
  }

  @Test
  public void checkPsampling() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().setP(.5F).build();
    assertTrue(sk1.isEmpty());
    //An empty P-sampling sketch where T < 1.0 and has never seen data is also empty
    // and will have a full preamble of 24 bytes.  But when compacted, theta returns to 1.0, so
    // it will be stored as only 8 bytes.
    assertEquals(sk1.compact().toByteArray().length, 8);
  }

  @Test
  public void checkEmptyToCompact() {
    final UpdatableThetaSketch sk1 = UpdatableThetaSketch.builder().build();
    final CompactThetaSketch csk = sk1.compact();
    assertTrue(csk instanceof EmptyCompactSketch);
    final CompactThetaSketch csk2 = csk.compact();
    assertTrue(csk2 instanceof EmptyCompactSketch);
    final CompactThetaSketch csk3 = csk.compact(true, MemorySegment.ofArray(new byte[8]));
    assertTrue(csk3 instanceof DirectCompactSketch);
    assertEquals(csk2.getCurrentPreambleLongs(), 1);
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
