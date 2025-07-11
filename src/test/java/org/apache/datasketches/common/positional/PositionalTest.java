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

package org.apache.datasketches.common.positional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class PositionalTest {

  @Test
  public void checkInvariants() {
    final Positional posit = Positional.getInstance(100);
    posit.setStartPositionEnd(40, 45, 50);
    posit.setStartPositionEnd(0, 0, 100);
    try {
      posit.setStartPositionEnd(0, 0, 101); fail();
    } catch (final PositionInvariantsException e) { } //OK
    try {
      posit.setPosition(101); fail();
    } catch (final PositionInvariantsException e) { } //OK
    try {
      posit.setStartPositionEnd(50, 45, 40); fail(); //out of order
    } catch (final PositionInvariantsException e) { } //OK
  }

  @Test
  public void checkGetSets() {
    final Positional posit = Positional.getInstance(100);
    posit.setStartPositionEnd(40, 45, 50);
    assertEquals(posit.getStart(), 40);
    assertEquals(posit.getPosition(), 45);
    assertEquals(posit.getEnd(), 50);
    assertEquals(posit.getRemaining(), 5);
    assertTrue(posit.hasRemaining());
    posit.resetPosition();
    assertEquals(posit.getPosition(), 40);
    posit.setPosition(45);
    assertEquals(posit.getPosition(), 45);
    posit.incrementPosition(1);
    assertEquals(posit.getPosition(), 46);
  }

}
