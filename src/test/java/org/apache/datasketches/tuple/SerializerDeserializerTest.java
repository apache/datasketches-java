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

import static org.apache.datasketches.common.Util.computeSeedHash;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple.SerializerDeserializer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializerDeserializerTest {

  @Test
  public void validSketchType() {
    byte[] bytes = new byte[4];
    bytes[SerializerDeserializer.TYPE_BYTE_OFFSET] = (byte) SerializerDeserializer.SketchType.CompactSketch.ordinal();
    Assert.assertEquals(SerializerDeserializer.getSketchType(MemorySegment.ofArray(bytes)), SerializerDeserializer.SketchType.CompactSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidSketchType() {
    byte[] bytes = new byte[4];
    bytes[SerializerDeserializer.TYPE_BYTE_OFFSET] = 33;
    SerializerDeserializer.getSketchType(MemorySegment.ofArray(bytes));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void validateFamilyNotTuple() {
    SerializerDeserializer.validateFamily((byte) 1, (byte) 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void validateFamilyWrongPreambleLength() {
    SerializerDeserializer.validateFamily((byte) Family.TUPLE.getID(), (byte) 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadSeedHash() {
    computeSeedHash(50541);
  }
}
