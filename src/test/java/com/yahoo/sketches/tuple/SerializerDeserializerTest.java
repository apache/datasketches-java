/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

public class SerializerDeserializerTest {

  @Test
  public void validSketchType() {
    byte[] bytes = new byte[4];
    bytes[SerializerDeserializer.TYPE_BYTE_OFFSET] = (byte) SerializerDeserializer.SketchType.CompactSketch.ordinal();
    Assert.assertEquals(SerializerDeserializer.getSketchType(Memory.wrap(bytes)), SerializerDeserializer.SketchType.CompactSketch);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void invalidSketchType() {
    byte[] bytes = new byte[4];
    bytes[SerializerDeserializer.TYPE_BYTE_OFFSET] = 33;
    SerializerDeserializer.getSketchType(Memory.wrap(bytes));
  }

//  @Test(expectedExceptions = SketchesArgumentException.class)
//  public void deserializeFromMemoryUsupportedClass() {
//    Memory mem = null;
//    SerializerDeserializer.deserializeFromMemory(mem, 0, "bogus");
//  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void validateFamilyNotTuple() {
    SerializerDeserializer.validateFamily((byte) 1, (byte) 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void validateFamilyWrongPreambleLength() {
    SerializerDeserializer.validateFamily((byte) Family.TUPLE.getID(), (byte) 0);
  }
}
