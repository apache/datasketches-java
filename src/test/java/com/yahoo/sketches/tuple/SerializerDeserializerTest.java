package com.yahoo.sketches.tuple;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.yahoo.sketches.Family;

public class SerializerDeserializerTest {

  @Test
  public void validSketchType() {
    byte[] bytes = new byte[4];
    bytes[SerializerDeserializer.TYPE_BYTE_OFFSET] = (byte) SerializerDeserializer.SketchType.CompactSketch.ordinal();
    Assert.assertEquals(SerializerDeserializer.getSketchTypeAbsolute(bytes), SerializerDeserializer.SketchType.CompactSketch);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void invalidSketchType() {
    byte[] bytes = new byte[4];
    bytes[SerializerDeserializer.TYPE_BYTE_OFFSET] = 33;
    SerializerDeserializer.getSketchTypeAbsolute(bytes);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void deserializeFromMemoryUsupportedClass() {
    SerializerDeserializer.deserializeFromMemory(null, 0, "bogus");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void toByteArrayUnsupportedObject() {
    SerializerDeserializer.toByteArray(new Integer(0));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void validateFamilyNotTuple() {
    SerializerDeserializer.validateFamily((byte) 1, (byte) 0); 
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void validateFamilyWrongPreambleLength() {
    SerializerDeserializer.validateFamily((byte) Family.TUPLE.getID(), (byte) 0); 
  }
}
