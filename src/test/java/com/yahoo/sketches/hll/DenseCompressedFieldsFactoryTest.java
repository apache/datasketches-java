package com.yahoo.sketches.hll;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 */
public class DenseCompressedFieldsFactoryTest
{

  @Test
  public void testMake() throws Exception
  {
    Preamble pre = Preamble.fromLogK(13);
    Fields fields = new DenseCompressedFieldsFactory().make(pre);
    assertEquals(fields.getPreamble(), pre);
    assertEquals(fields.getClass(), OnHeapCompressedFields.class);
  }
}