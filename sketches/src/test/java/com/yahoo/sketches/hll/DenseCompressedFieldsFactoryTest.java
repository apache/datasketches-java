/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

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
