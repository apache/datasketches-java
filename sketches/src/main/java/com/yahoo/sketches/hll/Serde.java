/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.WritableMemory;

/**
 * Positional putInt.  Puts the value at the current offset and returns the offset for the next int.
 */
public class Serde {
  public static int putInt(final WritableMemory mem, final int offset, final int value) {
    mem.putInt(offset, value);
    return offset + 4;
  }
}
