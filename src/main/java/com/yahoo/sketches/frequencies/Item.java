/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

/**
 * Defines an Item usable by the FrequentItems sketch.
 */
public interface Item {
  
  long getID();
  
  byte[] toByteArray();
  
  //Item deserializeFromMemory(Memory mem); //static method
  
}
