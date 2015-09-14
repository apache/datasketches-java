/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

/**
 * Used with the MemoryRequest to provide linkage between the Memory being freed and the newly
 * allocated memory that replaced it.
 * 
 * @author Lee Rhodes
 */
public class MemoryFreeAndLink {
  public Memory oldMemory;
  public Memory newMemory;
}
