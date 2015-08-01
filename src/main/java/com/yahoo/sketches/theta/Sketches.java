/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.memory.Memory;

/**
 * This class brings together the common sketch and set operation creation methods 
 * into one place.  
 */
public class Sketches {
  
  /**
   * Ref: {@link UpdateSketch#builder() UpdateSketch.builder()}
   * @return {@link UpdateSketch.Builder UpdateSketch.Builder}
   */
  public static UpdateSketch.Builder getUpdateSketchBuilder() {
    return UpdateSketch.builder();
  }
  
  /**
   * Ref: {@link Sketch#heapify(Memory) Sketch.heapify(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(Memory srcMem) {
    return Sketch.heapify(srcMem);
  }
  
  /**
   * Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(Memory srcMem, long seed) {
    return Sketch.heapify(srcMem, seed);
  }
  
  /**
   * Ref: {@link Sketch#wrap(Memory) Sketch.heapify(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(Memory srcMem) {
    return Sketch.wrap(srcMem);
  }
  
  /**
   * Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(Memory srcMem, long seed) {
    return Sketch.wrap(srcMem, seed);
  }
  
  /**
   * Ref: {@link SetOperation#builder() SetOperation.builder()}
   * @return {@link SetOperation.Builder SetOperation.Builder}
   */
  public static SetOperation.Builder getSetOpBuilder() {
    return SetOperation.builder();
  }
  
  /**
   * Ref: {@link SetOperation#heapify(Memory) SetOperation.heapify(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOp(Memory srcMem) {
    return SetOperation.heapify(srcMem);
  }
  
  /**
   * Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOp(Memory srcMem, long seed) {
    return SetOperation.heapify(srcMem, seed);
  }
  
  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(Memory srcMem) {
    return SetOperation.wrap(srcMem);
  }
  
  /**
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(Memory srcMem, long seed) {
    return SetOperation.wrap(srcMem, seed);
  }
  
}