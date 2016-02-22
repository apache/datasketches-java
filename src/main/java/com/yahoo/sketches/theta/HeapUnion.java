/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapUnion extends UnionImpl {
  
  /**
   * Construct a new Union SetOperation on the java heap. Called by SetOperation.Builder.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   */
  HeapUnion(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    super(HeapQuickSelectSketch.getInstance(lgNomLongs, seed, p, rf, true));
  }
  
  /**
   * Heapify a Union from a Memory object containing data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a> 
   */
  HeapUnion(Memory srcMem, long seed) {
    super(HeapQuickSelectSketch.getInstance(srcMem, seed), srcMem, seed);
  }
  
}
