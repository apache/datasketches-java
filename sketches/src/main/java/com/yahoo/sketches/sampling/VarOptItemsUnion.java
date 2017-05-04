/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

/**
 * @author Jon Malkin
 * @author Kevin Lang
 */
public class VarOptItemsUnion<T> {
  private VarOptItemsSketch<T> gadget_;
  private final int maxK_;

  /**
   * Empty constructor
   *
   * @param maxK Maximum allowed reservoir capacity for this union
   */
  private VarOptItemsUnion(final int maxK) {
    maxK_ = maxK;
  }


}
