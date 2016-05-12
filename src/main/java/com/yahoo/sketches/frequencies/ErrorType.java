/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

/**
 * Specifies one of two types of error regions of the statistical classification Confusion Matrix 
 * that can be excluded from a returned sample of Frequent Items. 
 */
public enum ErrorType {
  /**
   * No <i>Type I</i> error samples will be included in the sample set, 
   * which means all <i>Truly Negative</i> samples will be excluded from the sample set. 
   * However, there may be <i>Type II</i> error samples (<i>False Negatives</i>) 
   * that should have been included that were not. 
   * This is a subset of the NO_FALSE_NEGATIVES ErrorType.
   */
  NO_FALSE_POSITIVES, 
  /**
   * No <i>Type II</i> error samples will be excluded from the sample set, 
   * which means all <i>Truly Positive</i> samples will be included in the sample set. 
   * However, there may be <i>Type I</i> error samples (<i>False Positives</i>) 
   * that were included that should not have been. 
   */
  NO_FALSE_NEGATIVES}