/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

/**
 * This is to provide methods of producing unions and intersections of two Summary objects.
 * @param <S> type of Summary
 */
public interface SummarySetOperations<S extends Summary> {

  /**
   * This is called when a union of two sketches is produced, and both sketches have the same key.
   *
   * <p><b>Caution:</b> Do not modify the input Summary objects. Also do not return them directly,
   * unless they are immutable (most Summary objects are not). For mutable Summary objects, it is
   * important to create a new Summary object with the correct contents to be returned. Do not
   * return null summaries.
   *
   * @param a Summary from sketch A
   * @param b Summary from sketch B
   * @return union of Summary A and Summary B
   */
  public S union(S a, S b);

  /**
   * This is called when an intersection of two sketches is produced, and both sketches have the
   * same key.
   *
   * <p><b>Caution:</b> Do not modify the input Summary objects. Also do not return them directly,
   * unless they are immutable (most Summary objects are not). For mutable Summary objects, it is
   * important to create a new Summary object with the correct contents to be returned. Do not
   * return null summaries.
   *
   * @param a Summary from sketch A
   * @param b Summary from sketch B
   * @return intersection of Summary A and Summary B
   */
  public S intersection(S a, S b);

}
