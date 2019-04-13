/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import static com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary.stringArrHash;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.tuple.UpdatableSketch;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSketch extends UpdatableSketch<String[], ArrayOfStringsSummary> {

  /**
   * Constructs new sketch with default <i>K</i> = 4096 (<i>lgK</i> = 12), default ResizeFactor=X8,
   * and default <i>p</i> = 1.0.
   */
  public ArrayOfStringsSketch() {
    this(12);
  }

  /**
   * Constructs new sketch with default ResizeFactor=X8, default <i>p</i> = 1.0 and given <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   */
  public ArrayOfStringsSketch(final int lgK) {
    this(lgK, ResizeFactor.X8, 1.0F);
  }

  /**
   * Constructs new sketch with given ResizeFactor, <i>p</i> and <i>lgK</i>.
   * @param lgK Log_base2 of <i>Nominal Entries</i>.
   * <a href="{@docRoot}/resources/dictionary.html#nomEntries">See Nominal Entries</a>
   * @param rf ResizeFactor
   * <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param p sampling probability
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability</a>
   */
  public ArrayOfStringsSketch(final int lgK, final ResizeFactor rf, final float p) {
    super(1 << lgK, rf.lg(), p, new ArrayOfStringsSummaryFactory());
  }

  /**
   * Constructs this sketch from a Memory image, which must be from an ArrayOfStringsSketch, and
   * usually with data.
   * @param mem the given Memory
   */
  public ArrayOfStringsSketch(final Memory mem) {
    super(mem, new ArrayOfStringsSummaryDeserializer(), new ArrayOfStringsSummaryFactory());
  }

  /**
   * Updates the sketch with String arrays for both key and value.
   * @param strArrKey the given String array key
   * @param strArr the given String array value
   */
  public void update(final String[] strArrKey, final String[] strArr) {
    super.update(stringArrHash(strArrKey), strArr);
  }

}
