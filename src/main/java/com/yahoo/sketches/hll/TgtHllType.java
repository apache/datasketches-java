/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * Specifies the target type of HLL sketch to be created. It is a target in that the actual
 * allocation of the HLL array is deferred until sufficient number of items have been received.
 * Thus, the memory (and also the serialization) of the sketch during this early warmup phase starts
 * out very small (8 bytes) and then grows as required until the full HLL array is allocated.
 * <ul>
 * <li><b>HLL_8</b> This uses an 8-bit byte per HLL bucket. It is the
 * fastest in terms of update time, but has the largest storage footprint of about
 * <i>K</i> bytes.</li>
 *
 * <li><b>HLL_6</b> This uses a 6-bit field per HLL bucket. It is the next fastest in terms of
 * update time with a storage footprint of about <i>3/4 * K</i> bytes.</li>
 *
 * <li><b>HLL_4</b> This uses a 4-bit field per HLL bucket and uses a small additional array for
 * storing statistical exceptions. For the values of <i>lgConfigK &gt; 13</i> (<i>K</i> = 8192),
 * this additional array adds about 3% to the overall storage. It is the slowest in terms of
 * update time, but has the smallest storage footprint of about <i>K/2 * 1.03</i> bytes.</li>
 * </ul>
 *
 * @author Lee Rhodes
 */
public enum TgtHllType { HLL_4, HLL_6, HLL_8;

  public static final TgtHllType values[] = values();

  public static TgtHllType fromOrdinal(final int typeId) {
    return values[typeId];
  }
}
