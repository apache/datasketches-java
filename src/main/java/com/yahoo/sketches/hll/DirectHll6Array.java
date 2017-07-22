/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectHll6Array extends DirectHllArray {

  DirectHll6Array(final WritableMemory wmem) {
    super(wmem);
  }

  DirectHll6Array(final Memory mem) {
    super(mem);
  }

}
