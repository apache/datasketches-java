/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.PreambleUtil.FAMILY_ID;
import static com.yahoo.sketches.cpc.PreambleUtil.getFamilyId;
import static com.yahoo.sketches.cpc.PreambleUtil.getLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.getPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.getSerVer;
import static com.yahoo.sketches.cpc.PreambleUtil.putFamilyId;
import static com.yahoo.sketches.cpc.PreambleUtil.putLgK;
import static com.yahoo.sketches.cpc.PreambleUtil.putPreInts;
import static com.yahoo.sketches.cpc.PreambleUtil.putSerVer;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class PreambleUtilTest {

  @Test
  public void checkFirst8() {
    WritableMemory wmem = WritableMemory.allocate(36);
    int in = 0x1FF;
    putPreInts(wmem, in);
    assertEquals(getPreInts(wmem), 0xFF);

    putSerVer(wmem);
    assertEquals(getSerVer(wmem), 0x1);

    putFamilyId(wmem);
    assertEquals(getFamilyId(wmem), FAMILY_ID);

    putLgK(wmem, in);
    assertEquals(getLgK(wmem), 0xFF);

  }

}
