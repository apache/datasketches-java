/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class ModeTest {

  @Test
  public void checkGoodModeId() {
    Mode mode = Mode.EMPTY;
    mode.checkModeID((byte) 0);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadModeId() {
    Mode mode = Mode.EMPTY;
    mode.checkModeID((byte) 2);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadIdToMode() {
    Mode.idToMode((byte) 10);
  }

  @Test
  public void checkGoodStringToMode() {
    Mode mode = Mode.stringToMode("empty");
    assertEquals(mode, Mode.EMPTY);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkBadStringToMode() {
    Mode.stringToMode("abc");
  }


}
