/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.CompressionData.decodingTablesForHighEntropyByte;
import static com.yahoo.sketches.cpc.CompressionData.encodingTablesForHighEntropyByte;
import static com.yahoo.sketches.cpc.CompressionData.lengthLimitedUnaryDecodingTable65;
import static com.yahoo.sketches.cpc.CompressionData.lengthLimitedUnaryEncodingTable65;
import static com.yahoo.sketches.cpc.CompressionData.validateDecodingTable;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class CompressionDataTest {

  @Test
  public static void checkTables() {
    validateDecodingTable(lengthLimitedUnaryDecodingTable65, lengthLimitedUnaryEncodingTable65);

    for (int i = 0; i < (16 + 6); i++) {
      validateDecodingTable(decodingTablesForHighEntropyByte[i], encodingTablesForHighEntropyByte[i]);
    }
  }

}
