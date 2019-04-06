/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import com.yahoo.sketches.tuple.SummaryFactory;

/**
 * @author Lee Rhodes
 */
public class StringsSummaryFactory implements SummaryFactory<StringsSummary> {

  @Override
  public StringsSummary newSummary() {
    return new StringsSummary();
  }

}
