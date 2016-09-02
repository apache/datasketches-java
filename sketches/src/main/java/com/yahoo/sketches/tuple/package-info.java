/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

/**
 * The tuple package contains implementation of sketches based on the idea of
 * theta sketches with the addition of values associated with unique keys.
 * Two sets of tuple sketch classes are available at the moment:
 * generic tuple sketches with user-defined Summary, and a faster specialized
 * implementation with an array of double values.
 * See unit tests for usage examples.
 *
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.tuple;
