/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

/**
 * Space efficient map of n-byte keys to an approximate unique count of values.
 * Example: number of users per IPv4 address (4 bytes)
 * UniqueCountMap map = new UniqueCountMap(4);
 * for (String userId: listOfUserIds) {
 *   double uniqCountEstimateSoFar = map.update("1234".getBytes(), userId.getBytes());
 * }
 * 
 * @author Kevin Lang 
 * @author Lee Rhodes
 * @author Alexander Saydakov
 */
package com.yahoo.sketches.hllmap;
