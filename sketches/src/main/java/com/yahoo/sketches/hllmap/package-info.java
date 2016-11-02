/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

/**
 * Space efficient map of n-byte keys to an approximate unique count of values.
 * Example: number of users per IPv4 address (4 bytes),
 * expected number of unique IP addresses is 100,000,
 * and HLL parameter k=1024
 *
 * UniqueCountMap map = new UniqueCountMap(100_000, 4, 1024);
 * InetAddress inetAddress;
 * String userId;
 * ...
 * double uniqCountEstimateSoFar = map.update(inetAddress.getAddress(), userId.getBytes());
 * 
 * @see UniqueCountMap
 * 
 * @author Kevin Lang 
 * @author Lee Rhodes
 * @author Alexander Saydakov
 */
