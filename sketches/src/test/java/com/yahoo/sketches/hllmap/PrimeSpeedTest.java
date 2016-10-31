/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import java.math.BigInteger;
import java.util.Random;

public class PrimeSpeedTest {
  private static Random rand = new Random(0);

  public void testPrimes() {
    int lowerBound = 1 << 16;
    int upperBound = 1 << 30;
    int trials = 100000;
    long cumTime = 0;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    long maxPrime = Long.MIN_VALUE;
    for (int t=0; t<trials; t++) {
      long tgt = rand.nextInt(upperBound - lowerBound) + lowerBound;

      long startnS = System.nanoTime();
      BigInteger p = BigInteger.valueOf(tgt);
      long prime = p.nextProbablePrime().longValueExact();
      long endnS = System.nanoTime();

      long delta = endnS - startnS;
      maxPrime = (prime > maxPrime) ? prime : maxPrime;
      minTime = (delta < minTime) ? delta : minTime;
      maxTime = (delta > maxTime) ? delta : maxTime;
      cumTime += endnS - startnS;
    }
    double avg = (double)cumTime / trials;
    println("Min uSec : " + minTime/1000.0);
    println("Avg uSec : " + avg/1000.0);
    println("Max uSec : " + maxTime/1000.0);
    println("Max prime: " + maxPrime);
    println("Upr bound: " + upperBound);

  }

  static void println(String s) { System.out.println(s); }

}
