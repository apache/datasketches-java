/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.frequencies;

import org.testng.Assert;
//import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class DistTest {

  /**
   * @param prob the probability of success for the geometric distribution.
   * @return a random number generated from the geometric distribution.
   */
  public static long randomGeometricDist(double prob) {
    assert ((prob > 0.0) && (prob < 1.0));
    return 1 + (long) (Math.log(Math.random()) / Math.log(1.0 - prob));
  }

  public static double zeta(long n, double theta) {
    // the zeta function, used by the below zipf function
    // (this is not often called from outside this library)
    // ... but have made it public now to speed things up
    int i;
    double ans = 0.0;

    for (i = 1; i <= n; i++) {
      ans += Math.pow(1. / i, theta);
    }
    return (ans);
  }

  // This draws values from the zipf distribution
  // n is range, theta is skewness parameter
  // theta = 0 gives uniform distribution,
  // theta > 1 gives highly skewed distribution.
  public static long zipf(double theta, long n, double zetan) {
    double alpha;
    double eta;
    double u;
    double uz;
    double val;

    // randinit must be called before entering this procedure for
    // the first time since it uses the random generators

    alpha = 1. / (1. - theta);
    eta = (1. - Math.pow(2. / n, 1. - theta)) / (1. - (zeta(2, theta) / zetan));

    u = 0.0;
    while (u == 0.0) {
      u = Math.random();
    }
    uz = u * zetan;
    if (uz < 1.) {
      val = 1;
    } else if (uz < (1. + Math.pow(0.5, theta))) {
      val = 2;
    } else {
      val = 1 + (n * Math.pow(((eta * u) - eta) + 1., alpha));
    }

    return (long) val;
  }

  //@Test
  public static void testRandomGeometricDist() {
    long maxItem = 0L;
    double prob = .1;
    for (int i = 0; i < 100; i++) {
      long item = randomGeometricDist(prob);
      if (item > maxItem) {
        maxItem = item;
      }
      // If you succeed with probability p the probability
      // of failing 20/p times is smaller than 1/2^20.
      Assert.assertTrue(maxItem < (20.0 / prob));
    }
  }

}
