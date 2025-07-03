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

package org.apache.datasketches.hll;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 *
 */
final class CouponMapping {

  //Computed for Coupon lgK = 26 ONLY. Designed for the cubic interpolator function.
  static final double[] xArr = new double[] {
    0.0, 1.0, 20.0, 400.0,
    8000.0, 160000.0, 300000.0, 600000.0,
    900000.0, 1200000.0, 1500000.0, 1800000.0,
    2100000.0, 2400000.0, 2700000.0, 3000000.0,
    3300000.0, 3600000.0, 3900000.0, 4200000.0,
    4500000.0, 4800000.0, 5100000.0, 5400000.0,
    5700000.0, 6000000.0, 6300000.0, 6600000.0,
    6900000.0, 7200000.0, 7500000.0, 7800000.0,
    8100000.0, 8400000.0, 8700000.0, 9000000.0,
    9300000.0, 9600000.0, 9900000.0, 10200000.0
  };

  // CHECKSTYLE:OFF LineLength
  //Computed for Coupon lgK = 26 ONLY. Designed for the cubic interpolator function.
  static final double[] yArr = new double[] {
    0.0000000000000000, 1.0000000000000000, 20.0000009437402611, 400.0003963713384110,
    8000.1589294602090376, 160063.6067763759638183, 300223.7071597663452849, 600895.5933856170158833,
    902016.8065120954997838, 1203588.4983199508860707, 1505611.8245524743106216, 1808087.9449319066479802,
    2111018.0231759352609515, 2414403.2270142501220107, 2718244.7282051891088486, 3022543.7025524540804327,
    3327301.3299219091422856, 3632518.7942584538832307, 3938197.2836029687896371, 4244337.9901093561202288,
    4550942.1100616492331028, 4858010.8438911894336343, 5165545.3961938973516226, 5473546.9757476449012756,
    5782016.7955296505242586, 6090956.0727340159937739, 6400366.0287892958149314, 6710247.8893762007355690,
    7020602.8844453142955899, 7331432.2482349723577499, 7642737.2192891482263803, 7954519.0404754765331745,
    8266778.9590033423155546, 8579518.2264420464634895, 8892738.0987390466034412, 9206439.8362383283674717,
    9520624.7036988288164139, 9835293.9703129194676876, 10150448.9097250290215015, 10466090.8000503256917000
  };
  // CHECKSTYLE:ON LineLength
}
