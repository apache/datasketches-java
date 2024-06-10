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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.concurrent.TimeUnit;

import static org.apache.datasketches.hll.TgtHllType.HLL_8;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkUnion {
    private HllSketch sketch;

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(BenchmarkUnion.class.getSimpleName())
                        .forks(1)
                        .build();

        new Runner(opt).run();
    }


    @Setup
    public void setup() {
        sketch = new HllSketch(11, HLL_8);
        for (int i = 0; i < 29197004; i++) {
            sketch.update(i);
        }
    }

    @Benchmark
    public void benchHllUnionHLLToHLLMerge() {
        Union union = new Union(11);
        for (int i = 0; i < 1_000_000; i++) {
            union.update(sketch);
        }
    }

}
