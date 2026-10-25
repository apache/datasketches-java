<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Security Policy

## Reporting a Vulnerability

Please do not report security vulnerabilities through public GitHub
issues, pull requests or mailing lists.

Report them privately to the Apache Security Team at
security@apache.org, or to the Apache DataSketches PMC at
private@datasketches.apache.org. The Apache Security Team forwards
reports to the PMC and coordinates the process described at
https://www.apache.org/security/.

Please include the affected version, a description of the issue and, if
possible, a way to reproduce it (for example, a serialized sketch that
triggers it).

## Supported Versions

The Apache DataSketches PMC supports only the latest release. Security
fixes are made in the next release and are not backported to earlier
releases. Users should upgrade to the latest release to receive fixes.

## Published Vulnerabilities

Published vulnerabilities and their advisories are listed at
https://datasketches.apache.org/docs/Community/Security.html.

## Security Model

### Deserialization

The methods that construct a sketch, union, intersection or filter from
serialized data must be safe to call on arbitrary input, including
input crafted by an attacker. This includes the `heapify()`, `wrap()`,
`writableWrap()`, `getInstance()` and `deserialize()` methods that take
a `MemorySegment`, a byte array or a `String`.

Given a malformed, truncated or corrupt input, these methods must
reject it by throwing a `SketchesException`, usually a
`SketchesArgumentException`. They must never:

- loop forever, or run for a time far out of proportion to the size of
  the input;
- allocate memory based on sizes declared in the input that are
  inconsistent with the rest of the input, or that exceed the documented
  limits of that sketch type (for example, a current size larger than
  the declared maximum size, or larger than the configured K allows);
- return a sketch whose internal state is inconsistent, such that a
  later update, merge, query or serialization of that sketch loops
  forever or fails with an exception other than a `SketchesException`.

A failure to meet this is treated as a security vulnerability.

The limit on allocation applies to memory that the library allocates
while deserializing. A `MemorySegment` that the caller provides to
`wrap()` or `writableWrap()` may be larger than the sketch currently
needs, for example so that a direct sketch can grow in place without a
new allocation. This is supported and is not a violation. The library
must not trust any bytes in such a segment beyond the serialized sketch
image, and must initialize that space before using it.

Because Java checks array and `MemorySegment` bounds, a malformed input
cannot cause an out-of-bounds read or write of memory. An
`IndexOutOfBoundsException` or other unexpected runtime exception thrown
from these methods is still a bug and should be reported, but on its own
it is not treated as a vulnerability.

Applications that deserialize sketches from untrusted sources should
catch `SketchesException`. Note that a well-formed sketch can
legitimately be large. Applications that accept sketches from untrusted
sources should limit the size of the input they accept.

Sketches of generic items (for example `FrequentItemsSketch`,
`KllItemsSketch` and `QuantilesItemsSketch`) deserialize their items
with an `ArrayOfItemsSerDe` supplied by the caller. The serializers
provided by this library are covered by this policy; a serializer
written by the application is the application's responsibility.

### Other APIs

Other APIs, such as sketch construction parameters, updates, merges and
queries, assume a caller that follows the documented preconditions. For
example, sketch sizes must be within the documented limits, and a
sketch must not be used concurrently from multiple threads without
external synchronization. Violating a documented precondition is a bug
in the calling application, not a vulnerability in this library.

### Estimates

Sketches produce approximate results with probabilistic error bounds.
An adversary who controls the input data can bias the estimates, for
example by choosing items that hash to specific values when the hash
seed is known. This is inherent to sketching and is not a vulnerability.
Applications where this matters should use a secret, non-default seed.
