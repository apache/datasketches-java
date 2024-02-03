/*
 * Copyright 2015 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package filters;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Adapted version of xxHash implementation from https://github.com/Cyan4973/xxHash.
 * This implementation provides endian-independant hash values, but it's slower on big-endian platforms.
 */
class XxHash {
    // Primes if treated as unsigned
    private static final long P1 = -7046029288634856825L;
    private static final long P2 = -4417276706812531889L;
    private static final long P3 = 1609587929392839161L;
    private static final long P4 = -8796714831421723037L;
    private static final long P5 = 2870177450012600261L;

    static long xxHash64(ByteBuffer input, long seed) {
        long hash;
        input.order(ByteOrder.LITTLE_ENDIAN);
        int offset = input.position();
        int remaining = input.remaining();
        int length = remaining;

        if (remaining >= 32) {
            long v1 = seed + P1 + P2;
            long v2 = seed + P2;
            long v3 = seed;
            long v4 = seed - P1;

            do {
                v1 += input.getLong(offset) * P2;
                v1 = Long.rotateLeft(v1, 31);
                v1 *= P1;

                v2 += input.getLong(offset+8) * P2;
                v2 = Long.rotateLeft(v2, 31);
                v2 *= P1;

                v3 += input.getLong(offset+16) * P2;
                v3 = Long.rotateLeft(v3, 31);
                v3 *= P1;

                v4 += input.getLong(offset+24) * P2;
                v4 = Long.rotateLeft(v4, 31);
                v4 *= P1;

                offset += 32;
                remaining -= 32;
            } while (remaining >= 32);

            hash = Long.rotateLeft(v1, 1)
                + Long.rotateLeft(v2, 7)
                + Long.rotateLeft(v3, 12)
                + Long.rotateLeft(v4, 18);

            v1 *= P2;
            v1 = Long.rotateLeft(v1, 31);
            v1 *= P1;
            hash ^= v1;
            hash = hash * P1 + P4;

            v2 *= P2;
            v2 = Long.rotateLeft(v2, 31);
            v2 *= P1;
            hash ^= v2;
            hash = hash * P1 + P4;

            v3 *= P2;
            v3 = Long.rotateLeft(v3, 31);
            v3 *= P1;
            hash ^= v3;
            hash = hash * P1 + P4;

            v4 *= P2;
            v4 = Long.rotateLeft(v4, 31);
            v4 *= P1;
            hash ^= v4;
            hash = hash * P1 + P4;
        } else {
            hash = seed + P5;
        }

        hash += length;

        while (remaining >= 8) {
            long k1 = input.getLong(offset);
            k1 *= P2;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= P1;
            hash ^= k1;
            hash = Long.rotateLeft(hash, 27) * P1 + P4;
            offset += 8;
            remaining -= 8;
        }

        if (remaining >= 4) {
            hash ^= (((long) input.getInt(offset)) & 0xFFFFFFFFL) * P1;
            hash = Long.rotateLeft(hash, 23) * P2 + P3;
            offset += 4;
            remaining -= 4;
        }

        while (remaining != 0) {
            hash ^= (((long) input.get(offset)) & 0xFF) * P5;
            hash = Long.rotateLeft(hash, 11) * P1;
            --remaining;
            ++offset;
        }

        hash ^= hash >>> 33;
        hash *= P2;
        hash ^= hash >>> 29;
        hash *= P3;
        hash ^= hash >>> 32;
        return hash;
    }

    static long xxHash64(long input, long seed){
        long hash;
        hash = seed + P5;
        hash += 8;
        input *= P2;
        input = Long.rotateLeft(input, 31);
        input *= P1;
        hash ^= input;
        hash = Long.rotateLeft(hash, 27) * P1 + P4;
        hash ^= hash >>> 33;
        hash *= P2;
        hash ^= hash >>> 29;
        hash *= P3;
        hash ^= hash >>> 32;
        return hash;
    }
    
}
