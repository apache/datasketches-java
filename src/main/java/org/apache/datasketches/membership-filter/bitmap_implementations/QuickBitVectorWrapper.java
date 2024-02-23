/*
 * Copyright 2014 Niv Dayan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package bitmap_implementations;

import java.util.Arrays;

public class QuickBitVectorWrapper extends Bitmap implements Cloneable {

	long[] bs;
	
	public QuickBitVectorWrapper(int bits_per_entry, long num_entries) {
		bs = QuickBitVector.makeBitVector(num_entries, bits_per_entry);
	}
	
	@Override
	public Object clone() {
	    QuickBitVectorWrapper qv = (QuickBitVectorWrapper) super.clone();
		qv.bs = Arrays.copyOf(bs, bs.length);
		return qv;
	}

	@Override
	public long size() {
		return (long)bs.length * Long.BYTES * 8L;
	}

	@Override
	public void set(long bit_index, boolean value) {
		if (value) {
			QuickBitVector.set(bs, bit_index);
		}
		else {
			QuickBitVector.clear(bs, bit_index);
		}
	}

	@Override
	public void setFromTo(long from, long to, long value) {
		QuickBitVector.putLongFromTo(bs, value, from, to - 1);
	}

	@Override
	public boolean get(long bit_index) {
		return QuickBitVector.get(bs, bit_index);
	}

	@Override
	public long getFromTo(long from, long to) {
		return QuickBitVector.getLongFromTo(bs, from, to - 1);
	}
	

}
