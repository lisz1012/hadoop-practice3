package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNKeyComparator extends WritableComparator {
	public TopNKeyComparator() {
		super(TopNKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TopNKey ak = (TopNKey) a;
		TopNKey bk = (TopNKey) b;
		if (ak.getYear() == bk.getYear()) {
			if (ak.getMonth() == bk.getMonth()) {
				if (ak.getDay() == bk.getDay()) {
					return Integer.compare(bk.getTemperature(), ak.getTemperature());
				} else {
					return Integer.compare(ak.getDay(), bk.getDay());
				}
			} else {
				return Integer.compare(ak.getMonth(), bk.getMonth());
			}
		} else {
			return Integer.compare(ak.getYear(), bk.getYear());
		}
	}
}
