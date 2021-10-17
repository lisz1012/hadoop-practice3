package com.lisz.hadoop.mapreduce.topn;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// 划定哪些value要被分在同一个
public class TopNGroupingComparator extends WritableComparator {
	public TopNGroupingComparator() {
		super(TopNKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TopNKey ak = (TopNKey) a;
		TopNKey bk = (TopNKey) b;
		if (ak.getYear() == bk.getYear()) {
			return Integer.compare(ak.getMonth(), bk.getMonth());
		} else {
			return Integer.compare(ak.getYear(), bk.getYear());
		}
	}
}
