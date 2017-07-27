package com.min.Hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class AvgUDAF1 extends UDAF {

	// static List list;
	private static int sum;
	private static int count;

	public static class AvgEvaluator1 implements UDAFEvaluator {
		public void init() {
			sum = 0;
			count = 0;
		}

		public boolean iterate(IntWritable d) {
			if (d != null) {
				sum += Integer.valueOf(d.toString());
				count++;
				return true;
			}
			return false;
		}

		public Text terminatePartial() {
			return new Text(sum + " " + count);
		}

		public boolean merge(Text d) {
			String[] data = d.toString().split(" ");
			sum += Integer.valueOf(data[0]);
			count += Integer.valueOf(data[1]);
			return true;
		}

		public IntWritable terminate() {
			return new IntWritable(sum / count);
		}
	}
}
