package com.min.Hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public class AvgUDAF extends UDAF {
	public static class State {
		private double sum;
		private int count;

	}

	public static class AvgValueUDAF implements UDAFEvaluator {

		private State state = new State();

		public void init() {
			// TODO Auto-generated method stub
			state.count = 0;
			state.sum = 0;
		}

		public boolean iterate(Double value) {
			if (value != null) {
				state.sum += value;
				state.count++;
			}
			return true;
		}

		public State terminatePartial() {
			return state.count == 0 ? null : state;
		}

		public boolean merge(State s) {
			if (s != null) {
				state.count += s.count;
				state.sum += s.sum;
			}
			return true;
		}

		// 返回最终结果
		public Double terminate() {
			return state.count == 0 ? null : Double.valueOf(state.sum / state.count);
		}
	}
}
