package com.min.Hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class MaxUDAF extends UDAF {

	public static class MaxValueUDAF implements UDAFEvaluator {

		private IntWritable result;

		public void init() {
			// TODO Auto-generated method stub
			result = null;
		}

		// 接收传入的参数
		// 近似map接收数据
		// 聚合每一行的值就调用一次iterate
		public boolean iterate(IntWritable iWritable) {
			if (iWritable == null) {
				return false;
			}
			if (result == null) {
				result = new IntWritable(iWritable.get());
			} else {
				result.set(Math.max(result.get(), iWritable.get()));
			}
			return true;
		}

		// 近似于聚合，返回当前的results
		public IntWritable terminatePartial() {
			return result;
		}

		// 聚合值调用方法，拿到没有被聚合 的值进行聚合
		public boolean merge(IntWritable iWritable) {
			return iterate(iWritable);
		}

		//返回最终结果
		public IntWritable terminate() {
			return result;
		}
	}
}
