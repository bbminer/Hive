package com.min.Hive;

import org.apache.hadoop.hive.ql.exec.UDF;

//UDF:一对一
//对一张表的某一列进行操作
public class Add extends UDF {
	public Integer evaluate(Integer... integers) {
		if (integers.length > 0) {
			Integer sum = 0;
			for (Integer integer : integers) {
				if (integer != null) {
					sum += integer;
				}
			}
			return sum;
		}
		return null;
	}
	
	public Integer evaluate(Integer a,Integer b,Integer c,Integer d) {
		return a+b+c+d;
	}
}
