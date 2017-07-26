package com.min.Hive;

import org.apache.hadoop.hive.ql.exec.UDF;
/*
 * 定义一个类继承UDF，添加方法
 * evaluate（）参数和返回值和函数的输入输出一致
 * 把项目打成jar包，用hive加载
 * 调用函数：hive新建一个function，指定到输出
 */
public class HiveUDF extends UDF{
	/*
	 * 接收一个被格式化的日期类型的字符串的参数
	 */
public static void evaluate() {
	
}
	
}
