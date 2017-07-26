package com.min.Hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;

/*
 * 定义一个类继承UDF，添加方法
 * evaluate（）参数和返回值和函数的输入输出一致
 * 把项目打成jar包，用hive加载
 * 调用函数：hive新建一个function，指定到输出
 */
public class HiveUDF extends UDF {
	/*
	 * 接收一个被格式化的日期类型的字符串的参数 在方法里将它提取出来，更改格式
	 */
	public static String evaluate(String date) {
		SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy hh:mm:ss Z",Locale.US);
		if (date.indexOf("[") > -1) {
			date = date.replace("[", "");
		}
		if (date.indexOf("]") > -1) {
			date = date.replace("]", "");
		}
		try {
			
			Date parse = format.parse(date);
			return new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(parse);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
