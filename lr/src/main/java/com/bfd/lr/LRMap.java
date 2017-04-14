package com.bfd.lr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Jama.Matrix;

public class LRMap extends Mapper<LongWritable, Text, Text, Text> {
	private Matrix thea = null;
	private int count = 0;
	private Matrix sum = null;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String param = conf.get("param", "");
		String[] datas = param.split(" ");
		double[] fields = new double[datas.length];
		for (int i = 0; i < datas.length; i++) {
			fields[i] = Double.valueOf(datas[i]);
		}
		thea = new Matrix(fields, 1);
	}

	private double sigmoid(Matrix param, Matrix data) {
		return 1.0/Math.pow(Math.E, param.times(data.transpose()).get(0, 0));
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] label_fields = value.toString().split("\t");
		int label = Integer.valueOf(label_fields[0]);
		String[] datas = label_fields[1].split(" ");
		int length = datas.length;
		// 随机参数
		double[] data = new double[length];
		for (int i = 0; i < length; i++) {
			data[i] = Double.valueOf(datas[i]);
		}
		Matrix x = new Matrix(data, 1);
		if (sum == null) {
			sum = x.times(sigmoid(thea, x) - label);
		} else {
			sum = sum.plus(x.times(sigmoid(thea, x) - label));
		}
		count += 1;
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 输出矩阵和数量
		int columns = sum.getColumnDimension();
		String params = String.valueOf(sum.get(0, 0));
		for (int i = 1; i < columns; i++) {
			params += " " + String.valueOf(sum.get(0, i));
		}
		context.write(new Text("result"), new Text(count + "\t" + params));
		super.cleanup(context);
	}

}
