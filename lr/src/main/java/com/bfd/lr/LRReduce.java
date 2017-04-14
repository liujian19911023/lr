package com.bfd.lr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Jama.Matrix;

public class LRReduce extends Reducer<Text, Text, Text, Text> {
	private double learning_rate = 0.01;
	private int count = 0;
	private Matrix sum = null;
	private Matrix thea=null;
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		sum=sum.times(learning_rate/count);
		thea=thea.minus(sum);
		int columns = sum.getColumnDimension();
		String params = String.valueOf(sum.get(0, 0));
		for (int i = 1; i < columns; i++) {
			params += " " + String.valueOf(sum.get(0, i));
		}
		context.write(new Text(params), null);
		super.cleanup(context);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> datas = values.iterator();
		while (datas.hasNext()) {
			Text data = datas.next();
			String[] count_params = data.toString().split("\t");
			count += Integer.valueOf(count_params[0]);
			String[] params = count_params[1].split(" ");
			int length = params.length;
			double[] param = new double[length];
			for (int i = 0; i < length; i++) {
				param[i] = Double.valueOf(params[i]);
			}
			if (sum == null) {
				sum = new Matrix(param, 1);
			} else {
				sum = sum.plus(new Matrix(param, 1));
			}
		}
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		learning_rate = conf.getDouble("learning_rate", 0.01);
		String param = conf.get("param", "");
		String[] datas = param.split(" ");
		double[] fields = new double[datas.length];
		for (int i = 0; i < datas.length; i++) {
			fields[i] = Double.valueOf(datas[i]);
		}
		thea = new Matrix(fields, 1);
	}

}
