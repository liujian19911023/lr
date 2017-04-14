package com.bfd.distribute.lr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Jama.Matrix;

public class GradientMap extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 行号\t列分块id\t标签\t开始\t结束\t内积\t特征
		String[] components = value.toString().split("\t");
		String column = components[1];
		int label = Integer.valueOf(components[2]);
		double innner_product = Double.valueOf(components[5]);
		String[] features = components[6].split(" ");
		int feature_length = features.length;
		double[] feature = new double[feature_length];
		for (int i = 0; i < feature_length; i++) {
			feature[i] = Double.valueOf(features[i]);
		}
		Matrix f_matrix = new Matrix(feature, 1);
		double sigmoid = sigmoid(innner_product);
		Matrix gradient = f_matrix.times((sigmoid - label));
		int columns = gradient.getColumnDimension();
		String params = String.valueOf(gradient.get(0, 0));
		for (int i = 1; i < columns; i++) {
			params += " " + String.valueOf(gradient.get(0, i));
		}
		context.write(new Text(column), new Text(params));

	}

	private double sigmoid(double inner_product) {
		return 1.0 / Math.pow(Math.E, -inner_product);
	}

}
