package com.bfd.distribute.lr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Jama.Matrix;

public class GradientCombiner extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Matrix sum=null;
		for(Text text:values){
			String[] features=text.toString().split(" ");
			int feature_length=features.length;
			double[] feature = new double[feature_length];
			for (int i = 0; i < feature_length; i++) {
				feature[i] = Double.valueOf(features[i]);
			}
			if(sum==null){
				sum= new Matrix(feature, 1);
			}else{
				sum=sum.plus(new Matrix(feature,1));
			}
			
		}
		int columns = sum.getColumnDimension();
		String params = String.valueOf(sum.get(0, 0));
		for (int i = 1; i < columns; i++) {
			params += " " + String.valueOf(sum.get(0, i));
		}
		context.write(key, new Text(params));
		
	}
	

}
