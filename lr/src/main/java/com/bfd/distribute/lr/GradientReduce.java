package com.bfd.distribute.lr;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Jama.Matrix;

public class GradientReduce extends Reducer<Text, Text, Text, Text> {
	private TreeMap<Integer,Matrix> result=new TreeMap<Integer,Matrix>();
	private int all_features=0;
	private double[] old_weight=null;
	private double learning_rate=0.01;
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		double[] weight=new double[all_features];
		int flag=0;
		for(Integer key:result.keySet()){
			Matrix sum=result.get(key);
			int columns = sum.getColumnDimension();
			for(int i=0;i<columns;i++){
				weight[flag++]=sum.get(0, i);
			}
		}
		Matrix old_weight_m=new Matrix(old_weight,1);
		Matrix gradient=new Matrix(weight,1);
		Matrix new_weight=old_weight_m.minus(gradient.times(learning_rate));
		String params = String.valueOf(new_weight.get(0, 0));
		for (int i = 1; i < all_features; i++) {
			params += " " + String.valueOf(new_weight.get(0, i));
		}
		context.write(new Text(params),null);
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for(Text text:values){
			String[] features=text.toString().split(" ");
			int feature_length=features.length;
			all_features+=feature_length;
			double[] feature = new double[feature_length];
			for (int i = 0; i < feature_length; i++) {
				feature[i] = Double.valueOf(features[i]);
			}
			int column=Integer.valueOf(key.toString());
			Matrix sum=result.get(column);
			if(sum==null){
				sum= new Matrix(feature, 1);
			}else{
				sum=sum.plus(new Matrix(feature,1));
			}
			result.put(column, sum);
			
		}
		
		
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf=context.getConfiguration();
		String[] weights=conf.get("weights").split(" ");
		int dim=weights.length;
		old_weight=new double[dim];
		for(int i=0;i<dim;i++){
			old_weight[i]=Double.valueOf(weights[i]);
		}
	}
	
	
	

}
