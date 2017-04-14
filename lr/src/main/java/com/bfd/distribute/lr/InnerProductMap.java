package com.bfd.distribute.lr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Jama.Matrix;

public class InnerProductMap  extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private double[] weight=null;
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] components=value.toString().split("\t");
		//行号\t列分块id\t标签\t开始\t结束\t特征
		String line=components[0];
		int start=Integer.valueOf(components[3]);
		int end=Integer.valueOf(components[4]);
		String[] features=components[5].split(" ");
		int feature_length=features.length;
		double[] feature=new double[feature_length];
		for(int i=0;i<feature_length;i++){
			feature[i]=Double.valueOf(features[i]);
		}
		double[] p_weight=new double[feature_length];
		for(int i=start;i<end;i++){
			p_weight[i-start]=weight[i];
		}
		Matrix w_matrix=new Matrix(p_weight,1);
		Matrix f_matrix=new Matrix(feature,1);
		double p_inner_product=w_matrix.transpose().times(f_matrix).get(0, 0);
		context.write(new Text(line),new DoubleWritable(p_inner_product));
		
		
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		//获取所有权重。
		Configuration conf=context.getConfiguration();
		String[] weights=conf.get("weights").split(" ");
		int dim=weights.length;
		weight=new double[dim];
		for(int i=0;i<dim;i++){
			weight[i]=Double.valueOf(weights[i]);
		}
		
		
	}
	

}
