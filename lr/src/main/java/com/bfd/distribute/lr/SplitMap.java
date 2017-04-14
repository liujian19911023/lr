package com.bfd.distribute.lr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitMap  extends Mapper<LongWritable, Text, Text, Text> {
	private int column_split_dim;
	
	private String arrayToString(String[] datas,int start,int end){
		StringBuilder ret=new StringBuilder();
		ret.append(datas[start]);
		for(int i=start+1;i<end;i++){
			ret.append(" ").append(datas[i]);
		}
		return ret.toString();
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String data=value.toString();
		String[] label_features=data.split("\t");
		String[] features=label_features[1].split(" ");
		int splits=(int)Math.ceil(features.length/Double.valueOf(column_split_dim));
		for(int i=0;i<splits;i++){
			int start=i*column_split_dim;
			int end=(i+1)*column_split_dim;
			if(i==splits-1){
				end=features.length-1;
			}
			//行号\t列分块id\t标签\t开始\t结束\t特征
			context.write(new Text(key.toString()),new Text(i+"\t"+start+"\t"+end+"\t"+arrayToString(features,start,end)));
		}
		
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		// 获取每个列分块的维度数，以及数据总维度
		Configuration conf = context.getConfiguration();
		column_split_dim = conf.getInt("column_split_dim", 100);

	}

}
