package com.bfd.distribute.lr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InnerProductReduce extends Reducer<Text, DoubleWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		double sum=0.0;
		Iterator<DoubleWritable> iter=values.iterator();
		while(iter.hasNext()){
			double value=iter.next().get();
			sum+=value;
		}
		context.write(key, new Text(String.valueOf(sum)));
	}
	

}
