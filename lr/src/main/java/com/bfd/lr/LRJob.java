package com.bfd.lr;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LRJob extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(new LRJob(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}

	public int run(String[] args) throws Exception {
		// 迭代次数 数据维度 学习率 输入文件目录 输出文件目录
		int iter = Integer.valueOf(args[0]);
		int dim = Integer.valueOf(args[1]);
		int count = 0;
		Random random = new Random();
		String params = "";
		for (int i = 0; i < dim; i++) {
			double thea = random.nextDouble();
			if (params.equals("")) {
				params = String.valueOf(thea);
			} else {
				params += " " + String.valueOf(thea);
			}
		}
		Configuration conf = getConf();
		String dir = System.getProperty("user.dir");
		conf.addResource(new FileInputStream(dir + "/conf/core-site.xml"));
		conf.addResource(new FileInputStream(dir + "/conf/hdfs-site.xml"));
		conf.setDouble("learning_rate", Double.valueOf(args[2]));
		FileSystem fs = FileSystem.get(conf);
		try {
			while (count < iter) {
				if (count == 1) {
					conf.set("param", params);
				} else {

					FSDataInputStream in = fs.open(new Path(args[4] + "/part-r-000000"));
					params = in.readLine();
					in.close();
					conf.set("param", params);
					fs.delete(new Path(args[4]), true);
				}
				Job job = Job.getInstance(conf, "LRJob");
				job.setJarByClass(LRMap.class);
				job.setJarByClass(LRReduce.class);
				job.setMapperClass(LRMap.class);
				job.setReducerClass(LRReduce.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setNumReduceTasks(1);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job, new Path(args[3]));
				FileOutputFormat.setOutputPath(job, new Path(args[4]));
				return job.waitForCompletion(true) ? 0 : 1;
			}
			fs.close();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;
	}

}
