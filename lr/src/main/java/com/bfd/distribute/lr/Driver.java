package com.bfd.distribute.lr;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bfd.lr.LRJob;
import com.bfd.lr.LRMap;

public class Driver extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(new LRJob(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}

	private static Connection getHiveConn(Properties prop) throws ClassNotFoundException, SQLException {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection(prop.getProperty("hive.jdbc.url"),
				prop.getProperty("hive.jdbc.username"), prop.getProperty("hive.jdbc.password"));
		return con;
	}

	public int run(String[] args) throws Exception {
		// 迭代次数 数据维度 学习率 多少列独立计算 输入文件目录 拆分后文件目录 内积计算结果目录 join后的表的目录 权重目录
		/**
		 * iter.count= data.dim= learning_rate= column.split.column=
		 * raw.data.path= split.data.path= inner.product.data.path=
		 * join.data.path= weight.data.path= hive.jdbc.url= hive.jdbc.username=
		 * hive.jdbc.password=
		 */
		Properties prop = new Properties();
		prop.load(new FileInputStream(args[0]));
		Connection conn = getHiveConn(prop);
		Statement stmt = conn.createStatement();
		// 行号\t列分块id\t标签\t开始\t结束\t特征
		String sql = "create table if not exists split_data(line_no string,column_id string,label string,start string,end string,feature string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE";
		stmt.execute(sql);
		sql = "create table if not exists inner_product(line_no string,inner_product string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE";
		stmt.execute(sql);
		// 行号\t列分块id\t标签\t开始\t结束\t内积\t特征
		sql = "create table if not exists feature_product(line_no string,column_id string,label string,start string,end string,inner_product string, feature string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE";
		stmt.execute(sql);
		int iter = Integer.valueOf(prop.getProperty("iter.count"));
		int dim = Integer.valueOf(prop.getProperty("data.dim"));
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
		conf.setDouble("learning_rate", Double.valueOf(prop.getProperty("learning_rate")));
		FileSystem fs = FileSystem.get(conf);
		conf.setInt("column_split_dim", Integer.valueOf(prop.getProperty("column.split.column")));
		conf.set("param", params);
		Job splitJob = Job.getInstance(conf, "LR_SplitJob");
		splitJob.setJarByClass(SplitMap.class);
		splitJob.setMapperClass(LRMap.class);
		splitJob.setMapOutputKeyClass(Text.class);
		splitJob.setMapOutputValueClass(Text.class);
		splitJob.setNumReduceTasks(0);
		splitJob.setOutputKeyClass(Text.class);
		splitJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(splitJob, new Path(prop.getProperty("raw.data.path")));
		FileOutputFormat.setOutputPath(splitJob, new Path(prop.getProperty("split.data.path")));
		int flag = splitJob.waitForCompletion(true) ? 0 : 1;
		try {
			while (count < iter) {
				if (count == 1) {
					conf.set("param", params);
				} else {
					FSDataInputStream in = fs.open(new Path(prop.getProperty("weight.data.path") + "/part-r-000000"));
					params = in.readLine();
					in.close();
					conf.set("param", params);
					if (fs.exists(new Path(prop.getProperty("inner.product.data.path")))) {
						fs.delete(new Path(prop.getProperty("inner.product.data.path")), true);
					}
					if (fs.exists(new Path(prop.getProperty("weight.data.path")))) {
						fs.delete(new Path(prop.getProperty("weight.data.path")), true);
					}
				}
				Job innerProductJob = Job.getInstance(conf, "LR_InnerProductJob");
				innerProductJob.setJarByClass(InnerProductMap.class);
				innerProductJob.setJarByClass(InnerProductReduce.class);
				innerProductJob.setMapperClass(InnerProductMap.class);
				innerProductJob.setReducerClass(InnerProductReduce.class);
				innerProductJob.setMapOutputKeyClass(Text.class);
				innerProductJob.setMapOutputValueClass(DoubleWritable.class);
				innerProductJob.setOutputKeyClass(Text.class);
				innerProductJob.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(innerProductJob, new Path(prop.getProperty("split.data.path")));
				FileOutputFormat.setOutputPath(innerProductJob, new Path(prop.getProperty("inner.product.data.path")));
				flag = innerProductJob.waitForCompletion(true) ? 0 : 1;
				// sql join
				sql = "insert overwrite table feature_product select a.line_no ,column_id ,label ,start ,end ,inner_product , feature from split_data as a inner join inner_product as b on a.line_no=b.line_no";
				Job gradientJob = Job.getInstance(conf, "LR_GradientJob");
				gradientJob.setJarByClass(GradientMap.class);
				gradientJob.setJarByClass(GradientReduce.class);
				gradientJob.setJarByClass(GradientCombiner.class);
				gradientJob.setMapperClass(GradientMap.class);
				gradientJob.setCombinerClass(GradientCombiner.class);
				gradientJob.setReducerClass(GradientReduce.class);
				gradientJob.setMapOutputKeyClass(Text.class);
				gradientJob.setMapOutputValueClass(DoubleWritable.class);
				gradientJob.setOutputKeyClass(Text.class);
				gradientJob.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(gradientJob, new Path(prop.getProperty("join.data.path")));
				FileOutputFormat.setOutputPath(gradientJob, new Path(prop.getProperty("weight.data.path")));
				flag = gradientJob.waitForCompletion(true) ? 0 : 1;
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
		return flag;
	}

}
