package edu.clu.cs;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumericalSummaryDriver {
	static public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		HashMap<String, CmdArgumentParser.OptionType> schema = new HashMap<String, CmdArgumentParser.OptionType>();
		
		schema.put("-i", CmdArgumentParser.OptionType.Required);
		schema.put("-o", CmdArgumentParser.OptionType.Required);
		schema.put("-c", CmdArgumentParser.OptionType.Optional);
		
		CmdArgumentParser parser = new CmdArgumentParser(schema, null);
		try {
			parser.parse(args);
		} catch (Exception e) {
			System.err.println(e.toString());

			System.err.println("Usage: edu.clu.cs.NumericalSummaryDriver -c <row_name/num> -i <input> -o <output>");
			System.exit(1);
		}
		
//		String outPathName = args[5];
//		String inPathName = args[3];
//		String columnName = args[1];
		String outPathName = parser.get("-o");
		String inPathName = parser.get("-i");
		String columnName = parser.get("-c");
		
		Configuration conf = new Configuration();
		conf.set("cols",  columnName);
		
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(NumericalSummaryDriver.class);
		job.setMapperClass(NumericalSummaryMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setReducerClass(NumericalSummaryReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NumericalSummaryTuple.class);
		
		//job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(inPathName));
		FileOutputFormat.setOutputPath(job, new Path(outPathName));
		job.waitForCompletion(true);
	}

}
