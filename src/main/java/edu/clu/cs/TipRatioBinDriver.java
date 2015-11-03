package edu.clu.cs;

import java.io.IOException;
import java.io.PrintWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TipRatioBinDriver {

	static public void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		String outPathName = "";
		String inPathName = "";

		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(getOptions(), args);
			outPathName = line.getOptionValue("o");
			inPathName = line.getOptionValue("i");
			if (line.hasOption("h")) {
				printHelp();
				System.exit(0);
			}
		} catch (ParseException exp) {
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			printHelp();
			System.exit(1);
		}

		execute(outPathName, inPathName);
	}

	public static void execute(String outPathName, String inPathName)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Group3 TipRatioBin");
		job.setJarByClass(TipRatioBinDriver.class);
		// job.setMapperClass(WeeklyEnsembleNSHourlyBinMapper.class);

		Configuration conf2 = new Configuration(false);
		
		ChainMapper.addMapper(job, BasicTripDataFilterMapper.class,
				LongWritable.class, TripDataTuple.class, LongWritable.class,
				TripDataTuple.class, conf2);
		ChainMapper.addMapper(job, TipRatioBinMapper.class,
				LongWritable.class, TripDataTuple.class, Text.class,
				IntWritable.class, conf2);
		ChainReducer.setReducer(job,
				IntSumReducer.class,Text.class,
				IntWritable.class, Text.class, IntWritable.class,
				conf2);

//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(FloatWritable.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(NumericalSummaryTuple.class);

		job.setNumReduceTasks(24);

		job.setInputFormatClass(TripDataInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inPathName));
		FileOutputFormat.setOutputPath(job, new Path(outPathName));
		job.waitForCompletion(true);
	}

	private static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		PrintWriter writer = new PrintWriter(System.err);
		formatter
				.printHelp(
						writer,
						80,
						"hadoop jar <SomeJar> edu.clu.cs.TipAmountBinDriver ",
						"CSC582-1 Project Help", getOptions(), 4, 8,
						"Author: Lingyan Zhou & Jianhong Zhu", true);
		writer.close();
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("output").isRequired()
				.hasArg().withDescription("output folder").create("o"));
		options.addOption(OptionBuilder.withLongOpt("input").isRequired()
				.hasArg().withDescription("input folder").create("i"));
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("print this help page").create("h"));
		return options;
	}

}
