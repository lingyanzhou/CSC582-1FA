package edu.clu.cs;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class NumericalSummaryMapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {
	private FloatWritable m_val = new FloatWritable();
	private Text[] m_colName = null;
	// private Random m_rnd = new Random();
	private int[] m_colIndex = null;

	@Override
	public void setup(Context context) {
		String[] cols = context.getConfiguration().get("cols").split(",");
		for (int i = 0; i < cols.length; ++i) {
			cols[i] = cols[i].trim();
		}
		m_colName = new Text[cols.length];
		m_colIndex = new int[cols.length];

		for (int i = 0; i < cols.length; ++i) {
			m_colName[i] = new Text(TripDataFormat.getColName(cols[i]));
			m_colIndex[i] = TripDataFormat.getColIndex(cols[i]);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split(",");

		for (int i = 0; i < m_colIndex.length; ++i) {
			try {
				m_val.set(Float.valueOf(fields[m_colIndex[i]].trim()));
			} catch (NumberFormatException e) {
				m_val.set(Float.NaN);
			}

			// if (m_rnd.nextFloat()<0.0001)
			context.write(m_colName[i], m_val);
		}

	}
}
