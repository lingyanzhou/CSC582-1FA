package edu.clu.cs;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class StringFloatRecordReader extends RecordReader<Text, FloatWritable> {

	private LineRecordReader m_lineReader = new LineRecordReader();
	private Text m_key = new Text();
	private FloatWritable m_value = new FloatWritable();

	@Override
	public void close() throws IOException {
		m_lineReader.close();
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		return m_key;
	}

	@Override
	public FloatWritable getCurrentValue() throws IOException,
			InterruptedException {
		return m_value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return m_lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		m_lineReader.initialize(arg0, arg1);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean ret =  m_lineReader.nextKeyValue();
		if (ret) {
			String[] tokens = m_lineReader.getCurrentValue().toString().split("\t");
			m_key.set(tokens[0]);
			m_value.set(Float.parseFloat(tokens[1]));
		}
		return ret;
	}

}
