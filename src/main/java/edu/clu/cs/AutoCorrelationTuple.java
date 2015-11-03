package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AutoCorrelationTuple implements Writable {
	float[][] m_table = null;
	int[] m_sums = null;
	long m_startTime = 0;

	public AutoCorrelationTuple() {

	}

	public void init(long startTime, long endTime, int delayAmount) {
		m_startTime = startTime;
		m_table = new float[(int) ((endTime - startTime) / 3600000.0)
				- delayAmount][delayAmount + 1];
	}

	public void putRecord(long time, float val) {
		int row = (int) ((time - m_startTime) / 3600000.0) - m_table[0].length
				+ 1;
		for (int i = Math.max(-row, 0); i < Math.min(m_table.length - row,
				m_table[0].length); ++i) {
			m_table[i + row][i] += val;
		}
	}

	public void putTable(AutoCorrelationTuple another) {
		for (int i = 0; i < m_table.length; ++i) {
			for (int j = 0; j < m_table[0].length; ++j) {
				m_table[i][j] += another.m_table[i][j];
			}
		}

	}

	public String toString() {
		String ret = "";
		float[] result = getResult();
		for (int i = 0; i < result.length; ++i) {
			ret += result[i];
			ret += ",";
		}
		return ret;
	}

	public float[] getResult() {
		int maxLag = m_table[0].length - 1;
		float[] ret = new float[maxLag + 1];

		for (int lag = 0; lag < maxLag + 1; ++lag) {
			for (int i = 0; i < m_table.length; ++i) {
				ret[lag] += m_table[i][0] * m_table[i][lag];
			}
		}

		return ret;
	}

	public void readFields(DataInput in) throws IOException {
		m_startTime = in.readLong();
		int row = in.readInt();
		int col = in.readInt();
		m_table = new float[row][col];
		for (int i = 0; i < row; ++i) {
			for (int j = 0; j < col; ++j) {
				m_table[i][j] = in.readFloat();
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(m_startTime);
		out.writeInt(m_table.length);
		out.writeInt(m_table[0].length);
		for (int i = 0; i < m_table.length; ++i) {
			for (int j = 0; j < m_table[0].length; ++j) {
				out.writeFloat(m_table[i][j]);
			}
		}

	}

	public void clear() {
		for (int i = 0; i < m_table.length; ++i) {
			for (int j = 0; j < m_table[0].length; ++j) {
				m_table[i][j] = 0;
			}
		}

	}

}
