package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FloatAverageTuple implements Writable {
	float m_sum = Float.NaN;
	long m_count = 0;

	public void clear() {
		m_sum = Float.NaN;
		m_count = 0;
	}

	public void putRecord(float val) {
		if (Float.isNaN(m_sum)) {
			m_sum = val;
		} else {
			if (!Float.isNaN(val)) {
				m_sum += val;
			}
		}
		if (!Float.isNaN(val)) {
			m_count += 1;
		}
	}

	public void putPartialResult(FloatAverageTuple another) {

		if (Float.isNaN(m_sum)) {
			m_sum = another.m_sum;
		} else {
			if (!Float.isNaN(another.m_sum)) {
				m_sum += another.m_sum;
			}
		}
		m_count += another.m_count;
	}

	public float getResult() {
		return m_sum / m_count;
	}

	public String toString() {
		return "" + getResult();
	}

	public void readFields(DataInput in) throws IOException {
		m_sum = in.readFloat();
		m_count = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(m_sum);
		out.writeLong(m_count);

	}

}
