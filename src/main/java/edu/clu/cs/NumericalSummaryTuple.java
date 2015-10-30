package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class NumericalSummaryTuple implements Writable {
	private float m_min = Float.NaN;
	private float m_max = Float.NaN;
	private long m_missingCount = 0;
	private long m_totalCount = 0;
	private double m_sum = Double.NaN;
	private double m_squaredSum = Double.NaN;

	public void clear() {
		m_min = Float.NaN;
		m_max = Float.NaN;
		m_missingCount = 0;
		m_totalCount = 0;
		m_sum = Double.NaN;
		m_squaredSum = Double.NaN;
	}

	public void putRecord(float val) {
		m_totalCount += 1;
		if (!Float.isNaN(val)) {
			if (m_max >= val) {
			} else {
				m_max = val;
			}

			if (m_min <= val) {
			} else {
				m_min = val;
			}

			if (!Double.isNaN(m_sum)) {
				m_sum += val;
			} else {
				m_sum = val;
			}

			if (!Double.isNaN(m_squaredSum)) {
				m_squaredSum += Math.pow(val, 2);
			} else {
				m_squaredSum = Math.pow(val, 2);
			}

		} else {
			m_missingCount += 1;
		}
	}

	public void putPartialSummary(NumericalSummaryTuple val) {
		m_totalCount += val.m_totalCount;
		m_missingCount += val.m_missingCount;
		if (!Float.isNaN(val.m_max)) {
			if (m_max >= val.m_max) {
			} else {
				m_max = val.m_max;
			}
		}

		if (!Float.isNaN(val.m_min)) {
			if (m_min <= val.m_min) {
			} else {
				m_min = val.m_min;
			}
		}

		if (!Double.isNaN(val.m_sum)) {
			if (!Double.isNaN(m_sum)) {
				m_sum += val.m_sum;
			} else {
				m_sum = val.m_sum;
			}
		}

		if (!Double.isNaN(val.m_squaredSum)) {
			if (!Double.isNaN(m_squaredSum)) {
				m_squaredSum += val.m_squaredSum;
			} else {
				m_squaredSum = val.m_squaredSum;
			}

		}
	}
	
	// public void setMin(float val) {
	// m_min = val;
	// }
	//
	// public void setMax(float val) {
	// m_max = val;
	// }
	//
	// public void setTotalCount(long val) {
	// m_totalCount = val;
	// }
	//
	// public void setMissingCount(long val) {
	// m_missingCount = val;
	// }
	//
	// public void setSum(float sum) {
	// m_sum = sum;
	//
	// }
	//
	// public void setSquaredSum(float m_squaredSum) {
	// this.m_squaredSum = m_squaredSum;
	// }

	// public float getSquaredSum() {
	// return m_squaredSum;
	// }
	//
	// public float getMin() {
	// return m_min;
	// }
	//
	// public float getMax() {
	// return m_max;
	// }
	//
	// public long getTotalCount() {
	// return m_totalCount;
	// }
	//
	// public long getMissingCount() {
	// return m_missingCount;
	// }
	//
	// public float getSum() {
	// return m_sum;
	// }

	private float getMean() {
		return (float) m_sum / (m_totalCount - m_missingCount);
	}

	private float getStdDev() {
		return (float) Math.sqrt(m_squaredSum / (m_totalCount - m_missingCount) - Math.pow(getMean(), 2));
	}

	public String toString() {
		return "" + m_min + "\t" + m_max + "\t" + getMean() + "\t"
				+ getStdDev() + "\t" + m_missingCount + "\t" + m_totalCount
				+ "\t" + m_sum;
	}

	public void readFields(DataInput in) throws IOException {
		m_min = in.readFloat();
		m_max = in.readFloat();
		m_sum = in.readDouble();
		m_squaredSum = in.readDouble();
		m_missingCount = in.readLong();
		m_totalCount = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(m_min);
		out.writeFloat(m_max);
		out.writeDouble(m_sum);
		out.writeDouble(m_squaredSum);
		out.writeLong(m_missingCount);
		out.writeLong(m_totalCount);
	}

}
