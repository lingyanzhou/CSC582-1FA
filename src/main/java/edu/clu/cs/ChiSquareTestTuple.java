package edu.clu.cs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ChiSquareTestTuple implements Writable {
	int[][] table = null;

	public void init(Factor var1, Factor var2) {
		table = new int[var1.getLevels().length][var2.getLevels().length];
	}

	public void putRecord(float var1, float var2) {
		if (!Float.isNaN(var1) && !Float.isNaN(var2)) {
			int var1Int = (int)var1;
			int var2Int = (int)var2;
			table[var1Int][var2Int] += 1;
		}
	}

	public void putPartialResult(ChiSquareTestTuple tuple) {
		if (tuple.table != null) {
			if (null == table) {
				table = tuple.table.clone();
			} else if (table.length == tuple.table.length
					&& table[0].length == tuple.table[0].length) {
				for (int i = 0; i < table.length; ++i) {
					for (int j = 0; j < table[0].length; ++j) {
						table[i][j] += tuple.table[i][j];
					}
				}
			}
		}
	}
	
	public float getResult() {
//		float ret = 0;
//		for (int i = 0; i < table.length; ++i) {
//			for (int j = 0; j < table[0].length; ++j) {
//				ret += table[i][j];
//			}
//		}
//		return ret;
		float[][] expected = new float[table.length][table[0].length];
		int[] dim1Margin = new int[table.length];
		int[] dim2Margin = new int[table[0].length];
		for (int i = 0; i < table.length; ++i) {
			for (int j = 0; j < table[0].length; ++j) {
				dim1Margin[i] += table[i][j];
				dim2Margin[j] += table[i][j];
			}
		}
		int n = 0;
		for (int i = 0; i < table.length; ++i) {
			n += dim1Margin[i];
		}
		for (int i = 0; i < table.length; ++i) {
			for (int j = 0; j < table[0].length; ++j) {
				expected[i][j] = (float)dim1Margin[i] * (float)dim2Margin[j] / (float)n;
			}
		}

		float ret = 0;
		for (int i = 0; i < table.length; ++i) {
			for (int j = 0; j < table[0].length; ++j) {
				ret += Math.pow(table[i][j] - expected[i][j], 2) / expected[i][j];
			}
		}
		return ret;
		
	}
	

	public void readFields(DataInput in) throws IOException {
		int dim1 = in.readInt();
		int dim2 = in.readInt();
		table = new int[dim1][dim2];
		for (int i = 0; i < table.length; ++i) {
			for (int j = 0; j < table[0].length; ++j) {
				table[i][j] += in.readInt();
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(table.length);
		out.writeInt(table[0].length);
		for (int i = 0; i < table.length; ++i) {
			for (int j = 0; j < table[0].length; ++j) {
				out.writeInt(table[i][j]);
			}
		}
	}


}
