package org.exp.demos.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PartMetadataWritable implements WritableComparable<PartMetadataWritable> {
	private Text rowkey; // dfs:dfs_file表中小文件的rowkey
	private LongWritable length; // dfs:dfs_file表中小文件的f:l(小文件长度)
	private LongWritable start;// dfs:dfs_file表中小文件的f:i(小文件起始位置)
	private Text datafile; // dfs:dfs_file表中小文件的f:n(小文件所属的大文件)
	
	public PartMetadataWritable(){
		set(new Text(), new LongWritable(), new LongWritable(), new Text());
	}

	public PartMetadataWritable(String text, long length, long start, String datafile) {
		set(new Text(text), new LongWritable(length), new LongWritable(start), new Text(datafile));
	}
	
	public void set(Text rowkey, LongWritable length, LongWritable start, Text datafile) {
		this.rowkey = rowkey;
		this.length = length;
		this.start = start;
		this.datafile = datafile;
	}

	public Text getRowkey() {
		return rowkey;
	}

	public LongWritable getLength() {
		return length;
	}

	public LongWritable getStart() {
		return start;
	}

	public Text getDatafile() {
		return datafile;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		rowkey.write(out);
		length.write(out);
		start.write(out);
		datafile.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		rowkey.readFields(in);
		length.readFields(in);
		start.readFields(in);
		datafile.readFields(in);
	}
	
	// 比较两个小文件(用于排除复制小文件),复制小文件与小文件有相同的f:n、f:i、f:l值,但是rowkey不同
	@Override
	public int compareTo(PartMetadataWritable o) {
		int compareDatafile = datafile.compareTo(o.datafile);
		if (compareDatafile == 0) {
			int compareLength = length.compareTo(o.getLength());
			if (compareLength == 0) {
				return start.compareTo(o.start);
			}
		}
		return compareDatafile;
	}
	
	@Override
	public String  toString(){
		String name = this.getRowkey().toString();
		String length = this.getLength().toString();
		String start = this.getStart().toString();
		StringBuilder sb = new StringBuilder();
		String o_str = sb.append(name + "," + length + "," + start + ";").toString();
		return o_str;
	}
	
}