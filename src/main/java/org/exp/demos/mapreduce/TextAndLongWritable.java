package org.exp.demos.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

class TextAndLongWritable implements WritableComparable<TextAndLongWritable>{
	private Text text;
	private LongWritable length;
	
	public TextAndLongWritable(){
		set(new Text(),new LongWritable());
	}
	public TextAndLongWritable(String text, long length){
		set(new Text(text),new LongWritable(length));
	}
	
	public void set(Text text, LongWritable length){
		this.text = text;
		this.length = length;
	}
	public Text getText(){
		return text;
	}
	public LongWritable getLongWritable(){
		return length;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
		length.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
		length.readFields(in);
	}
	
	@Override
	public int compareTo(TextAndLongWritable o) {
		int compareResult = text.compareTo(o.getText());
		if(compareResult!=0){
			return compareResult;
		}
		return (int)length.compareTo(o.getLongWritable());
	}
	
	public String  toString(){
		String name = this.getText().toString();
		String length = this.getLongWritable().toString();
		StringBuilder sb = new StringBuilder();
		String o_str = sb.append("["+name+","+length+"]").toString();
		return o_str;
	}
	
}