package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class FileRecordReader  extends RecordReader<Text, LongWritable> {

	InputSplit4EachFile split;
	String file;
	long fileLength;
	boolean finished;
	TaskAttemptContext context;
	@Override
	public void initialize(InputSplit s, TaskAttemptContext c) throws IOException, InterruptedException {
		context=c;
		this.split=(InputSplit4EachFile)s;
		file=split.getFileName();
		fileLength=context.getConfiguration().getLong("test.fileSize", 1);
		finished = false;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return !finished;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		finished=true;
		return new Text(file);
	}

	@Override
	public LongWritable getCurrentValue() throws IOException, InterruptedException {
		finished=true;
		return new LongWritable(fileLength);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return context.getProgress();
	}

	@Override
	public void close() throws IOException {
	}

}
