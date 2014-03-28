package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TestDFSIOInputFormat  extends InputFormat<Text, LongWritable> implements Configurable{
	private static final Log LOG=LogFactory.getLog(TestDFSIOInputFormat.class);
	private Configuration conf;
	@Override
	public void setConf(Configuration conf) {
		this.conf=conf;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		int nrFiles=context.getConfiguration().getInt("test.nrFiles",1);
		LOG.info("the nrFiles :"+nrFiles);
		List<InputSplit> splits=new ArrayList<InputSplit>(nrFiles);
		FileSystem fs=FileSystem.get(context.getConfiguration());
		if(context.getConfiguration().getEnum("test.cmd", Command.READ)==Command.READ){
			LOG.info("split for read");
			for(int i=0;i<nrFiles;i++){
				InputSplit4EachFile is=new InputSplit4EachFile();
				Path p=new Path(WisenergyTestDFSIO.DATA_DIR,WisenergyTestDFSIO.getFileName(i));
				is.setFileName(WisenergyTestDFSIO.getFileName(i));
				String[] host=getLocations(fs,p);
				is.setLocalSize(host.length);
				is.setLocations(host);
				splits.add(is);
			}
		}else{
			LOG.info("split for write");
			for(int i=0;i<nrFiles;i++){
				InputSplit4EachFile is=new InputSplit4EachFile();
				is.setFileName(WisenergyTestDFSIO.getFileName(i));
				is.setLocalSize(0);
				is.setLocations(new String[0]);
				splits.add(is);
			}
		}
		
		return splits;
	}
	private String[] getLocations(FileSystem fs,Path p) throws IOException{
		FileStatus f=fs.getFileStatus(p);
		BlockLocation[] bl=fs.getFileBlockLocations(f, 0, f.getLen());
		Set<String> hosts=new HashSet<String>();
		for(int i=0;i<bl.length;i++){
			for(String host:bl[i].getHosts()){
				hosts.add(host);
			}
		}
		return hosts.toArray(new String[0]);
	}
	@Override
	public RecordReader<Text, LongWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		RecordReader<Text, LongWritable> result=new FileRecordReader();
		result.initialize(split, context);
		return result;
	}

}
