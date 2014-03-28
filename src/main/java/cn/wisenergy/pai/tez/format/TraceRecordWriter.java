package cn.wisenergy.pai.tez.format;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;

public class TraceRecordWriter extends RecordWriter<TraceHeaderWritable, TraceDataWritable> {
	
	private final static Log LOG=LogFactory.getLog(TraceRecordWriter.class);
	private FSDataOutputStream headerStream;
	private FSDataOutputStream dataStream;
	private int traceCounter;
	
	public TraceRecordWriter(Configuration conf,Path headerPath,Path dataPath) throws IOException{
		FileSystem fs=FileSystem.get(conf);
		if(!fs.exists(dataPath)){
			dataStream = fs.create(dataPath);
			headerStream=fs.create(headerPath);
		}else{
			dataStream = fs.append(dataPath);
			headerStream = fs.append(headerPath);
		}
		
	}

	@Override
	public void write(TraceHeaderWritable key, TraceDataWritable value) throws IOException, InterruptedException {
		headerStream.write(key.getHeaderBytes());
//		LOG.info("header write:"+key.getHeaderBytes().length);
		dataStream.write(value.getDataBytes());
//		LOG.info("data write:"+value.getDataBytes().length);
		traceCounter++;
//		if(traceCounter%1000==0){
//			LOG.info("write trace:"+traceCounter);
//		}
		
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		LOG.info("close the output stream");
		headerStream.close();
		dataStream.close();
		
	}

}
