package cn.wisenergy.pai.tez.format;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.zookeeper.common.IOUtils;

import cn.wisenergy.pai.hadoop2.Constants;
import cn.wisenergy.pai.hadoop2.seismic.SegyUtil;
import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderFormat;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;

public class TraceRecordReader extends RecordReader<TraceHeaderWritable, TraceDataWritable> {
	private static final Log LOG=LogFactory.getLog(TraceRecordReader.class);
	private TraceSplit split;
	private FSDataInputStream headerStream;
	private FSDataInputStream dataStream;
	private long currentTrace=0l;
	private long totalTraces=0l;
	private long originalOffset;
	private TaskAttemptContext context;
	
	@Override
	public void initialize(InputSplit s, TaskAttemptContext c) throws IOException, InterruptedException {
		split=(TraceSplit)s;
		String filePath=split.getFilePath();
		String dataFileName=filePath+".dat";
		String headerFileName=filePath+".info";
		FileSystem fs=FileSystem.get(c.getConfiguration());
		headerStream = fs.open(new Path(headerFileName));
		dataStream = fs.open(new Path(dataFileName));
		LOG.info("split:"+split.toString());
		totalTraces=split.getTracesSize();
		originalOffset=split.getOffset();
		long startTrace=originalOffset/(Constants.DEFAULT_DATALEN);
		long originalHeaderStreamOffset=startTrace*Constants.HEADERLEN;
		LOG.info("the header file len "+fs.getFileStatus(new Path(headerFileName)).getLen()+",the offset is "+originalHeaderStreamOffset+",pos :"+headerStream.getPos());
		LOG.info("the data file len "+fs.getFileStatus(new Path(dataFileName)).getLen()+",the offset is "+originalOffset+",pos :"+dataStream.getPos());
		headerStream.seek(originalHeaderStreamOffset);
		dataStream.seek(originalOffset);
		currentTrace=0l;
		context=c;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return currentTrace != totalTraces;
	}

	@Override
	public TraceHeaderWritable getCurrentKey() throws IOException, InterruptedException {
		headerStream.seek(currentTrace*Constants.HEADERLEN);
		byte[] temp=new byte[Constants.HEADERLEN];
		headerStream.read(temp);
		TraceHeaderWritable key=new TraceHeaderWritable();
		key.setHeaderBytes(temp);
		int[] keys=new int[2];
		SegyUtil util = new SegyUtil(temp, false);
		keys[0] = util.readInt(TraceHeaderFormat.m_OriginalFieldRecordNo); // 8
		keys[1] = util.readInt(TraceHeaderFormat.m_TraceNoWithinOriginalField); // 4
		key.setKeys(keys);
		currentTrace++;
//		if(traceCounter%1000==0){
//			LOG.info("write trace:"+traceCounter);
//		}
		return key;
	}

	@Override
	public TraceDataWritable getCurrentValue() throws IOException, InterruptedException {
		dataStream.seek(currentTrace*Constants.DEFAULT_DATALEN);
		byte[] temp=new byte[Constants.DEFAULT_DATALEN];
		dataStream.read(temp);
		TraceDataWritable key=new TraceDataWritable();
		key.setDataBytes(temp);
		return key;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		LOG.info("the currentTrace :"+currentTrace);
		LOG.info("the progress :"+currentTrace/(float)totalTraces);
		return currentTrace/(float)totalTraces;
	}

	@Override
	public void close() throws IOException {
		LOG.info("close the record reader.");
		IOUtils.closeStream(headerStream);
		IOUtils.closeStream(dataStream);
	}

}
