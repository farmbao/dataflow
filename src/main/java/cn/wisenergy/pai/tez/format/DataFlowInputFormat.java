package cn.wisenergy.pai.tez.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import cn.wisenergy.pai.hadoop2.Constants;
import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;

public class DataFlowInputFormat extends InputFormat<TraceHeaderWritable,TraceDataWritable> {
	private static final Log LOG=LogFactory.getLog(DataFlowInputFormat.class);
	public static final String INPUT_DATAS_PATH="pai.input.path";
	private Configuration conf;

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		conf=context.getConfiguration();
		String filePath=conf.get("pai.input.path");
		FileSystem fs=FileSystem.get(conf);
		FileStatus dataFileStatus=fs.getFileStatus(new Path(filePath+".dat"));
		
		// bytes number in one block.
		long blockSize=dataFileStatus.getBlockSize();
		// bytes number in the file.
		long fileLen=dataFileStatus.getLen();
		// all blocks in the file.
		BlockLocation[] blocks=fs.getFileBlockLocations(dataFileStatus, 0, fileLen);
		// total traces in the file.
		long traces=fileLen/(Constants.DEFAULT_DATALEN);
		// traces number in one block.
		long tracesInOneBlock=blockSize/(Constants.DEFAULT_DATALEN);
		// bytes number in one block.
		long bytesInOneBlock=tracesInOneBlock*Constants.DEFAULT_DATALEN;
		int splitSize=(int)(traces/tracesInOneBlock);
		List<InputSplit> splits=new ArrayList<InputSplit>(splitSize+1);
		long currentOffset=0;
		while(currentOffset<=fileLen){
			TraceSplit split=new TraceSplit();
			split.setFilePath(filePath);
			split.setOffset(currentOffset);
			split.setLocations(getLocations(blocks,currentOffset));
			long length=0;
			if((currentOffset+bytesInOneBlock)<=fileLen){
				length=bytesInOneBlock;
				split.setTracesSize(tracesInOneBlock);
			}else{
				length=fileLen-currentOffset;
				split.setTracesSize(length/Constants.DEFAULT_DATALEN);
			}
			split.setLength(length);
			splits.add(split);
			currentOffset+=bytesInOneBlock;
		}
		for(InputSplit s:splits){
			LOG.info(s);
		}
		return splits;
	}

	private String[] getLocations(BlockLocation[] blks, long start) throws IOException {
		long minDistance = Long.MAX_VALUE;
		BlockLocation best = null;
		for (BlockLocation blk : blks) {
			long distance = Math.abs(start - blk.getOffset());
			if (distance < minDistance) {
				minDistance = distance;
				best = blk;
			}
		}
		return best.getHosts();
	}

	@Override
	public RecordReader<TraceHeaderWritable, TraceDataWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		TraceRecordReader reader=new TraceRecordReader();
		reader.initialize(split, context);
		return reader;
	}



}
