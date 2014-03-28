package cn.wisenergy.pai.tez.format;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.wisenergy.pai.hadoop2.seismic.TraceDataWritable;
import cn.wisenergy.pai.hadoop2.seismic.TraceHeaderWritable;

public class DataFlowOutputFormat extends FileOutputFormat<TraceHeaderWritable, TraceDataWritable> {

	private final static Log LOG = LogFactory.getLog(DataFlowOutputFormat.class);

	public DataFlowOutputFormat() {
		LOG.info("created the DataFlowOutputFormat");
	}

	@Override
	public RecordWriter<TraceHeaderWritable, TraceDataWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Path dataPath = getDefaultWorkFile(context, ".dat");
		Path infoPath =  getDefaultWorkFile(context, ".info");
		LOG.info("the dataPath is "+dataPath.toString());
		LOG.info("the infoPath is "+infoPath.toString());
		
		LOG.info("create the recordWriter");
		TraceRecordWriter writer = new TraceRecordWriter(context.getConfiguration(), infoPath,dataPath);
		return writer;
	}

//	@Override
//	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
//		super.getOutputCommitter(context);
//		Path output = getOutputPath(context);
//		return new TraceOutputCommitter(output, context);
//	}

}
