package cn.wisenergy.pai.tez.format;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class TraceOutputCommitter extends FileOutputCommitter {
	private final static Log LOG = LogFactory.getLog(TraceOutputCommitter.class);

	public TraceOutputCommitter(Path outputPath, JobContext context) throws IOException {
		super(outputPath, context);
	}
	public TraceOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
		super(outputPath, context);
	}

	 @Override
	  public boolean needsTaskCommit(TaskAttemptContext context
	                                 ) throws IOException {
		LOG.info("check need commit");
	    return needsTaskCommit(context, null);
	  }
	
	@Override
	public void setupTask(TaskAttemptContext context) throws IOException {
		LOG.info("setupTask");
		super.setupTask(context);
	}
	@Override
	public boolean needsTaskCommit(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
		LOG.info("check need commit");
		if (taskAttemptPath == null) {
			taskAttemptPath = getTaskAttemptPath(context);
		}
		FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
		LOG.info("the taskAttemptPath is "+taskAttemptPath+",exists?"+fs.exists(taskAttemptPath));
		return fs.exists(taskAttemptPath);
	}

	@Override
	public void commitJob(JobContext context) throws IOException {
		LOG.info("commit job");
		super.commitJob(context);
	}
	@Override
	public void commitTask(TaskAttemptContext context, Path taskAttemptPath) throws IOException {
		LOG.info("commitTask");
		TaskAttemptID attemptId = context.getTaskAttemptID();
		context.progress();
		if (taskAttemptPath == null) {
			taskAttemptPath = getTaskAttemptPath(context);
		}
		Path committedTaskPath = getCommittedTaskPath(context);
		FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
		if (fs.exists(taskAttemptPath)) {
			if (fs.exists(committedTaskPath)) {
				if (!fs.delete(committedTaskPath, true)) {
					throw new IOException("Could not delete " + committedTaskPath);
				}
			}
			if (!fs.rename(taskAttemptPath, committedTaskPath)) {
				throw new IOException("Could not rename " + taskAttemptPath + " to " + committedTaskPath);
			}
			LOG.info("Saved output of task '" + attemptId + "' to " + committedTaskPath);
		} else {
			LOG.warn("No Output found for " + attemptId);
		}
	}
}
