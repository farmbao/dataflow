package org.apache.hadoop.fs;

public class UserArguments {
	private static final int DEFAULT_BUFFER_SIZE = 1000000;
	private static final String DEFAULT_RES_FILE_NAME = "TestDFSIO_results.log";
	private int nrFiles =1;
	private int fileSize = 1;
	private int bufferSize =DEFAULT_BUFFER_SIZE;
	private Command cmd = Command.READ;
	private String resFile =DEFAULT_RES_FILE_NAME;
	private int replication =2;
	private boolean seq=false;
	private String framework="yarn";
	public int getNrFiles() {
		return nrFiles;
	}
	public void setNrFiles(int nrFiles) {
		this.nrFiles = nrFiles;
	}
	public int getFileSize() {
		return fileSize;
	}
	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}
	public Command getCmd() {
		return cmd;
	}
	public void setCmd(Command cmd) {
		this.cmd = cmd;
	}
	public String getResFile() {
		return resFile;
	}
	public void setResFile(String resFile) {
		this.resFile = resFile;
	}
	public int getReplication() {
		return replication;
	}
	public void setReplication(int replication) {
		this.replication = replication;
	}
	public int getBufferSize() {
		return bufferSize;
	}
	public void setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
	}
	
	public boolean isSeq() {
		return seq;
	}
	public void setSeq(boolean seq) {
		this.seq = seq;
	}
	
	public String getFramework() {
		return framework;
	}
	public void setFramework(String framework) {
		this.framework = framework;
	}
	@Override
	public String toString() {
		return "UserArguments [nrFiles=" + nrFiles + ", fileSize=" + fileSize + ", bufferSize=" + bufferSize + ", cmd="
				+ cmd + ", resFile=" + resFile + ", replication=" + replication + ", seq=" + seq + ", framework="
				+ framework + "]";
	}
	
}
