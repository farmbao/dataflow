package cn.wisenergy.pai.tez.format;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
/**
 * the TraceSplit class is an entity class for split information.
 * 
 * @author wangxin
 *
 */
public class TraceSplit extends InputSplit  implements Writable{
	//file path
	private String filePath;
	//host list
	private String[] locations;
	// hosts size
	private int localSize =0;
	// bytes number
	private long length =0;
	// traces number
	private long tracesSize=0;
	// the offset of the file by byte
	private long offset=0;
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return locations;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(filePath);
		out.writeLong(length);
		out.writeLong(tracesSize);
		out.writeLong(offset);
		if(locations!=null && locations.length>0){
			localSize=locations.length;
			out.writeInt(localSize);
			for(int i = 0 ;i<localSize;i++){
				out.writeUTF(locations[i]);
			}
		}else{
			out.writeInt(0);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		filePath=in.readUTF();
		length=in.readLong();
		tracesSize=in.readLong();
		offset=in.readLong();
		localSize=in.readInt();
		if(localSize>0){
			locations=new String[localSize];
			for(int i=0;i<localSize;i++){
				locations[i]=in.readUTF();
			}
		}else{
			locations=new String[0];
		}
		
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String fileName) {
		this.filePath = fileName;
	}

	public long getTracesSize() {
		return tracesSize;
	}

	public void setTracesSize(long traceSize) {
		this.tracesSize = traceSize;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public void setLocations(String[] locations) {
		this.locations = locations;
		this.localSize=locations.length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	@Override
	public String toString() {
		return "[" + filePath + ", locations=" + Arrays.toString(locations) + ", localSize=" + localSize + ", length=" + length + ", tracesSize=" + tracesSize + ", offset=" + offset
				+ "]";
	}

}
