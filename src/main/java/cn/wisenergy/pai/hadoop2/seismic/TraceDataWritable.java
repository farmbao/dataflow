package cn.wisenergy.pai.hadoop2.seismic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TraceDataWritable implements Writable{
	private byte[] dataBytes = {};
	private int length=0;
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(length);
		out.write(dataBytes);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		length=in.readInt();
		byte[] temp=new byte[length];
		in.readFully(temp);
		dataBytes=temp;
		
	}

	public byte[] getDataBytes() {
		return dataBytes;
	}
	public void setDataBytes(byte[] b) {
		length=b.length;
		this.dataBytes = new byte[length];
		System.arraycopy(b, 0, dataBytes,0,length);
	}
	@Override
	public String toString() {
		return "TraceDataWritable [dataBytes=" + dataBytes + ", length=" + length + "]";
	}
	
}
