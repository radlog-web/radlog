package edu.ucla.cs.wis.bigdatalog.database.store;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class Data implements Serializable {
	private static final long serialVersionUID = 1L;
	private byte[] data;
	private int offset; 
	
	public Data() {}
	
	public Data(byte[] data) {
		this.data = new byte[data.length];
		System.arraycopy(data,  0, this.data, 0, data.length);
		this.offset = 0;
	}
	
	public Data(int size) {
		this.data = new byte[size];
		
		for (int i = 0; i < size; i++) 
			this.data[i] = 0;
		
		this.offset = 0;
	}
	
	public byte[] getData() { return this.data; }
	
	public int getSize() {return this.data.length;}
	
	public void reset() {
		this.offset = 0;
	}
	
	public void clear() {
		this.data = new byte[data.length];
	}
	
	public int getOffset() {return this.offset;}
	
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	public boolean canWrite(int size) {
		return ((data.length - offset) >= size);
	}
	
	public void write(byte val) {
		if ((offset + 1) > data.length)
			throw new DatabaseException("Data cannot be written.  Not enough available space.");

		data[offset++] = val;
	}
	
	public void writeShort(short val) {
		if ((offset + 2) > data.length)
			throw new DatabaseException("Data cannot be written.  Not enough available space.");

		data[offset++] = (byte) ((val >> 8) & 0xFF);
		data[offset++] = (byte) (val & 0xFF);
	}

	public void writeInt(int val) {
		if ((offset + 4) > data.length)
			throw new DatabaseException("Data cannot be written.  Not enough available space.");

		data[offset++] = (byte) ((val >> 24) & 0xFF);
		data[offset++] = (byte) ((val >> 16) & 0xFF);
		data[offset++] = (byte) ((val >> 8) & 0xFF);
		data[offset++] = (byte) (val & 0xFF);
	}
	
	public void writeLong(long val) {
		if ((offset + 8) > data.length)
			throw new DatabaseException("Data cannot be written.  Not enough available space.");

		data[offset++] = (byte) ((val >> 56) & 0xFF);
		data[offset++] = (byte) ((val >> 48) & 0xFF);
		data[offset++] = (byte) ((val >> 40) & 0xFF);
		data[offset++] = (byte) ((val >> 32) & 0xFF);
		data[offset++] = (byte) ((val >> 24) & 0xFF);
		data[offset++] = (byte) ((val >> 16) & 0xFF);
		data[offset++] = (byte) ((val >> 8) & 0xFF);
		data[offset++] = (byte) (val & 0xFF);
	}

	public void write(byte[] val) {
		if ((val.length + offset) > data.length)
			throw new DatabaseException("Data cannot be written.  Not enough available space.");
		
		System.arraycopy(val, 0, this.data, this.offset, val.length);
		this.offset += val.length;	
	}
	
	public byte[] read(int length) {
		int lengthToRead = length;
		if ((offset + length) > data.length)
			lengthToRead = data.length - offset;
			
		byte[] output = new byte[lengthToRead];
		System.arraycopy(data, offset, output, 0, lengthToRead);
		offset += lengthToRead;

		return output;
	}
	
	public byte[] read(int offset, int length) {
		int lengthToRead = length;
		if ((offset + length) > data.length)
			lengthToRead = data.length - offset;
			
		byte[] output = new byte[lengthToRead];		
		System.arraycopy(data, offset, output, 0, lengthToRead);		
		offset += lengthToRead;		
		return output;
	}
	
	public byte read() {
		if (offset >= data.length)
			return -1;
		
		return data[offset++];
	}
	
	public short readShort() {
		return (short)((data[offset++] & 255) << 8 
				| (data[offset++] & 255));
	}
	
	public int readInt() {
		return ((data[offset++] & 255) << 24 
				| (data[offset++] & 255) << 16 
				| (data[offset++] & 255) << 8 
				| (data[offset++] & 255));
	}
	
	public long readLong() {
		return 	(((long)data[offset++] << 56) |
	                ((long)(data[offset++] & 255) << 48) |
	                ((long)(data[offset++] & 255) << 40) |
	                ((long)(data[offset++] & 255) << 32) |
	                ((long)(data[offset++] & 255) << 24) |
	                ((data[offset++] & 255) << 16) |
	                ((data[offset++] & 255) <<  8) |
	                ((data[offset++] & 255) <<  0));
	}
	
	public double readDouble() {
		return Double.longBitsToDouble(readLong());
	}
	
	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}
	
	public char readChar() {
		int ch1 = read();
		int ch2 = read();
		if ((ch1 | ch2) < 0)
			throw new DatabaseException("Error reading char from page.");
		return (char)((ch1 << 8) + (ch2 << 0));
	}
	
	public final String readString(int length) {
		byte[] buffer = new byte[length];
		
		for (int i = 0; i < length; i++)
			buffer[i] = Byte.valueOf(read());
		
		return new String(buffer);
	}
	
	public void shiftLeft(int offset, int numberOfBytesToShift) {
		if (numberOfBytesToShift < 1)
			return;
		
		System.arraycopy(this.data, offset, this.data, offset - numberOfBytesToShift, this.data.length - offset);
	}
	
	public void shiftRight(int offset, int numberOfBytesToShift) {
		if (numberOfBytesToShift < 1)
			return;

		System.arraycopy(this.data, offset, this.data, offset + numberOfBytesToShift, this.data.length - (offset + numberOfBytesToShift));
		for (int i = 0; i < numberOfBytesToShift; i++)
			this.data[i + offset] = 0;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Size: " + this.data.length + ", Offset: " + this.offset);		
		return retval.toString();
	}
	
	public void grow(int bytesToAdd) {
		byte[] temp = this.data;
		this.data = new byte[this.data.length + bytesToAdd];
		System.arraycopy(temp, 0, this.data, 0, temp.length);
	}
}
