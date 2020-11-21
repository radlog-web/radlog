package edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbByte;
import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbShort;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleRowPageLeaf 
	extends TupleRowPage 
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private int bytesPerTuple;
	private DataType[] schema;
	private int numberOfColumns;
	private Data data;
	private BitSet deletedTuples;

	public TupleRowPageLeaf() { super(); }
	
	public TupleRowPageLeaf(int pageSize, int bytesPerTuple, DataType[] columnDataTypes, TypeManager typeManager) {
		super(typeManager);
		if (pageSize < 1)
			throw new DatabaseException("PageSize must be larger than 0.");
		
		if (bytesPerTuple < 1)
			throw new DatabaseException("BytesPerTuple must be larger than 0.");
		
		if (columnDataTypes == null || columnDataTypes.length == 0)
			throw new DatabaseException("ColumnDataTypes must be provided.");
		
		this.data = new Data(pageSize);
		this.bytesPerTuple = bytesPerTuple;
		this.schema = columnDataTypes;
		this.numberOfColumns = columnDataTypes.length;
		this.initialize();
	}
	
	private void initialize() {
		this.highWaterMark = 0;
		this.deletedTuples = new BitSet(this.getMaxNumberOfTuples());
		this.data.clear();
	}
	
	// APS 11/15/2013 - so we can directly access the tuple's binary form
	public Data getData() { return this.data; }
	
	public int getBytesPerTuple() { return this.bytesPerTuple; }
	
	public int getMaxNumberOfTuples() { return this.data.getSize() / this.bytesPerTuple; }
		
	@Override
	public int getHeight() { return 0; }
	
	public int getOffset() { return this.data.getOffset(); }
	
	public void setOffset(int offset) { this.data.setOffset(offset); }
	
	public void setOffsetByAddress(long address) {
		if (address < 0) {
			this.data.setOffset(0);
			return;
		}
		
		this.data.setOffset(((int)address) * this.bytesPerTuple);
	}
	
	public void setOffsetToHighWaterMark() {			
		this.data.setOffset(this.highWaterMark * this.bytesPerTuple);
	}
	
	@Override
	public int getFirstTupleAddress() {
		if (this.highWaterMark == 0)
			return -1;
		
		for (int i = 0; i < this.getMaxNumberOfTuples(); i++)
			if (!this.deletedTuples.get(i))
				return i;
		
		return -1;
	}
	
	@Override
	public int getLastTupleAddress() {
		if (this.highWaterMark == 0)
			return -1;
		
		return (this.highWaterMark - 1);
	}

	@Override
	public boolean isEmpty() {
		return (this.highWaterMark == 0);
	}
	
	@Override
	public boolean isFull() {
		return (this.highWaterMark == this.getMaxNumberOfTuples());
	}
	
	@Override
	public int appendTuple(AddressedTuple tuple) {
		if (!this.data.canWrite(this.bytesPerTuple))
			return -1;

		// if we made it this far, we have room on the page to write the tuple
		// only write the amount of columns from the tuple that we have specified in initialization
		switch (this.numberOfColumns) {
		case 1:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			break;
		case 2:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			break;
		case 3:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			break;
		case 4:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			this.writeDbType(this.schema[3], tuple.columns[3]);
			break;
		case 5:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			this.writeDbType(this.schema[3], tuple.columns[3]);
			this.writeDbType(this.schema[4], tuple.columns[4]);
			break;
		case 6:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			this.writeDbType(this.schema[3], tuple.columns[3]);
			this.writeDbType(this.schema[4], tuple.columns[4]);
			this.writeDbType(this.schema[5], tuple.columns[5]);
			break;
		default:
			for (int i = 0; i < this.numberOfColumns; i++)
				this.writeDbType(this.schema[i], tuple.columns[i]);		
		}		
		
		return highWaterMark++;
	}
	
	// this is used to rapidly move tuples between pages
	public int appendTupleData(byte[] tupleData) {
		if (tupleData.length != this.bytesPerTuple)
			return -1;
		
		if (!this.data.canWrite(this.bytesPerTuple))
			return -1;
		
		this.data.write(tupleData);
		
		return highWaterMark++;
	}
	
	@Override
	public int updateTuple(int address, AddressedTuple tuple) {
		this.setOffsetByAddress(address);
		
		//for (int i = 0; i < this.numberOfColumns; i++)
		//	if (!writeDbType(this.columnDataTypes[i], tuple.columns[i]))
		//		return -1;
		
		switch (this.numberOfColumns) {
		case 1:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			break;
		case 2:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			break;
		case 3:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			break;
		case 4:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			this.writeDbType(this.schema[3], tuple.columns[3]);
			break;
		case 5:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			this.writeDbType(this.schema[3], tuple.columns[3]);
			this.writeDbType(this.schema[4], tuple.columns[4]);
			break;
		case 6:
			this.writeDbType(this.schema[0], tuple.columns[0]);
			this.writeDbType(this.schema[1], tuple.columns[1]);
			this.writeDbType(this.schema[2], tuple.columns[2]);
			this.writeDbType(this.schema[3], tuple.columns[3]);
			this.writeDbType(this.schema[4], tuple.columns[4]);
			this.writeDbType(this.schema[5], tuple.columns[5]);
			break;
		default:
			for (int i = 0; i < this.numberOfColumns; i++)
				this.writeDbType(this.schema[i], tuple.columns[i]);		
		}
		
		return address;
	}

	@Override
	public int readTuple(int address, AddressedTuple tuple) {
		if (address >= this.highWaterMark)// || address < 0)
			return -1;

		if (this.deletedTuples.get(address))
			return -1;

		this.setOffsetByAddress(address);
		
		for (int i = 0; i < this.numberOfColumns; i++)
			tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], this.data, this.typeManager); 
		
		tuple.address = address;
		tuple.isDeleted = 0;
		return address;
	}
	
	// this is used to rapidly move tuples between pages
	public byte[] readTupleData(int address) {
		if (address >= this.highWaterMark || address < 0)
			return null;

		if (this.deletedTuples.get(address))
			return null;
		
		this.setOffsetByAddress(address);
		return this.data.read(this.bytesPerTuple);
	}
	
	@Override
	public void deleteAll() {
		this.data.reset();
		this.data.clear();
		this.initialize();
	}
	
	@Override
	public void deleteTuple(int address) {
		if (address > this.highWaterMark)
			return;
		// mark the tuple at address as invalid
		this.deletedTuples.set(address);
	}
	
	@Override
	public int commit() {
		if (this.deletedTuples.isEmpty())
			return 0;
		
		int numberDeleted = 0;
		
		// if deleting everything, just reset the data block		
		if (this.deletedTuples.intersects(new BitSet())) {
			numberDeleted = this.deletedTuples.size();
			this.deleteAll();
		} else {		
			// to commit the deletes, we will move the valid records to a new data block
			Data newData = new Data(this.data.getSize());
			Data temp;
			
			this.data.setOffset(0);
			//Tuple tuple;
			byte[] tupleData;
			for (int i = 0; i <= this.getLastTupleAddress(); i++) {			
				//tuple = this.readTuple(i);
				tupleData = this.readTupleData(i);
				// if we have null = we deleted the tuple
				//if (tuple == null) {
				if (tupleData == null) {
					numberDeleted++;
					continue;
				}
				temp = this.data;
				this.data = newData;
				this.data.write(tupleData);				
				this.data = temp;
			}
			
			this.data = newData;
			this.setOffset(0);
			this.deletedTuples = new BitSet(this.getMaxNumberOfTuples());
			this.highWaterMark -= numberDeleted;
		}		
	
		return numberDeleted;
	}
	
	private boolean writeDbType(DataType dataType, DbTypeBase val) {
		switch (dataType) {
			case INT:
				data.writeInt(((DbInteger)val).getValue());
				break;
			case STRING:
				data.writeInt(((DbString)val).getKey());
				break;
			case COMPLEX:
				data.writeInt(((DbComplex)val).getKey());
				break;
			case DOUBLE:
				switch (val.getDataType()) {
				case INT: 
					data.write(DbDouble.create(((DbInteger)val).getValue()).getBytes());
					break;
				case DOUBLE:
					data.write(val.getBytes());
					break;
				}
				break;
			case LONG:
				switch (val.getDataType()) {
				case INT:
					data.writeLong(((DbInteger)val).getValue());
					break;
				case LONG:
					data.writeLong(((DbLong)val).getValue());
					break;
				}
				break;
			case FLOAT:
				switch (val.getDataType()) {
				case INT: 
					data.write(DbFloat.create(((DbInteger)val).getValue()).getBytes());
					break;
				case FLOAT:
					data.write(val.getBytes());
					break;
				}
				break;
			case LONGLONG:
				data.write(DbLongLong.getBytes(val));
				break;
			case LONGLONGLONGLONG:
				data.write(DbLongLongLongLong.getBytes(val));
				break;
			case LIST:
				data.writeLong(((DbList)val).getKeyL());
				break;
			case SET:
				data.writeInt(((DbSet)val).getId());
				break;
			case KEYVALUESTORE:
				data.writeInt(((DbKeyValueStore)val).getId());
				break;
			case DATETIME:
				data.writeLong(((DbDateTime)val).getKeyL());
				break;
			case SHORT:
				data.writeShort(((DbShort)val).getValue());
				break;
			case BYTE:
				data.write(((DbByte)val).getValue());
				break;
			case AVERAGE:
				data.write(((DbAverage)val).getBytes());
				break;
			//case UNKNOWN:// hopefully this is only for nil				
			default:
				return false;
		}
		
		return true;
	}
		
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Max Number of tuples: " + this.getMaxNumberOfTuples() + "\n");
		retval.append("Number of tuples: " + this.highWaterMark + "\n");
		retval.append("IsEmpty: " + this.isEmpty() + "\n");
		retval.append("IsFull: " + this.isFull() + "\n");
		return retval.toString();
	}
	
	public MemoryMeasurement getSizeOf() {		
		List<Integer> variableLengthColumns = new ArrayList<>();
		int used = 0;
		int allocated = 0;
		
		// if any special types are stored in this page, get their size
		for (int i = 0; i < this.schema.length; i++)
			if (DataType.isVariableLength(this.schema[i]))
				variableLengthColumns.add(i);
	
		if (variableLengthColumns.size() == 0) {
			used = this.bytesPerTuple * this.highWaterMark;
			allocated = this.data.getSize();
		} else {
			AddressedTuple tuple = new AddressedTuple(this.numberOfColumns);
			for (int i = 0; i < this.schema.length; i++)
				tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], 0);
			MemoryMeasurement mm = null;			
			for (int i = 0; i < this.highWaterMark; i++) {
				this.readTuple(i, tuple);
				if (tuple.address > -1) {
					mm = tuple.getSizeOf();
					used += mm.getUsed();
					allocated += mm.getAllocated();
				} else {
					// must count towards something even if logically deleted
					used += this.bytesPerTuple;
					allocated += this.bytesPerTuple;
				}
			}
			
			// must include space required to store tuple entries that wasn't used 
			allocated += this.data.getSize() - (this.bytesPerTuple * this.highWaterMark);
		}
		
		return new MemoryMeasurement(used, allocated); 
	}
}
