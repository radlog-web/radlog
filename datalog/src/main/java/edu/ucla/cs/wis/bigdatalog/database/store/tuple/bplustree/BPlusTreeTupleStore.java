package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.type.BigEncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class BPlusTreeTupleStore 
	extends TupleStoreBase<Tuple> 
	implements RangeSearchableStore {
	private static final long serialVersionUID = 1L;
	
	protected int 				nodeSize;	
	protected DataType[] 		schema;
	protected int[]				keyColumns;	
	protected int[]				valueColumns;
	protected DataType[]		keyColumnTypes;	// types of column btree is indexed by
	protected DataType[]		valueColumnTypes;	// types of column for data
	protected int				bytesPerKey;
	protected int 				bytesPerValue;
	protected Tuple				templateTuple;
	
	public BPlusTreeTupleStore() { super(); }
	
	public BPlusTreeTupleStore(String relationName, DataType[] schema, int[] keyColumns, int nodeSize, TypeManager typeManager) {
		super(relationName, typeManager);
		
		this.schema = schema;
		this.keyColumns = keyColumns;
		this.nodeSize = nodeSize;
		//this.nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.tuplestores.bplustree.nodesize"));
	}
	
	public BPlusTreeTupleStore(String relationName, DataType[] schema, int nodeSize, TypeManager typeManager) {
		super(relationName, typeManager);
		
		// if not specified, all columns are key columns 
		int[] keyColumns = new int[schema.length];
		for (int i = 0; i < schema.length; i++)
			keyColumns[i] = i;
		
		this.schema = schema;
		this.keyColumns = keyColumns;
		this.nodeSize = nodeSize;
		//this.nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.tuplestores.bplustree.nodesize"));
	}

	public int getBytesPerKey() { return this.bytesPerKey; }
	
	public int getBytesPerValue() { return this.bytesPerValue; }
		
	public DataType[] getSchema() { return this.schema; }
	
	public int[] getKeyColumns() { return this.keyColumns; }
	
	public DataType[] getKeyColumnTypes() { return this.keyColumnTypes; }
		
	protected void initialize() {
		this.bytesPerKey = 0;
		this.keyColumnTypes = new DataType[this.keyColumns.length];
		
		// find size of key
		for (int i = 0; i < this.keyColumns.length; i++) {
			this.keyColumnTypes[i] = this.schema[this.keyColumns[i]];
			this.bytesPerKey += this.keyColumnTypes[i].getNumberOfBytes();										
		}
	
		// find size of the tuple		
		this.bytesPerValue = 0;
		for (int i = 0; i < this.schema.length; i++)
			this.bytesPerValue += this.schema[i].getNumberOfBytes();
		
		// now subtract key's # of bytes from the calculation above
		this.bytesPerValue -= this.bytesPerKey;
	
		// find value columns
		this.valueColumns = new int[this.schema.length - this.keyColumns.length];
		this.valueColumnTypes = new DataType[this.schema.length - this.keyColumns.length];
		
		int count = 0;
		for (int i = 0; i < this.schema.length; i++) {
			boolean found = false;
			for (int j = 0; j < this.keyColumns.length; j++) {
				if (i == keyColumns[j]) {
					found = true;
					break;
				}
			}
			if (!found) {
				this.valueColumnTypes[count] = this.schema[i];
				this.valueColumns[count++] = i;
			}
		}
		this.templateTuple = this.getEmptyTuple();
	}
	
	public byte[] getKey(DbTypeBase[] columns) {
		byte[] bytes = new byte[this.bytesPerKey];
		int offset = 0;
		for (int i = 0; i < this.keyColumnTypes.length; i++)
			offset = columns[this.keyColumns[i]].getBytes(bytes, offset);
		
		return bytes; 
	}
	
	public int getKeyI(DbTypeBase[] columns) {
		return ((EncodedType)columns[this.keyColumns[0]]).getKey();
	}
	
	public long getKeyL(DbTypeBase[] columns) {
		if (this.keyColumns.length == 2) {
			long keyPart1 = ((EncodedType)columns[this.keyColumns[0]]).getKey();
			long keyPart2 = ((EncodedType)columns[this.keyColumns[1]]).getKey();
			return (keyPart1 << 32) | (keyPart2 & 0xffffffffL);
		}
		
		return ((BigEncodedType)columns[this.keyColumns[0]]).getKeyL();
	}
	
	public int loadTuple(byte[] key, Tuple tuple) {
		int offset = 0;
		for (int i = 0; i < this.keyColumns.length; i++) {
			tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], key, offset, this.typeManager);
			offset += this.schema[i].getNumberOfBytes();
		}
		return 1;
	}
	
	public int loadTuple(int key, byte[] rowData, Tuple tuple) {
		if (rowData == null)
			return 0;
		
		byte[] keyBytes = ByteArrayHelper.getIntAsBytes(key);		
		return this.loadTuple(keyBytes, rowData, tuple);
	}
	
	public int loadTuple(long key, byte[] rowData, Tuple tuple) {
		if (rowData == null)
			return 0;
				
		byte[] keyBytes = ByteArrayHelper.getLongAsBytes(key);		
		return this.loadTuple(keyBytes, rowData, tuple);
	}
	
	public int loadTuple(byte[] key, byte[] rowData, Tuple tuple) {
		if (rowData == null)
			return 0;
		
		int offset = 0;
		for (int i = 0; i < this.keyColumns.length; i++) {
			tuple.columns[this.keyColumns[i]] = DbTypeBase.loadFrom(this.keyColumnTypes[i], key, offset, this.typeManager);
			offset += this.keyColumnTypes[i].getNumberOfBytes();
		}
		
		offset = 0;
		for (int i = 0; i < this.valueColumns.length; i++) {
			tuple.columns[this.valueColumns[i]] = DbTypeBase.loadFrom(this.valueColumnTypes[i], rowData, offset, this.typeManager);
			offset += this.valueColumnTypes[i].getNumberOfBytes();
		}
		
		return 1;
	}
	
	public int loadTuple(DbTypeBase[] keyColumns, byte[] rowData, Tuple tuple) {
		if (rowData == null)
			return 0;
		
		for (int i = 0; i < this.keyColumns.length; i++)
			tuple.columns[this.keyColumns[i]] = keyColumns[i];
		
		int offset = 0;
		for (int i = 0; i < this.valueColumns.length; i++) {
			tuple.columns[this.valueColumns[i]] = DbTypeBase.loadFrom(this.valueColumnTypes[i], rowData, offset, this.typeManager);
			offset += this.valueColumnTypes[i].getNumberOfBytes();
		}
		
		return 1;
	}
	
	public int loadTuple(int key, int subKey, int value, Tuple tuple) {
		if (this.keyColumnTypes[0] == DataType.INT)
			tuple.columns[this.keyColumns[0]] = DbInteger.create(key);
		else
			tuple.columns[this.keyColumns[0]] = DbString.load(key, this.typeManager);
		if (this.keyColumnTypes[1] == DataType.INT)
			tuple.columns[this.keyColumns[1]] = DbInteger.create(subKey);
		else
			tuple.columns[this.keyColumns[1]] = DbString.load(subKey, this.typeManager);
		
		tuple.columns[this.valueColumns[0]] = DbInteger.create(value);
		return 1;
	}
	
	// b+ tree are sorted by key - this is enough for now 
	@Override
	public boolean sort() { return true; }
	
	@Override
	public Tuple getEmptyTuple() {
		Tuple tuple = new Tuple(this.schema.length);
		for (int i = 0; i < this.schema.length; i++) {
			// APS 7/16/2014 using loadFrom and defaulting all values to 0
			tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], 0);
		}
		
		return tuple;
	}
	
	@Override
	public Tuple exists(Tuple tuple) {
		if (this.getTuple(tuple.columns, this.templateTuple) > 0)
			return this.templateTuple;
		return null;
	}
	
	abstract public int getTuple(DbTypeBase[] keyColumns, Tuple tuple);
	
	abstract public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor);
	
	abstract public void remove(Tuple tuple);
	
}
