package edu.ucla.cs.wis.bigdatalog.database.store.tuple;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysonly.BPlusTreeByteKeysOnlyLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleArrayStore 
	extends AddressedTupleStore 
	implements Serializable {
	//private static Logger logger = LoggerFactory.getLogger(TupleArrayStore.class.getName());
	
	private static final long serialVersionUID = 1L;
	
	protected DataType[] schema;
	protected ArrayList<AddressedTuple> arrayList;
	
	public TupleArrayStore() { super(); }
	
	public TupleArrayStore(String relationName, DataType[] schema, TypeManager typeManager) {
		super(relationName, typeManager);
		this.schema = schema;
		this.initialize();
	}
	
	public DataType[] getSchema() { return this.schema; }
	
	private void initialize() {
		this.arrayList = new ArrayList<>();
	}
	
	@Override
	public void add(AddressedTuple tuple) {
		// must copy tuple to sever reference with original tuple
		AddressedTuple copy = tuple.copy();
		copy.address = this.arrayList.size();
		// set the address in the input tuple since it should be -1
		// this way, the input tuple can be used to insert the address into a secondary index
		tuple.address = copy.address; 
		this.arrayList.add(copy);
	}
	
	@Override
	public void update(AddressedTuple tuple) {
		Tuple tupleToUpdate = this.arrayList.get(tuple.address);

		for (int i = 0; i < tupleToUpdate.columns.length; i++)
			tupleToUpdate.setColumn(i, tuple.columns[i]);
	}

	@Override
	public int get(int address, AddressedTuple tuple) {	
		AddressedTuple temp = this.arrayList.get(address);
		if (temp.isDeleted == 1)
			return 0;

		for (int i = 0; i < temp.columns.length; i++)
			tuple.columns[i] = temp.columns[i];
		
		tuple.address = temp.address;
		
		return 1;
	}
	
	@Override
	public void remove(AddressedTuple tuple) {
		this.arrayList.get(tuple.address).setDeleted();
	}
	
	@Override
	public void remove(int address) {
		if (address >= this.arrayList.size())
			return;
		this.arrayList.get(address).setDeleted();
	}
	
	@Override
	public void removeAll() {
		this.initialize();
	}
	
	@Override
	public int commit() {
		int numberOfTuplesDeleted = 0;
		for (int i = this.arrayList.size()-1; i >= 0; i--) {
			if (this.arrayList.get(i).isDeleted()) {
				this.arrayList.remove(i);
				numberOfTuplesDeleted++;
			}
		}
		
		for (int i = 0; i < this.arrayList.size(); i++)
			this.arrayList.get(i).address = i;
				
		return numberOfTuplesDeleted;
	}
	
	@Override
	public AddressedTuple exists(AddressedTuple tuple) {
		boolean found = true;

		for (AddressedTuple temp : this.arrayList) {
			for (int j = 0; j < tuple.columns.length; j++) {
				if (!tuple.columns[j].equals(temp.columns[j])) {
					found = false;
					break;
				}
			}
			
			if (found && !temp.isDeleted()) {
				return temp;
			}
			
			found = true;
		}
		return null;
	}
	
	@Override
	public int getFirstTupleAddress() {
		if (this.arrayList.size() == 0)
			return -1;
		return 0;
	}
	
	@Override
	public int getLastTupleAddress() {
		return this.arrayList.size() - 1;
	}	
	
	@Override
	public int getNumberOfTuples() {
		return this.arrayList.size();
	}
	
	@Override
	public String toString() {
		StringBuilder retval = new StringBuilder();
		if (this.arrayList == null) {
			retval.append("\nRelation is empty");
			return retval.toString();
		}
		 
		for (Tuple tuple : this.arrayList) {
			retval.append("\n");
			retval.append(((AddressedTuple)tuple).toString(true));
		}
		return retval.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		MemoryMeasurement mm;
		int used = 0;
		int allocated = 0;
		
		for (Tuple tuple : this.arrayList) {
			mm = tuple.getSizeOf();
			used += mm.getUsed();
			allocated += mm.getAllocated();			
		}
		return new MemoryMeasurement(used, allocated);  
	}
	
	@Override
	public boolean sort() { 
		// To sort this arraylist, we:
		//   - load a bplustree with arraylist entries
		//   - clear this arraylist
		//   - load it again from the bplustree
		// 1) declare BPlusTree keyed on all columns
		// 2) load BPlusTree with all tuples
		// 3) removeAll from this arraylist
		// 4) load all from bplustree back into this arraylist
		//int nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.tuplestores.bplustree.nodesize"));
		
		// need schema to order keys properly
		if (this.schema == null)
			return false;
		
		int bytesPerTuple = 0;
		for (DataType dataType : this.schema)
			bytesPerTuple += dataType.getNumberOfBytes();

		BPlusTreeByteKeysOnly bPlusTree = new BPlusTreeByteKeysOnly(256, bytesPerTuple, this.schema, new int[this.schema.length], /*
				this.deALSConfiguration, */this.typeManager); 
 		
		AddressedTuple tuple;
		byte[] tupleData;
		for (int i = 0; i < this.arrayList.size(); i++) {
			tuple = this.arrayList.get(i); 
			if (tuple != null) {
				tupleData  = new byte[bytesPerTuple];
				int offset = 0;
				for (int j = 0; j < tuple.columns.length; j++)
					offset = tuple.columns[j].getBytes(tupleData, offset);

				bPlusTree.insert(tupleData);
			}
		}
		
		if (bPlusTree.getNumberOfEntries() == this.getNumberOfTuples()) {
			this.removeAll();
			
			BPlusTreeLeaf<?> currentBPlusTreeLeaf = bPlusTree.getFirstChild();
			int keyIndex = 0;
		
			// get leaf
			// get keys
			// when out of keys, get next leaf
			// when out of leaves, done			
			while (currentBPlusTreeLeaf != null) {
				while (keyIndex < currentBPlusTreeLeaf.getHighWaterMark()) {
					tupleData = new byte[bytesPerTuple];
					System.arraycopy(((BPlusTreeByteKeysOnlyLeaf) currentBPlusTreeLeaf).getKeys(), keyIndex++ * bytesPerTuple, tupleData, 0, bytesPerTuple);
					// create tuple from byte array
					int offset = 0;
					tuple = new AddressedTuple(this.schema.length);
					for (int i = 0; i < this.schema.length; i++) {
						tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], tupleData, offset, this.typeManager);
						offset += this.schema[i].getNumberOfBytes();
					}
					this.add(tuple);
				}
				
				// out of keys, so move to next leaf
				currentBPlusTreeLeaf = currentBPlusTreeLeaf.getNext();
				keyIndex = 0;
			}
		}
		// cleanup
		bPlusTree.deleteAll();
		
		return true;
	}
	
	@Override
	public AddressedTuple getEmptyTuple() {
		AddressedTuple tuple = new AddressedTuple(this.schema.length);
		for (int i = 0; i < this.schema.length; i++) {
			// APS 7/16/2014 using loadFrom and defaulting all values to 0
			tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], 0);
		}
		
		return tuple;
	}
}
