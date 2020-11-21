package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class ChangeTracker implements Serializable {
	private static final long serialVersionUID = 1L;
		
	abstract public void add(long key);
	
	abstract public void add(int key);
	
	abstract public void add(byte[] key);
	
	abstract public int getNumberOfEntries();
	
	abstract public void clear();
	
	abstract public void delete();
	
	public static ChangeTracker getChangeTracker(int[] keyColumns, DataType[] keyColumnTypes, DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		
		switch (ChangeTracker.getChangeTrackerType(keyColumns, keyColumnTypes, deALSConfiguration)) {
			case 1:
				return new IntBitmapChangeTracker();
			case 2: {
				int nodeSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.store.changetracking.intkeys.nodesize"));
				return new IntTreeChangeTracker(keyColumns, keyColumnTypes, nodeSize);
			} case 3: {
				int nodeSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.store.changetracking.longkeys.nodesize"));
				return new LongTreeChangeTracker(keyColumns, keyColumnTypes, nodeSize);
			} case 4: {
				int nodeSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.store.changetracking.intkeysbitmapvalues.nodesize"));
				return new IntTreeIntBitmapChangeTracker(keyColumns, keyColumnTypes, nodeSize, typeManager);
			} default: {
				int nodeSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.store.changetracking.generalkeys.nodesize"));
				return new GeneralChangeTracker(keyColumns, keyColumnTypes, nodeSize);
			}
		}
	}
		
	public static int getChangeTrackerType(int[] keyColumns, DataType[] keyColumnTypes, DeALSConfiguration deALSConfiguration) {
		if (keyColumns.length == 1 && keyColumnTypes[0].getNumberOfBytes() == 4) {
			if (deALSConfiguration.compareProperty("deals.database.store.changetracking.intkeys.usebitmap", "true"))
				return 1;
			
			return 2;
		} else if ((keyColumns.length == 1) && (keyColumnTypes[0].getNumberOfBytes() == 8)) {
			return 3;
		} else if ((keyColumns.length == 2) 
				&& ((keyColumnTypes[0].getNumberOfBytes() == 4) 
						&& (keyColumnTypes[1].getNumberOfBytes()) == 4)) {
			if (deALSConfiguration.compareProperty("deals.database.store.changetracking.longkeys.usebitmap", "true"))
				return 4;

			return 3;
		}
		return 5;
	}
}