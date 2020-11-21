package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class FilteredScanCursor<T extends Tuple> 
	extends Cursor<T> 
	implements SelectionCursor<T> {
	protected Cursor<T>		scanCursor;
	protected int[] 		filteredColumns;
	protected DbTypeBase[]	filteredColumnValues;
	  
	protected FilteredScanCursor(Relation<T> relation, int[] filteredColumns) {
		super(relation);

		if (filteredColumns != null) {
			this.filteredColumns = new int[filteredColumns.length];
			this.filteredColumnValues = new DbTypeBase[filteredColumns.length];

			for (int i = 0; i < filteredColumns.length; i++)
				this.filteredColumns[i] = filteredColumns[i];
		}
		// the underlying cursor with the values from the relation
		this.scanCursor = (Cursor<T>) this.relation.getDatabase().getCursorManager().createScanCursor(this.relation);
	}
	
	public int[] getFilterColumns() { return this.filteredColumns; }
	
	public DbTypeBase[] getFilter() { return this.filteredColumnValues; }

	public void reset() {
		this.setFilterValues(null);
		this.scanCursor.reset();
	}
	
	public void reset(DbTypeBase[] indexColumnValues) {
	    this.setFilterValues(indexColumnValues);
	    this.scanCursor.reset();	    
	}

	public void setFilterValues(DbTypeBase[] indexColumnValues) {
		if (this.filteredColumns == null)
			return;
		
		switch (this.filteredColumns.length) {
			case 5:
				this.filteredColumnValues[4] = indexColumnValues[4];
			case 4:
				this.filteredColumnValues[3] = indexColumnValues[3];
			case 3:
				this.filteredColumnValues[2] = indexColumnValues[2];
			case 2:
				this.filteredColumnValues[1] = indexColumnValues[1];
			case 1:
				this.filteredColumnValues[0] = indexColumnValues[0];
				break;
			default: {
				for (int i = 0; i < this.filteredColumnValues.length; i++)
					this.filteredColumnValues[i] = indexColumnValues[i];		
			}
		}	
	}

	protected boolean matchTuple(DbTypeBase[] columns) {
		if (this.filteredColumns == null)
			return true;
		
		for (int i = 0; i < this.filteredColumns.length; i++)
			if (!columns[this.filteredColumns[i]].equals(this.filteredColumnValues[i]))
				return false;					
		
		return true;
	}

	@Override
	public int getTuple(T tuple) {
	    while (this.scanCursor.getTuple(tuple) > 0) {
	    	if (this.matchTuple(tuple.columns))
	    		return 1;
	    }
	    return 0;
	}
	
	@Override
	public void moveNext() { this.scanCursor.moveNext(); }

	@Override
	public T getEmptyTuple() { return this.scanCursor.getEmptyTuple(); }	
}
