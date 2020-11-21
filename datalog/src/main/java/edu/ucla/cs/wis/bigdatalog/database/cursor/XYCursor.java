package edu.ucla.cs.wis.bigdatalog.database.cursor;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.RelationManagerException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class XYCursor extends NaiveCursor {
	private static Logger logger = LoggerFactory.getLogger(XYCursor.class.getName());

	public 		int 			XYStage;
	public 		int 			siblingXYStage;   
	protected  int 			siblingBaseTupleAddress;
	protected  int 			siblingEndTupleAddress;
	protected	IndexCursor 	indexCursor;
	protected	AddressedTuple 	tuple;
	protected 	DeALSContext	deALSContext;

	protected XYCursor(Relation<AddressedTuple> relation, int[] filteredColumns, DeALSContext deALSContext,
			Database database) {
		super(relation, filteredColumns);
		this.XYStage = 0;
		this.siblingXYStage = -1;

		this.siblingBaseTupleAddress = -1;
		this.siblingEndTupleAddress = -1;
		
		if (filteredColumns != null)
			this.indexCursor = (IndexCursor) database.getCursorManager().createIndexCursor(relation, filteredColumns);
		this.tuple = this.relation.getEmptyTuple();
		this.deALSContext = deALSContext;
	}

	public IndexCursor getIndexCursor() { return this.indexCursor; }
	
	public void initialize() {
		this.siblingBaseTupleAddress = -1;
		this.siblingEndTupleAddress = -1;

		super.initialize();
	}

	public void reset(DbTypeBase[] indexColumnValues) {
		if (this.indexCursor != null)
			this.indexCursor.reset(indexColumnValues);
		else
			super.reset(indexColumnValues);
	}
	
	@Override
	public int getTuple(AddressedTuple tuple) {
		if (this.XYStage > this.siblingXYStage)
			this.expandCurrentPhase();
		
		// TODO - APS 11/21/2013 - XY runs really slow because of this scan
		// but IndexCursor has a problem with deletes.  Fix later
		if ((this.indexCursor == null) 
				|| (this.filteredColumns.length != this.indexCursor.filteredColumns.length))
			return super.getTuple(tuple);
		
		return this.indexCursor.getTuple(tuple);		
	}
	
	public void refresh() {
		if (this.indexCursor != null)
			this.indexCursor.refreshXY();
	}

	protected void expandCurrentPhase() {
		if (DEBUG) {
			this.deALSContext.logTrace(logger, "Entering expandCurrentPhase");
			this.deALSContext.logDebug(logger, "{}", this);
		}
				//System.out.print(" expand ");
		// -1 is the same as null
		if (this.baseTupleAddress == -1) {
			if (this.siblingBaseTupleAddress == -1) {
				this.baseTupleAddress = this.tupleStore.getFirstTupleAddress();
			} else {
				//System.out.println("@" + this.siblingEndTupleAddress);
				/*if ((this.siblingEndTupleAddress + 1) <= this.tupleStore.getLastTupleAddress())
					this.baseTupleAddress = this.siblingEndTupleAddress + 1;
				else
					this.baseTupleAddress = -1;*/
				if (this.getNextTuple(this.siblingEndTupleAddress, this.tuple) > 0)
					this.baseTupleAddress = tuple.address;
				
				/*
				Tuple tuple = this.getNextTuple(this.siblingEndTupleAddress);
				if (tuple != null) {
					this.baseTupleAddress = tuple.address;
					System.out.println("found @" + tuple.address);
				}*/
			}
			
			if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0)
				this.currentTupleAddress = this.baseTupleAddress;
			else 
				this.currentTupleAddress = -1;
			/*
			if (this.baseTupleAddress >= 0 && this.baseTupleAddress <= this.tupleStore.getLastTupleAddress())
				this.currentTupleAddress = this.baseTupleAddress;*/
		} else {
			/*if (this.tuple == null) {
				this.tuple = this.getNextTuple(this.endTupleAddress);
			}*/
			if (this.currentTupleAddress == -1) {
				if (this.getNextTuple(this.endTupleAddress, this.tuple) > 0)
					this.currentTupleAddress = this.tuple.address;
			}
		}
		
		if (this.baseTupleAddress != -1)
			this.endTupleAddress = this.tupleStore.getLastTupleAddress();
				
		if (DEBUG) {
			this.deALSContext.logDebug(logger, "{}", this);		
			this.deALSContext.logTrace(logger, "Exiting expandCurrentPhase");
		}
	}

	@Override
	public void beginNextStage(DeALSContext deALSContext, int stageId) {
		this.deALSContext = deALSContext;
		//this.timesCalled[4]++;
		//long start = System.nanoTime();	
		if (DEBUG) {
			this.deALSContext.logTrace(logger, "Entering beginNextPhase");		
			this.deALSContext.logDebug(logger, "{}", this);
		}
				
		if (this.siblingBaseTupleAddress == -1) {
			super.beginNextStage(this.deALSContext, stageId);
		} else {
			if (this.getNextTuple(this.siblingEndTupleAddress, this.tuple) == 0) {
				this.baseTupleAddress = -1;
			} else {
				this.baseTupleAddress = this.tuple.address;
			}
			/*if ((this.siblingEndTupleAddress + 1) <= this.tupleStore.getLastTupleAddress())
				this.baseTupleAddress = this.siblingEndTupleAddress + 1;
			else
				this.baseTupleAddress = -1;*/
			this.endTupleAddress = this.tupleStore.getLastTupleAddress();
			if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0)
				this.currentTupleAddress = this.tuple.address;
			else
				this.currentTupleAddress = -1;
			/*if (this.baseTupleAddress >= 0 && this.baseTupleAddress <= this.tupleStore.getLastTupleAddress())
				this.currentTupleAddress = this.baseTupleAddress;
				*/
			this.XYStage = this.siblingXYStage + 1;
		}
		
		if (DEBUG) {
			this.deALSContext.logDebug(logger, "{}", this);
			this.deALSContext.logTrace(logger, "Exiting beginNextPhase");
		}
		
		//this.timeSpent[4] += System.nanoTime() - start;
	}

	public int deleteTuplesInStage(int cliqueStage) {
		if (DEBUG)
			this.deALSContext.logTrace(logger, "Entering deleteTuplesInStage for cliqueStage = {}", cliqueStage);
		
		int numberDeleted = 0;
		
		if (this.baseTupleAddress == this.siblingBaseTupleAddress)
			numberDeleted = 0;
		
		if ((this.siblingBaseTupleAddress != -1) && (this.siblingXYStage == cliqueStage))
			numberDeleted = this.removeRange(this.siblingBaseTupleAddress, this.siblingEndTupleAddress);

		if ((this.baseTupleAddress != -1) && (this.XYStage == cliqueStage))
			numberDeleted = this.removeRange(this.baseTupleAddress, this.endTupleAddress);
		
		if (DEBUG)
			this.deALSContext.logTrace(logger, "Exiting deleteTuplesInStage with numberDeleted = {}", numberDeleted);
		
		return numberDeleted;
	}
	
	private int removeRange(int startAddress, int endAddress) {
		AddressedTuple tuple = (AddressedTuple) this.tupleStore.getEmptyTuple();
		int numberDeleted = 0;
		while(startAddress <= endAddress) {
			if (this.tupleStore.get(startAddress++, tuple) > 0) {
				//this.tupleStore.remove(startAddress++);
				this.relation.remove(tuple);
				numberDeleted++;
			}
		}

		//int numberDeleted = this.tupleStore.commit();
		//int numberDeleted = count;
		//System.out.println("delete " + numberDeleted + " tuples");
		// adjust the indices only if tuples have been deleted
		if (numberDeleted > 0) {
			//this.relation.rebuildIndexes();
			if (this.indexCursor != null)
				this.indexCursor.reset(this.indexCursor.filteredColumnValues);
		}

		return numberDeleted;
	}
	
	public boolean isDuplicateTupleInSameStage(Tuple tuple) {	
		if (DEBUG)
			this.deALSContext.logTrace(logger, "Entering isDuplicateTupleInSameStage for tuple = {}", tuple.toString());
		
		if (this.XYStage <= this.siblingXYStage)
			throw new RelationManagerException("Duplicate tuple in same stage.  Wrong stage. XYStage:" + this.XYStage + " siblingXYStage:" + this.siblingXYStage);

		this.expandCurrentPhase();

		//tuple.columns[tuple.getArity() - 1] = DbInteger.create(this.XYStage);
		if (this.relation.getUniqueIndex().exists(tuple)) {
			if (DEBUG)
				this.deALSContext.logTrace(logger, "Exiting isDuplicateTupleInSameStage with duplicateFound = true");
			return true;
		}
		
		if (DEBUG)
			this.deALSContext.logTrace(logger, "Exiting isDuplicateTupleInSameStage with duplicateFound = false");
		
		return false;
	}

	protected int switchCounter = 0;
	public void switchCursor() {
		//if (DEBUG && Runtime.isTraceEnabled())
			//this.deALSContext.logTrace(logger, "Entering switchCursor");
		
		//if (DEBUG && Runtime.isDebugEnabled())
			//logger.debug(this.toString());
				
			//this.switchCounter++;
			//System.out.println(this.relation.getName() + " " + this.switchCounter);
			
		int temp = this.baseTupleAddress;
		this.baseTupleAddress = this.siblingBaseTupleAddress;
		this.siblingBaseTupleAddress = temp;
		
		temp = this.endTupleAddress;
		this.endTupleAddress = this.siblingEndTupleAddress;
		this.siblingEndTupleAddress = temp;
		
		int temp2 = this.XYStage;
		this.XYStage = this.siblingXYStage;
		this.siblingXYStage = temp2;

		if (this.siblingBaseTupleAddress == -1 && this.siblingEndTupleAddress > -1)
			System.out.println("mismatch");

		if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0)
			this.currentTupleAddress = this.tuple.address;
		else
			this.currentTupleAddress = -1;
		/*if (this.baseTupleAddress <= this.tupleStore.getLastTupleAddress())
			this.currentTupleAddress = this.baseTupleAddress;
		else
			this.currentTupleAddress = -1;*/
		
		//if (DEBUG && Runtime.isDebugEnabled())
			//logger.debug(this.toString());
		
		//if (DEBUG && Runtime.isTraceEnabled())
			//this.deALSContext.logTrace(logger, "Exiting switchCursor");		
	}

	public void beginYPhaseForXYLiteral(XYPredicateType xyPredicateType, int cliqueStage) {
		if (DEBUG) {
			this.deALSContext.logTrace(logger, "Entering beginYPhaseForXYLiteral for xyPredicateType = {}, cliqueStage = {}", xyPredicateType, cliqueStage);
			this.deALSContext.logDebug(logger, "{}", this);
		}
				
		if (xyPredicateType == XYPredicateType.NEW) {
			this.siblingBaseTupleAddress = this.tupleStore.getFirstTupleAddress();
			this.siblingEndTupleAddress = this.tupleStore.getLastTupleAddress();

			this.XYStage = cliqueStage;
			this.siblingXYStage = cliqueStage - 1;

			this.baseTupleAddress = -1;
			this.endTupleAddress = -1;
		} else {
			// OLD
			this.baseTupleAddress = this.tupleStore.getFirstTupleAddress();
			this.endTupleAddress = this.tupleStore.getLastTupleAddress();

			this.XYStage = cliqueStage - 1;
			this.siblingXYStage = cliqueStage;

			this.siblingBaseTupleAddress = -1;
			this.siblingEndTupleAddress = -1;
		}
		
		if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0) {
			this.currentTupleAddress = this.tuple.address;
		} else {
			this.currentTupleAddress = -1;
			this.baseTupleAddress = -1;
		}
		/*if (this.baseTupleAddress >= 0 && this.baseTupleAddress <= this.tupleStore.getLastTupleAddress())
			this.currentTupleAddress = this.baseTupleAddress;
			*/
		// APS added 8/1/2013 - trying to fix xy final rules
		/*if (this.tuple == null)
			this.baseTupleAddress = -1;
		else
			this.currentTupleAddress = this.baseTupleAddress;*/
				
		// indexcursor not being reset properly when has cached values
		/*if (this.indexCursor != null) {
			if (this.indexCursor.possibleMatchAddresses != null) {
				for (int i = this.indexCursor.possibleMatchAddresses.length - 1; i >= 0; i--) {
					//if (this.indexCursor.possibleMatchAddresses[i] > this.)
				}
			}
			this.indexCursor.reset(this.indexCursor.filteredColumnValues);
		}*/
		
		if (this.siblingBaseTupleAddress == -1 && this.siblingEndTupleAddress > -1)
			System.out.println("mismatch");
		
		if (DEBUG) {
			this.deALSContext.logDebug(logger, "{}", this);
			this.deALSContext.logTrace(logger, "Exiting beginYPhaseForXYLiteral for xyPredicateType = {}, cliqueStage = {}", xyPredicateType, cliqueStage);
		}
	}

	public void beginYPhaseForXYOrNode(XYPredicateType xyPredicateType, int cliqueStage) {
		if (DEBUG) {
			this.deALSContext.logTrace(logger, "Entering beginYPhaseForXYOrNode for xyPredicateType = {}, cliqueStage = {}", xyPredicateType, cliqueStage);
			this.deALSContext.logDebug(logger, "{}", this);
		}
		
		 /* NEW_XY_NODE or OLD_XY_NODE but need to reset this.tuple */
		if ((xyPredicateType == XYPredicateType.NEW) || 
				((this.baseTupleAddress == -1) || (this.XYStage < (cliqueStage - 1)))) {
			this.beginYPhaseForXYLiteral(xyPredicateType, cliqueStage);
			return;
		}

		int newEndTupleAddress = this.tupleStore.getLastTupleAddress();
		
		//if ((this.tuple == null) && (newEndTupleAddress != this.endTupleAddress))
		//	this.tuple = this.getNextTuple(this.endTupleAddress);
		/*if ((this.currentTupleAddress == -1) && (newEndTupleAddress != this.endTupleAddress))
			this.currentTupleAddress = this.endTupleAddress + 1;
			*/
		if ((this.currentTupleAddress == -1) && (newEndTupleAddress != this.endTupleAddress)) {
			if (this.getNextTuple(this.endTupleAddress, this.tuple) > 0)
				this.currentTupleAddress = this.tuple.address;
			else
				this.currentTupleAddress = -1;				
		}

		// APS added 3/11/2013 - this adjusts the address downard after deletes
		//this.baseTupleAddress -= (this.endTupleAddress - newEndTupleAddress);
		this.baseTupleAddress = this.endTupleAddress; 
		this.endTupleAddress = newEndTupleAddress;

		this.XYStage = cliqueStage - 1;		
		this.siblingXYStage = cliqueStage;

		this.siblingBaseTupleAddress = -1;
		this.siblingEndTupleAddress = -1;
		
		/*if (this.indexCursor != null) {
			this.indexCursor.reset(this.indexCursor.filteredColumnValues);
		}*/
		//this.currentTupleAddress = this.baseTupleAddress;					

		if (this.siblingBaseTupleAddress == -1 && this.siblingEndTupleAddress > -1)
			System.out.println("mismatch");
		
		if (DEBUG) {
			this.deALSContext.logDebug(logger, "{}", this);
			this.deALSContext.logTrace(logger, "Entering beginYPhaseForXYOrNode for xyPredicateType = {}, cliqueStage = {}", xyPredicateType, cliqueStage);
		}
	}

	public void resetCursorAfterCopyRule(XYPredicateType xyPredicateType) {
		if (DEBUG) {
			this.deALSContext.logTrace(logger, "Entering resetCursorAfterCopyRule for xyPredicateType = {}", xyPredicateType);
			this.deALSContext.logDebug(logger, "{}", this);
		}
				
		if (xyPredicateType == XYPredicateType.NEW) {
			this.baseTupleAddress = this.siblingBaseTupleAddress;
			this.endTupleAddress = this.siblingEndTupleAddress;
		} else {
			this.siblingBaseTupleAddress = this.baseTupleAddress;
			this.siblingEndTupleAddress = this.endTupleAddress;
		}
		
		//this.tuple = this.tupleStore.get(this.baseTupleAddress);
		//if (this.baseTupleAddress >= 0 && this.baseTupleAddress <= this.tupleStore.getLastTupleAddress())
		//	this.currentTupleAddress = this.baseTupleAddress;
		if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0) {
			this.currentTupleAddress = this.tuple.address;
		} else {
			this.currentTupleAddress = -1;
			this.baseTupleAddress = -1;
		}
		
		/*if (this.tuple == null)
			this.baseTupleAddress = -1;	// APS added 8/1/2013 - trying to fix xy final rules
		else
			this.currentTupleAddress = this.baseTupleAddress;*/					

		if (DEBUG) {
			this.deALSContext.logDebug(logger, "{}", this);
			this.deALSContext.logTrace(logger, "Exiting resetCursorAfterCopyRule for xyPredicateType = {}", xyPredicateType);
		}
	}

	public void resetCursorAfterDeleteRule(XYPredicateType xyPredicateType) {
		if (DEBUG) {
			this.deALSContext.logTrace(logger, "Entering resetCursorAfterDeleteRule for xyPredicateType = {}", xyPredicateType);
			this.deALSContext.logDebug(logger, "{}", this);
		}
		
		//System.out.println(this.tupleStore.toString());
		
		if (xyPredicateType == XYPredicateType.NEW) {
			this.siblingBaseTupleAddress = -1;
			this.siblingEndTupleAddress = -1;
			this.baseTupleAddress = this.tupleStore.getFirstTupleAddress();
			this.endTupleAddress = this.tupleStore.getLastTupleAddress();			
		} else {
			this.baseTupleAddress = -1;
			this.endTupleAddress = -1;
			this.siblingBaseTupleAddress = this.tupleStore.getFirstTupleAddress();
			this.siblingEndTupleAddress = this.tupleStore.getLastTupleAddress();
		} 

		if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0) {
			this.currentTupleAddress = this.tuple.address;
		} else {
			this.baseTupleAddress = -1;
			this.currentTupleAddress = -1;
		}
		//this.tuple = this.tupleStore.get(this.baseTupleAddress);
		//if (this.baseTupleAddress >= 0 && this.baseTupleAddress <= this.tupleStore.getLastTupleAddress())
		//	this.currentTupleAddress = this.baseTupleAddress;
						
		// APS added 8/1/2013 to attempt to fix final xy rules
		/*if (this.tuple == null)
			this.baseTupleAddress = -1;
		else
			this.currentTupleAddress = this.baseTupleAddress;*/
		
		/*if (this.indexCursor != null) {
			if (this.indexCursor.possibleMatchAddresses != null) {
				System.out.println(Arrays.toString(this.indexCursor.possibleMatchAddresses));
				for (int i = this.indexCursor.possibleMatchAddresses.length - 1; i >= 0; i--) {
					//if (this.indexCursor.possibleMatchAddresses[i] > this.)
				}
			}
			//this.indexCursor.reset(this.indexCursor.filteredColumnValues);
		}*/
		
		if (DEBUG) {
			this.deALSContext.logDebug(logger, "{}", this);		
			this.deALSContext.logTrace(logger, "Exiting resetCursorAfterDeleteRule for xyPredicateType = {}", xyPredicateType);
		}
	}
	
	@Override
	public int getIterationSize() {
		// this only works after beginYPhaseForXYLiteral() is called
		if (this.siblingBaseTupleAddress == -1 && this.siblingBaseTupleAddress == -1)
			return 0;
		
		return (this.siblingEndTupleAddress - this.siblingBaseTupleAddress) + 1;
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(super.toString());
		output.append("\nthis.XYStage | this.siblingXYStage [" + this.XYStage + " | " + this.siblingXYStage + "]");
		output.append("\nthis.siblingBaseTupleAddress | this.siblingEndTupleAddress [" + this.siblingBaseTupleAddress + " | " + this.siblingEndTupleAddress + "]");
		return output.toString();
	}
	
	// gets the tuple at or after location [address + 1]
	protected int getNextTuple(int address, AddressedTuple tuple) {
		//Tuple tuple = null;
		//int count = 0;
		while (((this.tupleStore.get(++address, tuple)) == 0)
				&& address <= this.tupleStore.getLastTupleAddress()) {
			/*System.out.println(count++);*/}
		
		if (address <= this.tupleStore.getLastTupleAddress())
			return 1;
		return 0;
	}		
}
