package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.raw.BPlusTreeByteKeysOnlyCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeByteKeysOnly 
	extends BPlusTreeStoreStructure<BPlusTreeByteKeysOnlyPage, BPlusTreeByteKeysOnlyLeaf, BPlusTreeByteKeysOnlyNode>
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeByteKeysOnlyInsertResult insertResult;
	
	protected BPlusTreeByteKeysOnly(){ super(); }
	
	public BPlusTreeByteKeysOnly(int nodeSize, int bytesPerKey, DataType[] keyColumnTypes) {
		super(nodeSize, bytesPerKey, BPlusTreeStoreStructure.getKeyColumns(keyColumnTypes), keyColumnTypes);
		this.insertResult = new BPlusTreeByteKeysOnlyInsertResult();
		this.initialize();
	}

	@Override
	protected BPlusTreeByteKeysOnlyPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysOnlyLeaf(this.nodeSize, this.bytesPerKey);
		
		return new BPlusTreeByteKeysOnlyNode(this.nodeSize, this.bytesPerKey);
	}
		
	public void insert(byte[] key) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeByteKeysOnlyNode newRoot = (BPlusTreeByteKeysOnlyNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public byte[] get(byte[] key) {
		if (this.rootNode.isEmpty())
			return null;
		
		return this.rootNode.get(key);
	}
	
	public boolean delete(byte[] key) {
		if (this.rootNode.isEmpty())
			return false;
		
		boolean status = this.rootNode.delete(key);
		if (status) {
			this.numberOfEntries--;
			if (this.numberOfEntries == 0) {
				this.rootNode = null;
				this.rootNode = this.allocatePage();
			}
		}
		return status;
	}	
	
	public void merge(BPlusTreeByteKeysOnly other, boolean print) {
		if (other.isEmpty())
			return;
		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
		}
				
		BPlusTreeByteKeysOnlyCursor otherCursor = new BPlusTreeByteKeysOnlyCursor(other);
		BPlusTreeByteKeysOnlyCursor.Result otherResult = otherCursor.new Result(other.bytesPerKey);
		int otherStatus = otherCursor.get(otherResult);
		
		while (otherStatus == 0) {
			this.insert(otherResult.value);
			otherStatus = otherCursor.get(otherResult);			
		}
		if (print)
			System.out.println("merge:" + this.toStringShort());
	}
	
	public BPlusTreeByteKeysOnly intersect(BPlusTreeByteKeysOnly other, boolean print) {
		if (this.isEmpty() || other.isEmpty())
			return null;
		
		BPlusTreeByteKeysOnly intersection = new BPlusTreeByteKeysOnly(this.nodeSize, this.bytesPerKey, this.keyColumnTypes);
				
		BPlusTreeByteKeysOnlyCursor thisCursor = new BPlusTreeByteKeysOnlyCursor(this);
		BPlusTreeByteKeysOnlyCursor otherCursor = new BPlusTreeByteKeysOnlyCursor(other);
		BPlusTreeByteKeysOnlyCursor.Result thisResult = thisCursor.new Result(this.bytesPerKey);
		BPlusTreeByteKeysOnlyCursor.Result otherResult = otherCursor.new Result(other.bytesPerKey);
		int thisStatus = thisCursor.get(thisResult);
		int otherStatus = otherCursor.get(otherResult);
			
		while ((thisStatus == 0) && (otherStatus == 0)) {
			int compareResult = ByteArrayHelper.compare(thisResult.value, otherResult.value, this.bytesPerKey);
			// we did not find thisResult.value in other
			if (compareResult < 0) {
				thisStatus = thisCursor.get(thisResult);
			} 
			// we did not find otherResult.value in this
			else if (compareResult > 0) {
				otherStatus = otherCursor.get(otherResult);
			}
			// both contain the item, insert to intersection and move both cursors forward
			else {
				intersection.insert(thisResult.value);
				thisStatus = thisCursor.get(thisResult);
				otherStatus = otherCursor.get(otherResult);
			}
		}

		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
			System.out.println("intersection:" + intersection.toStringShort());
		}
		
		return intersection;
	}
	
	public BPlusTreeByteKeysOnly difference(BPlusTreeByteKeysOnly other, boolean print) {
		if (this.isEmpty() || other == null || other.isEmpty())
			return null;
		
		BPlusTreeByteKeysOnly difference = new BPlusTreeByteKeysOnly(this.nodeSize, this.bytesPerKey, this.keyColumnTypes);
				
		BPlusTreeByteKeysOnlyCursor thisCursor = new BPlusTreeByteKeysOnlyCursor(this);
		BPlusTreeByteKeysOnlyCursor otherCursor = new BPlusTreeByteKeysOnlyCursor(other);
		BPlusTreeByteKeysOnlyCursor.Result thisResult = thisCursor.new Result(this.bytesPerKey);
		BPlusTreeByteKeysOnlyCursor.Result otherResult = otherCursor.new Result(other.bytesPerKey);
		int thisStatus = thisCursor.get(thisResult);
		int otherStatus = otherCursor.get(otherResult);
			
		while ((thisStatus == 0) && (otherStatus == 0)) {
			int compareResult = ByteArrayHelper.compare(thisResult.value, otherResult.value, this.bytesPerKey);
			// we did not find thisResult.value in other, so add to difference
			if (compareResult < 0) {
				difference.insert(thisResult.value);
				thisStatus = thisCursor.get(thisResult);
			} 
			// we did not find otherResult.value in this
			else if (compareResult > 0) {
				otherStatus = otherCursor.get(otherResult);
			}
			// we made a match, so move both cursors forward
			else {
				thisStatus = thisCursor.get(thisResult);
				otherStatus = otherCursor.get(otherResult);
			}
		}
		
		// other has been exhausted so everything left from this can be put in to output tree
		while (thisStatus == 0) {
			difference.insert(thisResult.value);
			thisStatus = thisCursor.get(thisResult);			
		}

		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
			System.out.println("difference:" + difference.toStringShort());
		}
		
		return difference;
	}
	
	/*
	private void writeObject(ObjectOutputStream stream) throws IOException {
		System.out.println("writeobject");
		// unhook all the next fields in bplustree leaves
		clearNextsInLeaves(this.rootNode);
		stream.defaultWriteObject();
	}
	
	private void clearNextsInLeaves(BPlusTreeByteKeysOnlyPage page) {
		if (page instanceof BPlusTreeByteKeysOnlyNode) {			
			BPlusTreeByteKeysOnlyNode node = (BPlusTreeByteKeysOnlyNode)page;
			if (node.getHeight() == 1) {
				for (int i = 0; i < node.highWaterMark; i++)
					((BPlusTreeByteKeysOnlyLeaf) node.children[i]).setNext(null);
			}
			
			for (BPlusTreeByteKeysOnlyPage child : node.children)
				clearNextsInLeaves(child);			
		}
	}
	
	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		// hook back up transient next fields for b+tree scan
		// these have to be transient in order for standard java serialization to work 
		hookUpNextsInLeaves(this.rootNode, new LastRightMostLeaf());
	}*/
	
	private void hookUpNextsInLeaves(BPlusTreeByteKeysOnlyPage page, LastRightMostLeaf lrml) {
		if (page instanceof BPlusTreeByteKeysOnlyNode) {			
			BPlusTreeByteKeysOnlyNode node = (BPlusTreeByteKeysOnlyNode)page;
			if (node.getHeight() == 1) {
				BPlusTreeByteKeysOnlyLeaf leaf = (BPlusTreeByteKeysOnlyLeaf)node.children[0];
				if (lrml.leaf != null)
					lrml.leaf.setNext(leaf);
				
				for (int i = 1; i < node.highWaterMark; i++) {
					leaf.setNext((BPlusTreeByteKeysOnlyLeaf) node.children[i]);
					leaf = (BPlusTreeByteKeysOnlyLeaf) node.children[i];
				}
				lrml.leaf = leaf;
			}
			
			for (BPlusTreeByteKeysOnlyPage child : node.children)
				 hookUpNextsInLeaves(child, lrml);
		}
	}
	
	private class LastRightMostLeaf {
		BPlusTreeByteKeysOnlyLeaf leaf;
	}
	
	public void prepareForIterator() {		
		// hook back up transient next fields for b+tree scan
		// these have to be transient in order for standard java serialization to work 
		hookUpNextsInLeaves(this.rootNode, new LastRightMostLeaf());		
	}
	
	public String toStringStatistics() {
		StringBuilder output = new StringBuilder();
		output.append("\n////START - B+ Tree Statistics (NodeSize: "+this.nodeSize+", BytesPerKey:"+this.bytesPerKey+", KeysPerNode:"+this.getNodeSize()/getBytesPerKey()+") - START /////\n");		
		if (this.rootNode.isEmpty()) {
			output.append("empty");
		} else {
			output.append("height: " + this.getHeight() + "\n");
			output.append("# of entries: " + this.getNumberOfEntries() + "\n");
			
			List<Integer> leafStats = new LinkedList<>();			// number of entries per leaf
			List<Triple<Integer, Integer, Integer>> nodeStats = new LinkedList<>(); // number of entries, number of children, height per node
			gatherStatistics(this.rootNode, leafStats, nodeStats);
			
			int numberOfLeaves = leafStats.size();
			long totalLeafEntries = 0;
			for (Integer i : leafStats)
				totalLeafEntries += i;
			
			output.append("# of leaves: " + numberOfLeaves + "\n");
			output.append("avg. entries per leaf: " + (double)totalLeafEntries / (double)numberOfLeaves + "\n");
			
			int numberOfNodes = nodeStats.size();
			
			output.append("# of interior nodes: " + numberOfNodes + "\n");
			
			// height, <# of nodes, total entries, total # of children>
			Map<Integer, Triple<Integer, Integer, Integer>> map = new HashMap<>();
			for (Triple<Integer, Integer, Integer> trip : nodeStats) {
				Triple<Integer, Integer, Integer> newTrip = map.get(trip.getThird());
				if (newTrip == null) {
					newTrip = new Triple<>(0,0,0);			
					map.put(trip.getThird(), newTrip);
				}
				newTrip.first += 1;
				newTrip.second += trip.getFirst();
				newTrip.third += trip.getSecond();
			}			
			
			Triple<Integer, Integer, Integer> trip;
			for (int i = 1; i < map.size(); i++) {
				trip = map.get(i);
				output.append(trip.first + " interior nodes of height "+i+"\n");
				output.append("interior node of height "+i+" avg entries per node:"+ (double)trip.second/(double)trip.first + "\n");
				output.append("interior node of height "+i+" avg children per node:"+ (double)trip.third/(double)trip.first + "\n");
			}
			
		}
		output.append("////END - B+ Tree Statistics - END /////\n");
		return output.toString();
	}
	
	private void gatherStatistics(BPlusTreeByteKeysOnlyPage page, 
			List<Integer> leafEntriesStats, 
			List<Triple<Integer, Integer, Integer>> nodeEntriesStats) {
		if (page instanceof BPlusTreeByteKeysOnlyLeaf) {
			BPlusTreeByteKeysOnlyLeaf leaf = (BPlusTreeByteKeysOnlyLeaf)page;
			leafEntriesStats.add(leaf.highWaterMark);
		} else {
			BPlusTreeByteKeysOnlyNode node = (BPlusTreeByteKeysOnlyNode)page;
			nodeEntriesStats.add(new Triple<>(node.getNumberOfEntries(), node.getHighWaterMark(), node.getHeight()));
			for (int i = 0; i < node.getHighWaterMark(); i++)
				gatherStatistics(node.children[i], leafEntriesStats, nodeEntriesStats);
		}
	}
}
