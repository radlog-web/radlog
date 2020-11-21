package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;

public abstract class RangeSearchKeys<T> implements Serializable {
	private static final long serialVersionUID = 1L;
	public T startKey;
	public T endKey;
	
	public RangeSearchKeys() {}
	
	public RangeSearchKeys(T startKey, T endKey) {
		this.startKey = startKey;
		this.endKey = endKey;
	}
	
	public static RangeSearchKeys<?> createRangeSearchKeys(long startKey) {
		return new LongRangeSearchKeys(startKey, 0);
	}
	
	public static RangeSearchKeys<?> createRangeSearchKeys(RangeSearchableStore store, DbTypeBase[] keys) {
		// can't do range search given all keys
		if (keys.length == store.getKeyColumns().length)
			return null;
		
		if ((keys.length == 1) && (store.getKeyColumns().length == 2)) {
			// it will be an integer
			int value = ((EncodedType)keys[0]).getKey();
			long startKey = (long)value << 32; // 0 is farthest left range
			long endKey = ((long)value << 32) | (-1 & 0xFFFFFFFFL); // -1 is farthest right range 
			/*
			System.out.println(((long)value << 32) | (Integer.MAX_VALUE & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (Integer.MAX_VALUE & 0xFFFFFFFFL)));
			System.out.println(((long)value << 32) | (2 & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (2 & 0xFFFFFFFFL)));
			System.out.println(((long)value << 32) | (1 & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (1 & 0xFFFFFFFFL)));
			System.out.println(((long)value << 32) | (0 & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (0 & 0xFFFFFFFFL)));
			
			System.out.println(((long)value << 32) | (-1 & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (-1 & 0xFFFFFFFFL)));
			System.out.println(((long)value << 32) | (-2 & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (-2 & 0xFFFFFFFFL)));
			System.out.println(((long)value << 32) | (Integer.MIN_VALUE & 0xFFFFFFFFL));
			System.out.println(Long.toBinaryString(((long)value << 32) | (Integer.MIN_VALUE & 0xFFFFFFFFL)));
			
			for (int i = Integer.MIN_VALUE; i < -1;) {
				System.out.println(i & 0xFFFFFFFFL);
				i = i >> 1;
			}
			
			for (int i = 1; i < Integer.MAX_VALUE;) {
				System.out.println(i & 0xFFFFFFFFL);
				i = i << 1;
			}
			*/
			/*
			System.out.println(((long)0 << 32) + 0);
			System.out.println((((long)0 << 32) + 0) >> 32);
			System.out.println(((long)0 << 32) + 1);
			System.out.println((((long)0 << 32) + 1) >> 32);
			System.out.println((int)((long)0 << 32) + 1);
			System.out.println(((long)0 << 32) + -1);
			System.out.println(((long)10 << 32) + -1234);
			System.out.println(((long)10 << 32) + 1234);
			System.out.println(((long)-10 << 32) + 1234);
			System.out.println(((long)-10 << 32) + -1234);
			
			System.out.println(100 & 0xFFFFFFFFL);
			System.out.println(-100 & 0xFFFFFFFFL);
			
			long v;
			for (int i = 0; i < 10; i++) {
				v = (((long)i << 32) | (Integer.MAX_VALUE & 0xFFFFFFFFL));
				System.out.println(v);
				if (((int)(v >> 32)) != i)
					System.out.println("stop");
				
				v = (((long)i << 32) | (Integer.MIN_VALUE & 0xFFFFFFFFL));
				System.out.println(v);
				if (((int)(v >> 32)) != i)
					System.out.println("stop");
			}
			
			System.out.println(((long)value << 32) + Integer.MIN_VALUE);
			System.out.println(((long)value << 32) + Integer.MAX_VALUE);
			System.out.println(((long)value << 32) + Integer.MAX_VALUE + 1);
			System.out.println("lower search key: " + (Integer.MAX_VALUE & 0xFFFFFFFFL));
			System.out.println("upper search key: " + ((long)value << 32));
			System.out.println("start key: " + startKey);
			System.out.println("  end key: " + endKey);*/
			return new LongRangeSearchKeys(startKey, endKey);
		} else if (store.getKeyColumns().length > 2) {
			byte[] startKey = new byte[store.getBytesPerKey()];
			byte[] endKey = new byte[store.getBytesPerKey()];						
			
			int offset = 0;
			for (int i = 0; i < keys.length; i++) {
				byte[] keyBytes = keys[i].getBytes();
				System.arraycopy(keyBytes, 0, startKey, offset, keyBytes.length);				
				offset += keyBytes.length;
			}
			
			System.arraycopy(startKey, 0, endKey, 0, offset);
			
			// pad rest of startkey with 0s
			for (int i = offset; i < store.getBytesPerKey(); i++)
				startKey[i] = 0;

			// pad rest of endkey with 255s
			for (int i = offset; i < store.getBytesPerKey(); i++)
				endKey[i] = (byte) 0xFF;
			
			return new ByteRangeSearchKeys(startKey, endKey);
		}
		
		return null;
	}
}
