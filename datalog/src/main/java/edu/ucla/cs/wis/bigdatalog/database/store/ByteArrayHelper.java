package edu.ucla.cs.wis.bigdatalog.database.store;

import java.util.Arrays;

public class ByteArrayHelper {
	// returns 0 if equal
	// returns > 0 if key1 greater than key2
	// returns < 0 if key2 greater than key1
	public static int compare(byte[] key1, byte[] key2, int key2StartOffset, int keySize) {
		for (int i = 0; i < keySize; i++) {
			int result = (key1[i] & 0xFF) - (key2[key2StartOffset+i] & 0xFF);
			if (result != 0)
				return result;
        }
        return 0;
	}
		
	public static int compare4(byte[] key1, byte[] key2) {
		int result = (key1[0] & 0xFF) - (key2[0] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (key1[1] & 0xFF) - (key2[1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (key1[2] & 0xFF) - (key2[2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (key1[3] & 0xFF) - (key2[3] & 0xFF);
		if (result4 != 0)
			return result4;

        return 0;
	}
	
	public static int compare4(byte[] key1, int offset1, byte[] key2, int offset2) {
		int result = (key1[offset1] & 0xFF) - (key2[offset2] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (key1[offset1+1] & 0xFF) - (key2[offset2+1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (key1[offset1+2] & 0xFF) - (key2[offset2+2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (key1[offset1+3] & 0xFF) - (key2[offset2+3] & 0xFF);
		if (result4 != 0)
			return result4;

        return 0;
	}
	
	public static int compare8(byte[] key1, byte[] key2) {
		int result = (key1[0] & 0xFF) - (key2[0] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (key1[1] & 0xFF) - (key2[1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (key1[2] & 0xFF) - (key2[2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (key1[3] & 0xFF) - (key2[3] & 0xFF);
		if (result4 != 0)
			return result4;
		
		int result5 = (key1[4] & 0xFF) - (key2[4] & 0xFF);
		if (result5 != 0)
			return result5;

		int result6 = (key1[5] & 0xFF) - (key2[5] & 0xFF);
		if (result6 != 0)
			return result6;

		int result7 = (key1[6] & 0xFF) - (key2[6] & 0xFF);
		if (result7 != 0)
			return result7;

		int result8 = (key1[7] & 0xFF) - (key2[7] & 0xFF);
		if (result8 != 0)
			return result8;

        return 0;
	}
	
	public static int compare8(byte[] key1, int offset1, byte[] key2, int offset2) {
		int result = (key1[offset1] & 0xFF) - (key2[offset2] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (key1[offset1 + 1] & 0xFF) - (key2[offset2 + 1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (key1[offset1 + 2] & 0xFF) - (key2[offset2 + 2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (key1[offset1 + 3] & 0xFF) - (key2[offset2 + 3] & 0xFF);
		if (result4 != 0)
			return result4;
		
		int result5 = (key1[offset1 + 4] & 0xFF) - (key2[offset2 + 4] & 0xFF);
		if (result5 != 0)
			return result5;

		int result6 = (key1[offset1 + 5] & 0xFF) - (key2[offset2 + 5] & 0xFF);
		if (result6 != 0)
			return result6;

		int result7 = (key1[offset1 + 6] & 0xFF) - (key2[offset2 + 6] & 0xFF);
		if (result7 != 0)
			return result7;

		int result8 = (key1[offset1 + 7] & 0xFF) - (key2[offset2 + 7] & 0xFF);
		if (result8 != 0)
			return result8;

        return 0;
	}
	
	public static int compare16(byte[] key1, byte[] key2) {		
		int result = (key1[0] & 0xFF) - (key2[0] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (key1[1] & 0xFF) - (key2[1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (key1[2] & 0xFF) - (key2[2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (key1[3] & 0xFF) - (key2[3] & 0xFF);
		if (result4 != 0)
			return result4;
		
		int result5 = (key1[4] & 0xFF) - (key2[4] & 0xFF);
		if (result5 != 0)
			return result5;

		int result6 = (key1[5] & 0xFF) - (key2[5] & 0xFF);
		if (result6 != 0)
			return result6;

		int result7 = (key1[6] & 0xFF) - (key2[6] & 0xFF);
		if (result7 != 0)
			return result7;

		int result8 = (key1[7] & 0xFF) - (key2[7] & 0xFF);
		if (result8 != 0)
			return result8;
		
		int result9 = (key1[8] & 0xFF) - (key2[8] & 0xFF);
		if (result9 != 0)
			return result9;
		
		int result10 = (key1[9] & 0xFF) - (key2[9] & 0xFF);
		if (result10 != 0)
			return result10;
		
		int result11 = (key1[10] & 0xFF) - (key2[10] & 0xFF);
		if (result11!= 0)
			return result11;
		
		int result12 = (key1[11] & 0xFF) - (key2[11] & 0xFF);
		if (result12 != 0)
			return result12;
		
		int result13 = (key1[12] & 0xFF) - (key2[12] & 0xFF);
		if (result13 != 0)
			return result13;
		
		int result14 = (key1[13] & 0xFF) - (key2[13] & 0xFF);
		if (result14 != 0)
			return result14;
		
		int result15 = (key1[14] & 0xFF) - (key2[14] & 0xFF);
		if (result15 != 0)
			return result15;
		
		int result16 = (key1[15] & 0xFF) - (key2[15] & 0xFF);
		if (result16 != 0)
			return result16;

        return 0;
	}	

	public static int compare(byte[] key1, byte[] key2, int keySize) {
		for (int i = 0; i < keySize; i++) {
			int result = (key1[i] & 0xFF) - (key2[i] & 0xFF);
			if (result != 0)
				return result;
        }
        return 0;
	}
	
	public static int compareInteger(byte[] bytes, int offset, int intValue) {
		byte[] intBytes = new byte[4];
		intBytes[0] = (byte) ((intValue >> 24) & 0xFF);
		intBytes[1] = (byte) ((intValue >> 16) & 0xFF);
		intBytes[2] = (byte) ((intValue >> 8) & 0xFF);
		intBytes[3] = (byte) (intValue & 0xFF);

		int result = (bytes[offset] & 0xFF) - (intBytes[0] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (bytes[offset + 1] & 0xFF) - (intBytes[1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (bytes[offset + 2] & 0xFF) - (intBytes[2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (bytes[offset + 3] & 0xFF) - (intBytes[3] & 0xFF);
		if (result4 != 0)
			return result4;

        return 0;
	}
	
	public static int compareLong(byte[] bytes, int offset, long longValue) {
		byte[] longBytes = new byte[8];
		longBytes[0] = (byte) ((longValue >> 56) & 0xFF);
		longBytes[1] = (byte) ((longValue >> 48) & 0xFF);
		longBytes[2] = (byte) ((longValue >> 40) & 0xFF);
		longBytes[3] = (byte) ((longValue >> 32) & 0xFF);
		longBytes[4] = (byte) ((longValue >> 24) & 0xFF);
		longBytes[5] = (byte) ((longValue >> 16) & 0xFF);
		longBytes[6] = (byte) ((longValue >> 8) & 0xFF);
		longBytes[7] = (byte) (longValue & 0xFF);
		
		int result = (bytes[offset] & 0xFF) - ((byte) ((longValue >> 56) & 0xFF) & 0xFF);
		if (result != 0)
			return result;

		int result2 = (bytes[offset + 1] & 0xFF) - (longBytes[1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (bytes[offset + 2] & 0xFF) - (longBytes[2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (bytes[offset + 3] & 0xFF) - (longBytes[3] & 0xFF);
		if (result4 != 0)
			return result4;
		
		int result5 = (bytes[offset + 4] & 0xFF) - (longBytes[4] & 0xFF);
		if (result5 != 0)
			return result5;

		int result6 = (bytes[offset + 5] & 0xFF) - (longBytes[5] & 0xFF);
		if (result6 != 0)
			return result6;

		int result7 = (bytes[offset + 6] & 0xFF) - (longBytes[6] & 0xFF);
		if (result7 != 0)
			return result7;

		int result8 = (bytes[offset + 7] & 0xFF) - (longBytes[7] & 0xFF);
		if (result8 != 0)
			return result8;

        return 0;
	}	
	
	public static byte getByte(byte[] bytes, int offset) {
		return (byte)(bytes[offset++] & 255);
	}
	
	public static short getBytesAsShort(byte[] bytes, int offset) {
		return (short) ((bytes[offset++] & 255) << 8 
				| (bytes[offset] & 255));
	}
	
	public static int getBytesAsInt(byte[] bytes, int offset) {
		return ((bytes[offset++] & 255) << 24 
				| (bytes[offset++] & 255) << 16 
				| (bytes[offset++] & 255) << 8 
				| (bytes[offset] & 255));
	}
	
	public static float getBytesAsFloat(byte[] bytes, int offset) {
		/*int val = ((bytes[offset++] & 255) << 24 
				| (bytes[offset++] & 255) << 16 
				| (bytes[offset++] & 255) << 8 
				| (bytes[offset] & 255));*/		
		return Float.intBitsToFloat(getBytesAsInt(bytes, offset));
	}
	
	public static long getBytesAsLong(byte[] bytes, int offset) {
		return 	(((long)bytes[offset++] << 56) |
                ((long)(bytes[offset++] & 255) << 48) |
                ((long)(bytes[offset++] & 255) << 40) |
                ((long)(bytes[offset++] & 255) << 32) |
                ((long)(bytes[offset++] & 255) << 24) |
                ((bytes[offset++] & 255) << 16) |
                ((bytes[offset++] & 255) <<  8) |
                ((bytes[offset++] & 255) <<  0));
	}
	
	public static double getBytesAsDouble(byte[] bytes, int offset) {
		/*long val = 	(((long)bytes[offset++] << 56) |
                ((long)(bytes[offset++] & 255) << 48) |
                ((long)(bytes[offset++] & 255) << 40) |
                ((long)(bytes[offset++] & 255) << 32) |
                ((long)(bytes[offset++] & 255) << 24) |
                ((bytes[offset++] & 255) << 16) |
                ((bytes[offset++] & 255) <<  8) |
                ((bytes[offset++] & 255) <<  0));*/	
		return Double.longBitsToDouble(getBytesAsLong(bytes, offset));
	}
	
	public static byte[] getShortAsBytes(short shortValue) {
		byte[] bytes = new byte[2];
		bytes[0] = (byte) ((shortValue >> 8) & 0xFF);
		bytes[1] = (byte) (shortValue & 0xFF);
		return bytes;
	}
	
	public static int getShortAsBytes(short shortValue, byte[] bytes, int offset) {
		bytes[offset++] = (byte) ((shortValue >> 8) & 0xFF);
		bytes[offset++] = (byte) (shortValue & 0xFF);
		return offset;
	}
				
	public static byte[] getIntAsBytes(int intValue) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) ((intValue >> 24) & 0xFF);
		bytes[1] = (byte) ((intValue >> 16) & 0xFF);
		bytes[2] = (byte) ((intValue >> 8) & 0xFF);
		bytes[3] = (byte) (intValue & 0xFF);
		return bytes;
	}
		
	public static int getIntAsBytes(int intValue, byte[] bytes, int offset) {
		bytes[offset++] = (byte) ((intValue >> 24) & 0xFF);
		bytes[offset++] = (byte) ((intValue >> 16) & 0xFF);
		bytes[offset++] = (byte) ((intValue >> 8) & 0xFF);
		bytes[offset++] = (byte) (intValue & 0xFF);
		return offset;
	}
	
	public static void setIntAsBytes(int intValue, byte[] bytes) {
		int startOffset = bytes.length - 4;
		int offset = startOffset;
		bytes[offset++] = (byte) ((intValue >> 24) & 0xFF);
		bytes[offset++] = (byte) ((intValue >> 16) & 0xFF);
		bytes[offset++] = (byte) ((intValue >> 8) & 0xFF);
		bytes[offset++] = (byte) (intValue & 0xFF);
		
		if (intValue < 0 && startOffset > 0)
			set(bytes, 0, startOffset, (byte) -1);		
	}
	
	public static byte[] getLongAsBytes(long longValue) {
		byte[] bytes = new byte[8];
		bytes[0] = (byte) ((longValue >> 56) & 0xFF);
		bytes[1] = (byte) ((longValue >> 48) & 0xFF);
		bytes[2] = (byte) ((longValue >> 40) & 0xFF);
		bytes[3] = (byte) ((longValue >> 32) & 0xFF);
		bytes[4] = (byte) ((longValue >> 24) & 0xFF);
		bytes[5] = (byte) ((longValue >> 16) & 0xFF);
		bytes[6] = (byte) ((longValue >> 8) & 0xFF);
		bytes[7] = (byte) (longValue & 0xFF);
		return bytes;
	}
	
	public static int getLongAsBytes(long longValue, byte[] bytes, int offset) {
		bytes[offset++] = (byte) ((longValue >> 56) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 48) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 40) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 32) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 24) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 16) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 8) & 0xFF);
		bytes[offset++] = (byte) (longValue & 0xFF);
		return offset;
	}
	
	public static byte[] getFloatAsBytes(float floatValue) {
		return getIntAsBytes(Float.floatToIntBits(floatValue));
	}
	
	public static int getFloatAsBytes(float floatValue, byte[] bytes, int offset) {
		return getIntAsBytes(Float.floatToIntBits(floatValue), bytes, offset);
	}
	
	public static byte[] getDoubleAsBytes(double doubleValue) {
		return getLongAsBytes(Double.doubleToLongBits(doubleValue));
	}
	
	public static int getDoubleAsBytes(double doubleValue, byte[] bytes, int offset) {
		return getLongAsBytes(Double.doubleToLongBits(doubleValue), bytes, offset); 
	}
	
	public static void setLongAsBytes(long longValue, byte[] bytes) {
		int startOffset = bytes.length - 8; 
		int offset = startOffset;
		bytes[offset++] = (byte) ((longValue >> 56) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 48) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 40) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 32) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 24) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 16) & 0xFF);
		bytes[offset++] = (byte) ((longValue >> 8) & 0xFF);
		bytes[offset++] = (byte) (longValue & 0xFF);
		
		if (longValue < 0 && startOffset > 0)
			set(bytes, 0, startOffset, (byte) -1);		
	}
	
	public static byte[] getBytesAsBytes(byte[] bytes, int offset) {
		return Arrays.copyOfRange(bytes, offset, bytes.length);
	}
	
	public static byte[] getBytesAsBytes(byte[] bytes, int offset, int length) {
		return Arrays.copyOfRange(bytes, offset, length);
	}
	
	public static int roundToNearestPowerOfTwo(int number) {
		int newSize = number; 
		newSize--;
		newSize |= newSize >> 1;
		newSize |= newSize >> 2;
		newSize |= newSize >> 4;
		newSize |= newSize >> 8;
		newSize |= newSize >> 16;
		newSize++;
		return newSize;
	}
	
	public static boolean isEmpty(byte[] bytes, int offset, int length) {
		for (int i = 0; i < length; i++)
			if (bytes[offset + i] != 0)
				return false;
		
		return true;
	}
	
	public static String toString(byte[] bytes) {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < bytes.length; i++)
			output.append(bytes[i]);
			
		return output.toString();
	}
		
	private static void set(byte[] bytes, int start, int end, byte value) {
		for (int i = start; i < end; i++)
			bytes[i] = value;
	}
	
	public static int greaterThanSigned(byte[] key1, byte[] key2) {
		// if MSB is 0x8, it is a negative number
		byte key1Sign = (byte) (key1[0] & 0x8);
		byte key2Sign = (byte) (key2[0] & 0x8);

		if (key1Sign == 0x8) { 
			if (key2Sign == 0)
				return -1;

		} else {
			if (key2Sign == 0x8)
				return 1;
		}

		// both positive
		int result = (key1[0] & 0xFF) - (key2[0] & 0xFF);
		if (result != 0)
			return result;

		int result2 = (key1[1] & 0xFF) - (key2[1] & 0xFF);
		if (result2 != 0)
			return result2;

		int result3 = (key1[2] & 0xFF) - (key2[2] & 0xFF);
		if (result3 != 0)
			return result3;

		int result4 = (key1[3] & 0xFF) - (key2[3] & 0xFF);
		if (result4 != 0)
			return result4;
		
		int result5 = (key1[4] & 0xFF) - (key2[4] & 0xFF);
		if (result5 != 0)
			return result5;

		int result6 = (key1[5] & 0xFF) - (key2[5] & 0xFF);
		if (result6 != 0)
			return result6;

		int result7 = (key1[6] & 0xFF) - (key2[6] & 0xFF);
		if (result7 != 0)
			return result7;

		int result8 = (key1[7] & 0xFF) - (key2[7] & 0xFF);
		if (result8 != 0)
			return result8;
		
		int result9 = (key1[8] & 0xFF) - (key2[8] & 0xFF);
		if (result9 != 0)
			return result9;
		
		int result10 = (key1[9] & 0xFF) - (key2[9] & 0xFF);
		if (result10 != 0)
			return result10;
		
		int result11 = (key1[10] & 0xFF) - (key2[10] & 0xFF);
		if (result11!= 0)
			return result11;
		
		int result12 = (key1[11] & 0xFF) - (key2[11] & 0xFF);
		if (result12 != 0)
			return result12;
		
		int result13 = (key1[12] & 0xFF) - (key2[12] & 0xFF);
		if (result13 != 0)
			return result13;
		
		int result14 = (key1[13] & 0xFF) - (key2[13] & 0xFF);
		if (result14 != 0)
			return result14;
		
		int result15 = (key1[14] & 0xFF) - (key2[14] & 0xFF);
		if (result15 != 0)
			return result15;
		
		int result16 = (key1[15] & 0xFF) - (key2[15] & 0xFF);
		if (result16 != 0)
			return result16;

        return 0;
	}	
}
	
