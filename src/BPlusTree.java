import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class BPlusTree {
	private static final byte SIZE_INT = 4;
	private static final byte PAYLOAD_SIZE = 2;
	private static final int PAGE_SIZE = 512;
	private static final int CELL_POINTER_SIZE = 2;
	private static final int OTHER_COLUMNS_COUNT = 1;
	private static final int MAX_TEXT_SIZE = 115;
	private static final byte PAGE_TYPE_SIZE = 1;
	private static final byte CELL_COUNT_SIZE =1 ;
	private static final short CONTENT_AREA_SIZE = 2;
	
	RandomAccessFile davisBaseTable;
	
	public BPlusTree(RandomAccessFile davisBaseTable) {
		this.davisBaseTable = davisBaseTable;
	}
	
	int getRowId(int pageNum) throws IOException{
		davisBaseTable.seek((pageNum-1)*PAGE_SIZE);
		int pageType = davisBaseTable.readByte();
		
		int rowId = -1;
		if(pageType == 13)
		{
			int cellCount = davisBaseTable.readByte();
			if(cellCount == 0)
			{
				rowId = 1;
			}
			else
			{
				//System.out.println((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(cellCount-1)*2);
			davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(cellCount-1)*2);
			int point = davisBaseTable.readShort();
			davisBaseTable.seek(point+PAYLOAD_SIZE);
			rowId = davisBaseTable.readInt()+1;
			}
		}
		else
		{
			davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
			rowId = getRowId(davisBaseTable.readInt());
		}
		return rowId;
	}
	@SuppressWarnings("unused")
	int findLeafPageNum(int key,int pageNum) throws Exception
	{
		davisBaseTable.seek((pageNum-1)*PAGE_SIZE);
		int pageType = davisBaseTable.readByte();
		int cellCount = davisBaseTable.readByte();
		int contentArea = davisBaseTable.readShort();
		if(pageType == 13)
		{
			for(int i = 1 ; i <= cellCount ; i++)
			{
				int cellPointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer+PAYLOAD_SIZE); 
				int rowId = davisBaseTable.readInt();
				if(rowId == key)
				{
					throw new Exception("Key already Exists");

				}
			}
		}
		else
		{
			int rightpointer = davisBaseTable.readInt();
			boolean flag = true;
			for(int i = 1,p=0 ; i <= cellCount ; i++)
			{
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+(i-1)*CELL_POINTER_SIZE);
				int cellPointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer);
				int lftptr = davisBaseTable.readInt();
				int rowId = davisBaseTable.readInt();
				if(key <= rowId)
				{
					flag = false;
					pageNum = findLeafPageNum(key, lftptr);
					break;
				}
			}
			if(flag)
			{
				pageNum =  findLeafPageNum(key, rightpointer);
			}
		}
		return pageNum;
	}
	int findPositionInLeafNode(int key,int pageNum) throws IOException
	{
		davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE);
		int cellCount = davisBaseTable.readByte();
		for(int i = 0 ; i < cellCount ; i++)
		{
			davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+i*2);
			int cellpointer = davisBaseTable.readShort();
			davisBaseTable.seek(cellpointer+PAYLOAD_SIZE);
			int rowId = davisBaseTable.readInt();
			if(key < rowId)
			{
				return i+1;
			}
			
		}
		return cellCount+1;
		
	}
	@SuppressWarnings("unused")
	int findParentPage(int pageNum,int parentPage) throws IOException
	{
		boolean flag = true;
		davisBaseTable.seek((parentPage-1)*PAGE_SIZE);
		int pageType = davisBaseTable.readByte();
		if(pageType == 13)
		{
			return -1;
		}
		else
		{
		int cellCount = davisBaseTable.readByte();
		int contentArea = davisBaseTable.readShort();
		int rightptr = davisBaseTable.readInt();
		if(rightptr == pageNum)
		{
			return parentPage;
		}
		for(int i = 0 ; i < cellCount; i++)
		{
			davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+i*2);
			 davisBaseTable.seek(davisBaseTable.readShort());
			 int lftptr = davisBaseTable.readInt();
			if(lftptr == pageNum)
			{
				flag = false;
				break;
			}
		}
		if(flag)
			{
			boolean lFlag = true;
			for(int i = 0 ; i < cellCount; i++)
			{
				davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+i*2);
				 davisBaseTable.seek(davisBaseTable.readShort());
				 int lftptr = davisBaseTable.readInt();
				 int pPage= findParentPage(pageNum, lftptr);
				 if(pPage != -1)
				 {
					 parentPage = pPage;
					 lFlag = false;
					 break;
				 }
			}
			if(lFlag)
			{
				parentPage = findParentPage(pageNum, rightptr);
			}
			}
		}
		return parentPage;
	}
	@SuppressWarnings("unused")
	void insertIntoLeaf(int key,List<ArrayList<String>> tableDetsList) throws Exception
	{
		/**
		 * 2 conditions - enough space , not enough space
		 * enough space - insert the table details
		 * not enough space - if parentPageNum is root , create 2 new pages else create one new page - split should propagate to interior page
		 * interior page - enough space not enough space
		 */
		int pageNum = findLeafPageNum(key,1);
		int position = findPositionInLeafNode(key,pageNum);
		int maxrecordSize = calculateRecordSize(tableDetsList,"MAX_SIZE");
		int maxCellCount = calcMaxCells(maxrecordSize);
		int recordSize = calculateRecordSize(tableDetsList, "NORMAL_SIZE");
		davisBaseTable.seek(PAGE_SIZE*(pageNum-1)+1); // seeking to cellcount
		byte cellCount = davisBaseTable.readByte();
		cellCount++;
		int contentArea = davisBaseTable.readShort();
		long fileLength = davisBaseTable.length();
		int payloadSize = recordSize - (CELL_POINTER_SIZE+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+tableDetsList.size());
		if(cellCount <= maxCellCount)
		{
			contentArea =  updateHeader(position,cellCount,recordSize,contentArea,(pageNum-1)*PAGE_SIZE,"LEAF");
			//insert record
			davisBaseTable.seek(contentArea);
			insertLeafRecord(payloadSize,key,tableDetsList);
		}
		else
		{
			/**
			 * check if parentpage == 1 (root) - create 2 pages for left and right child
			 * else copy latter half record into new page and propagate			 
			 */
			int leftLeafCount = (cellCount%2 == 1 )? (cellCount+1)/2 : cellCount/2;
			//copying record details into arrayList using byte buffer
			ArrayList<byte[]> dupParentRec = new ArrayList<byte[]>(); 
			for(int i = 1,p=1 ; i <= cellCount ; i++)
			{
				if(position == i)
				{
					ByteBuffer b =ByteBuffer.allocate(recordSize-CELL_POINTER_SIZE).putShort((short) payloadSize)
							.putInt(key).put((byte) tableDetsList.size());
					for(List<String> arrayList : tableDetsList)
					{
							byte code = getSerialTypeCode(arrayList.get(1),arrayList.get(0)); //type, value
							byte newCode = -1;
							if(arrayList.get(0) == null)
							{
								if(code == 4 )
								{
									newCode = 0;
								}
								else if(code == 5)
								{
									newCode = 1;
								}else if(code == 6 || code == 8)
								{
									newCode = 2;
								}else if(code == 7 || code == 9 || code == 10 || code == 11)
								{
									newCode = 3;
								}
							}
							else
							{
								newCode = code;
							}
							b.put(newCode);
					}
					for(List<String> arrayList : tableDetsList)
					{
						String detStr =  (String)arrayList.get(1);
						switch (detStr){
						
						case "TINYINT" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.put((byte) 0);
							}
							else
							{
								b.put(Byte.parseByte(arrayList.get(0)));
							}
							break;
						case "SMALLINT" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putShort((short) 0);
							}
							else
							{
								b.putShort(Short.parseShort(arrayList.get(0)));
							}
							break;
						case "INT" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putInt(0);
							}
							else
							{
								b.putInt(Integer.parseInt(arrayList.get(0)));
							}
							break;
						case "BIGINT" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putLong(0);
							}else
							{
								b.putLong(Long.parseLong(arrayList.get(0)));
							}
							break;
						case "REAL" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putFloat(0);
							}else
							{
								b.putFloat(Float.parseFloat(arrayList.get(0)));
							}
							break;
						case "DOUBLE" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putDouble(0);
							}else
							{
								b.putDouble(Double.parseDouble(arrayList.get(0)));
							}
							break;
						case "DATETIME" :
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putLong(0);
							}else
							{
								SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
								Date d = formatter.parse(arrayList.get(0));
								long timestamp = d.getTime();
								b.putLong(timestamp);
							}
							break;
						case "DATE" :
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.putLong(0);
							}else
							{
								SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
								Date d = formatter.parse(arrayList.get(0));
								long timestamp = d.getTime();
								b.putLong(timestamp);
							}
							break;
						case "TEXT" : 
							if("NULL".equalsIgnoreCase(arrayList.get(0)))
							{
								b.put("".getBytes());
							}else
							{
								
								b.put(arrayList.get(0).getBytes());
							}
							
							break;			
						
						}
					
				}
					dupParentRec.add(b.array());
				}
					else
				{
					davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(p-1)*CELL_POINTER_SIZE);
					short ptr = davisBaseTable.readShort();
					byte [] b = copyRecordIntoByteArr(ptr);
					dupParentRec.add(b);
					p++;
				}
			}
			/**
			 * if page Num == 1 then copy the record arrayList into 2 leaf pages
			 * else copy the latter half of records into new page
			 * propagate split to interior node
			 */
			if(pageNum == 1)
			{
				
				davisBaseTable.setLength(fileLength+PAGE_SIZE*2);
				davisBaseTable.seek(fileLength); // left node
				davisBaseTable.writeByte(13);//  leaf page
				davisBaseTable.writeByte(0); // cell count to 0
				davisBaseTable.seek(fileLength+PAGE_SIZE); // right node
				davisBaseTable.writeByte(13);
				davisBaseTable.writeByte(0); // cell count to 0
				
				for(int i = 0 ; i < leftLeafCount;i++)
				{
					davisBaseTable.seek(fileLength+PAGE_TYPE_SIZE);
					byte cellcount = davisBaseTable.readByte();
					cellcount++;
					contentArea = (short) getcontentArea(davisBaseTable.readShort(), fileLength);
					byte [] byteArr =dupParentRec.get(i);
					contentArea = (short) (contentArea - byteArr.length);
					davisBaseTable.seek(fileLength+PAGE_TYPE_SIZE);
					davisBaseTable.writeByte(cellcount);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(fileLength+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+i*CELL_POINTER_SIZE);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(contentArea);
					davisBaseTable.write(byteArr);
				}
				for(int i = leftLeafCount,p=0 ; i  <dupParentRec.size();i++,p++)
				{
					davisBaseTable.seek(fileLength+PAGE_SIZE+PAGE_TYPE_SIZE);
					byte cellcount = davisBaseTable.readByte();
					cellcount++;
					contentArea = (short) getcontentArea(davisBaseTable.readShort(), fileLength+PAGE_SIZE);
					byte [] byteArr =dupParentRec.get(i);
					contentArea = (short) (contentArea - byteArr.length);
					davisBaseTable.seek(fileLength+PAGE_SIZE+PAGE_TYPE_SIZE);
					davisBaseTable.writeByte(cellcount);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(fileLength+PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+p*CELL_POINTER_SIZE);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(contentArea);
					davisBaseTable.write(byteArr);
				}
				byte [] barr = dupParentRec.get(leftLeafCount-1);
				ByteBuffer b = ByteBuffer.wrap(barr);
				int upkey = b.getInt(2);
				insertIntoInteriorNode(fileLength, fileLength+PAGE_SIZE, upkey,pageNum);
				
			}
			else
			{
				davisBaseTable.setLength(fileLength+PAGE_SIZE*1);
				davisBaseTable.seek(fileLength); // left node
				davisBaseTable.writeByte(13);//  leaf page
				davisBaseTable.writeByte(0); // cell count to 0
				for(int i = leftLeafCount,p=0 ; i  <dupParentRec.size();i++,p++)
				{
					davisBaseTable.seek(fileLength+PAGE_TYPE_SIZE);
					byte cellcount = davisBaseTable.readByte();
					cellcount++;
					contentArea = (short) getcontentArea(davisBaseTable.readShort(), fileLength);
					byte [] byteArr =dupParentRec.get(i);
					contentArea = (short) (contentArea - byteArr.length);
					davisBaseTable.seek(fileLength+PAGE_TYPE_SIZE);
					davisBaseTable.writeByte(cellcount);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(fileLength+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+p*CELL_POINTER_SIZE);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(contentArea);
					davisBaseTable.write(byteArr);
				}
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(leftLeafCount-1)*CELL_POINTER_SIZE);
				int ptr = davisBaseTable.readShort();
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE);
				davisBaseTable.writeByte(leftLeafCount);
				davisBaseTable.writeShort(ptr);
				
				byte [] barr = dupParentRec.get(leftLeafCount-1);
				ByteBuffer b = ByteBuffer.wrap(barr);
				int upkey = b.getInt(2);
				insertIntoInteriorNode((pageNum-1)*PAGE_SIZE, fileLength, upkey,pageNum);
			}
			
		}
	}
	private byte[] copyRecordIntoByteArr(short ptr) throws IOException {
	davisBaseTable.seek(ptr);
	short  payload = davisBaseTable.readShort();
	int rowId = davisBaseTable.readInt();
	byte othercolCount = davisBaseTable.readByte();
	int recSize = PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+othercolCount+payload;
	
	ByteBuffer b  = ByteBuffer.allocate(recSize);
	b.putShort(payload);
	b.putInt(rowId);
	b.put(othercolCount);
	byte[] colTypeCode = new byte[othercolCount];
	for(int k = 0; k < othercolCount ; k++ )
	{
		colTypeCode[k] = davisBaseTable.readByte();
		b.put(colTypeCode[k]);
	}
	for(int k = 0; k < othercolCount ; k++ )
	{
		switch (colTypeCode[k]){
		case 0 : 
			davisBaseTable.read(new byte[1]);
			break;
		case 1 :
			davisBaseTable.read(new byte[2]);
			break;
		case 2 :
			davisBaseTable.read(new byte[4]);
			break;
		case 3 :
			davisBaseTable.read(new byte[8]);
			break;
		case 4 :
			b.put(davisBaseTable.readByte());
			break;
		case 5 : 
			b.putShort(davisBaseTable.readShort());
			break;
		case 6 : 
			b.putInt(davisBaseTable.readInt());
			break;
		case 7 : 
			b.putLong(davisBaseTable.readLong());
			break;
		case 8 : 
			b.putFloat(davisBaseTable.readFloat());
			break;
		case 9 : 
			b.putDouble(davisBaseTable.readDouble());
			break;
		
		case 10 : 
			b.putLong(davisBaseTable.readLong());
			break;
		
		case 11 : 
			b.putLong(davisBaseTable.readLong());
			break;
		
		default :
			byte [] bArr = new byte[colTypeCode[k]-12];
			davisBaseTable.read(bArr);
			b.put(bArr);
			break;
		}
	}
	return b.array();
	}

	@SuppressWarnings("unused")
	private void insertIntoInteriorNode(long leftPagePtr, long rightPagePtr, int key, int pageNum) throws IOException {
		// step -1
		if(pageNum == 1)
		{
			davisBaseTable.seek(0);
			davisBaseTable.writeByte(5);
			davisBaseTable.writeByte(1);
			davisBaseTable.writeShort(PAGE_SIZE-SIZE_INT-SIZE_INT);
			davisBaseTable.writeInt((int) ((rightPagePtr+PAGE_SIZE)/PAGE_SIZE));
			davisBaseTable.writeShort(PAGE_SIZE-SIZE_INT-SIZE_INT);
			davisBaseTable.seek(PAGE_SIZE-SIZE_INT-SIZE_INT);
			davisBaseTable.writeInt((int) ((leftPagePtr+PAGE_SIZE)/PAGE_SIZE));
			davisBaseTable.writeInt(key);
			return;
		}
		/** 
		 * step -1
		 * search the parentPagenum
		 * 
		 * step -2
		 * check if space is there, if yes sort and insert
		 * 
		 * step -3
		 * split n update the current page it and  propagate
		 */
		int parentPage = findParentPage(pageNum, 1);
		int maxRecordSize = CELL_POINTER_SIZE+SIZE_INT+SIZE_INT;
		int maxCellCount = (PAGE_SIZE - PAGE_TYPE_SIZE - CELL_COUNT_SIZE - CONTENT_AREA_SIZE - SIZE_INT)/maxRecordSize;
		davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE); // seeking to cellcount
		int parentCellCount  = davisBaseTable.readByte();
		parentCellCount++;
		
		int position = findInsertPosition(parentPage,key);
		davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE); 
		int parentContentArea =davisBaseTable.readShort();
		
		int rightPageNum = davisBaseTable.readInt();
		//step -2
		if(parentCellCount <= maxCellCount)	
		{
			parentContentArea = updateHeader(position, parentCellCount, maxRecordSize, parentContentArea, (parentPage-1)*PAGE_SIZE,"INNER");
			davisBaseTable.seek(parentContentArea);
			davisBaseTable.writeInt((int) ((leftPagePtr+PAGE_SIZE)/PAGE_SIZE));
			davisBaseTable.writeInt(key);
			if(position > parentCellCount-1)
			{
			davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
			davisBaseTable.writeInt((int) ((rightPagePtr+PAGE_SIZE)/PAGE_SIZE));
			}
		}
		// step -3
		else
		{
			/**
			 *
			 * a. if parent page == 1 (root) then split into 2 pages copy data into 2 pages and clean and update the parent page
			 * b. else create new page copy the latter half details and propagate split
			 */
			// copying data into temporary arrayList
			long parentFileLength = davisBaseTable.length();
			int leftNodeCount = parentCellCount/2;
			ArrayList<byte[]> dupParentRec = new ArrayList<byte[]>(); 
			for(int i = 1,p=1 ; i <=parentCellCount ; i++)
			{
				if(position == i)
				{
					byte [] byteArr = ByteBuffer.allocate(SIZE_INT+SIZE_INT).putInt((int) (leftPagePtr+PAGE_SIZE)/PAGE_SIZE).putInt(key).array();
					dupParentRec.add(byteArr);
				}else
				{
					davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+(p-1)*CELL_POINTER_SIZE);
					short contentArea = davisBaseTable.readShort();
					davisBaseTable.seek(contentArea);
					ByteBuffer b = ByteBuffer.allocate(2*SIZE_INT);
					int  leftptr = davisBaseTable.readInt();
					int rowId = davisBaseTable.readInt();
					b.putInt(leftptr);
					b.putInt(rowId);
					dupParentRec.add(b.array());
					p++;
				}
			}
			// updating leftpointer of next node to point to right page of newly splited node in case insertion is at middle
			if(position <= parentCellCount-1)
			{
				ByteBuffer b2 = ByteBuffer.wrap(dupParentRec.get(position));
				b2.putInt(0, (int) ((rightPagePtr+PAGE_SIZE)/PAGE_SIZE));
				dupParentRec.add(position, b2.array());
			}
			
			// step -a 
			if(parentPage == 1)
			{
			davisBaseTable.setLength(parentFileLength+PAGE_SIZE*2);
			davisBaseTable.seek(parentFileLength); // left node
			davisBaseTable.writeByte(5);//  leaf page
			davisBaseTable.writeByte(0); // cell count to 0
			davisBaseTable.seek(parentFileLength+PAGE_SIZE); // right node
			davisBaseTable.writeByte(5);
			davisBaseTable.writeByte(0); // cell count to 0
			//copying to left page
			for(int i = 0 ; i < leftNodeCount;i++)
			{
				davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE);
				byte cellcount = davisBaseTable.readByte();
				cellcount++;
				short contentArea = (short) getcontentArea(davisBaseTable.readShort(), parentFileLength);
				byte [] byteArr =dupParentRec.get(i);
				contentArea = (short) (contentArea - byteArr.length);
				davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE);
				davisBaseTable.writeByte(cellcount);
				davisBaseTable.writeShort(contentArea);
				davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+i*CELL_POINTER_SIZE);
				davisBaseTable.writeShort(contentArea);
				davisBaseTable.seek(contentArea);
				davisBaseTable.write(byteArr);
			}
			//copying to right page
			for(int i = leftNodeCount+1,p=0 ; i  <dupParentRec.size();i++,p++)
			{
				davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE);
				byte cellcount = davisBaseTable.readByte();
				cellcount++;
				short contentArea = (short) getcontentArea(davisBaseTable.readShort(), parentFileLength+PAGE_SIZE);
				byte [] byteArr =dupParentRec.get(i);
				contentArea = (short) (contentArea - byteArr.length);
				davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE);
				davisBaseTable.writeByte(cellcount);
				davisBaseTable.writeShort(contentArea);
				davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+p*CELL_POINTER_SIZE);
				davisBaseTable.writeShort(contentArea);
				davisBaseTable.seek(contentArea);
				davisBaseTable.write(byteArr);
			}
			
			
			byte [] upNodeArr = dupParentRec.get(leftNodeCount) ;
			ByteBuffer b = ByteBuffer.wrap(upNodeArr);
			int lRightptr = b.getInt(0);
			int upkey = b.getInt(SIZE_INT);
			// updating right pointer of new left page
			davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
			davisBaseTable.writeInt(lRightptr);
			//updating right pointer of new right page
			if(position > parentCellCount-1)
			{
				davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
				davisBaseTable.writeInt((int) ((rightPagePtr+PAGE_SIZE)/PAGE_SIZE));
			}
			else
			{
				davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
				int ptr = davisBaseTable.readInt();
				davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
				davisBaseTable.writeInt(ptr);
			}
			
		
			/*
			davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT
					+(leftNodeCount)*CELL_POINTER_SIZE);
			short newContArea =  davisBaseTable.readShort();*/
			
			//davisBaseTable.writeShort(newContArea);
			
			insertIntoInteriorNode(parentFileLength, parentFileLength+PAGE_SIZE, upkey,parentPage);
			
			}
			// when parent page is not a root page
			else
			{
				davisBaseTable.setLength(parentFileLength+PAGE_SIZE);
				davisBaseTable.seek(parentFileLength); // left node
				davisBaseTable.writeByte(5);//  leaf page
				davisBaseTable.writeByte(0); // cell count to 0
				//read the records from parentPage
				
				// copying to right page
				for(int i = leftNodeCount+1,p=0 ; i <dupParentRec.size();i++,p++)
				{
					davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE);
					byte cellcount = davisBaseTable.readByte();
					cellcount++;
					short contentArea = (short) getcontentArea(davisBaseTable.readShort(), parentFileLength);
					byte [] byteArr =dupParentRec.get(i);
					contentArea = (short) (contentArea - byteArr.length);
					davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE);
					davisBaseTable.writeByte(cellcount);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(parentFileLength+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+p*CELL_POINTER_SIZE);
					davisBaseTable.writeShort(contentArea);
					davisBaseTable.seek(contentArea);
					davisBaseTable.write(byteArr);
				}
				//updating right pointer of left node
				byte [] upNodeArr = dupParentRec.get(leftNodeCount) ;
				ByteBuffer b = ByteBuffer.wrap(upNodeArr);
				int lRightptr = b.getInt(0);
				int upkey = b.getInt(SIZE_INT);
				davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT
						+(leftNodeCount)*CELL_POINTER_SIZE);
				short newContArea =  davisBaseTable.readShort();
				davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE);
				davisBaseTable.writeByte(leftNodeCount);
				davisBaseTable.writeShort(newContArea);
				davisBaseTable.writeInt(lRightptr);
				
				// updating right pointer to right page
				if(position > parentCellCount-1)
				{
					davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
					davisBaseTable.writeInt((int) ((rightPagePtr+PAGE_SIZE)/PAGE_SIZE));
				}
				else
				{
					davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
					int ptr = davisBaseTable.readInt();
					davisBaseTable.seek(parentFileLength+PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE);
					davisBaseTable.writeInt(ptr);
				}
				insertIntoInteriorNode((parentPage-1)*PAGE_SIZE, parentFileLength, upkey,parentPage);
				}	
			}	
	}
	private int findInsertPosition(int parentPage, int key) throws IOException {
		davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE);
		int cellCount = davisBaseTable.readByte();
		int pointer  = -1;
		int rowId = -1;
		for(int i = 0 ; i <cellCount ; i++)
		{
			davisBaseTable.seek((parentPage-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+i*CELL_POINTER_SIZE);
			pointer = davisBaseTable.readShort();
			davisBaseTable.seek(pointer+SIZE_INT);
			rowId = davisBaseTable.readInt();
			if(key < rowId)
			{
				return i+1;
			}
			
		}
		return cellCount+1;
	}		
	private void insertLeafRecord(int payloadSize, int key, List<ArrayList<String>> tableDetsList) throws IOException, ParseException {
		davisBaseTable.writeShort(payloadSize);    // Size of payload
		davisBaseTable.writeInt(key);      // rowid=1 (is also column_1)
		davisBaseTable.writeByte(tableDetsList.size());     // number of columns in addition to rowid column_1
		for(List<String> arrayList : tableDetsList)
		{
				byte code = getSerialTypeCode(arrayList.get(1),arrayList.get(0)); //type, value
				byte newCode = -1;
				if(arrayList.get(0) == null)
				{
					if(code == 4 )
					{
						
						newCode = 0;
					}
					else if(code == 5)
					{
						newCode = 1;
					}else if(code == 6 || code == 8)
					{
						newCode = 2;
					}else if(code == 7 || code == 9 || code == 10 || code == 11)
					{
						newCode = 3;
					}
				}
				else
				{
					newCode = code;
				}
				davisBaseTable.writeByte(newCode);
		}
		writeRecord(tableDetsList);
	}
	private int calculateRecordSize(List<ArrayList<String>> tabledets, String recordParam) {
		int recordsize =0;
		recordsize = CELL_POINTER_SIZE+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+tabledets.size();
		for(ArrayList<String> list : tabledets)
		{
			String detStr =  list.get(1);
			switch (detStr){
			
			case "TINYINT" : 
				recordsize += 1;
				break;
			case "SMALLINT" : 
				recordsize += 2;
				break;
			case "INT" : 
				recordsize += 4;
				break;
			case "BIGINT" : 
				recordsize += 8;
				break;
			case "REAL" : 
				recordsize += 4;
				break;
			case "DOUBLE" : 
				recordsize += 8;
				break;
			case "DATETIME" : 
				recordsize += 8;
				break;
			case "DATE" : 
				recordsize += 8;
				break;
			case "TEXT" : 
				recordsize += ("NORMAL_SIZE").equals(recordParam) ? list.get(0).toString().length(): MAX_TEXT_SIZE;
				break;
			}
		}
		return recordsize;
	}
	private int calcMaxCells(int recordSize) {
		int maxcells = (PAGE_SIZE - 4)/recordSize; // 4 = 1 pagetype ,1 cellcount , 2 contentarea
		return maxcells;
	}
	private int updateHeader( int position, int cellCount, int recordSize, int contentArea, long fileLength, String string) throws IOException {
		long filePointer = davisBaseTable.getFilePointer();
		int pointer = -1;
		for(int j = cellCount; j >= position;j--)
		{
			davisBaseTable.seek(filePointer+(j-1)*2);
			pointer = davisBaseTable.readShort();
			davisBaseTable.writeShort(pointer);
			if(j == position)
			{
				davisBaseTable.seek(filePointer+(j-1)*2);
				pointer = getcontentArea(contentArea,fileLength) - recordSize+CELL_POINTER_SIZE;
				davisBaseTable.writeShort(pointer);
				if("LEAF".equals(string))
					{
						davisBaseTable.seek(filePointer-3); // start of cellcount
					}
				else
				{
					davisBaseTable.seek(filePointer-7);
				}
				davisBaseTable.writeByte(cellCount);
				davisBaseTable.writeShort(pointer);
			}
		}
		return pointer;
		
	}
	private int getcontentArea(int contentArea, long fileLength) {
		if(contentArea == 0)
		{
			return (int) ((PAGE_SIZE)+fileLength);
		}
		return contentArea;
	}
	private byte getSerialTypeCode(Object object, Object object2) {
		String detStr = (String)object;
		byte serialCode = -1;
		switch (detStr){
		
		case "TINYINT" : {
			serialCode = 4;
			break;
		}
		case "SMALLINT" : {
			serialCode = 5;
			break;
		}
		case "INT" : {
			serialCode = 6;
			break;
		}
		case "BIGINT" : {
			serialCode = 7;
			break;
		}
		case "REAL" : {
			serialCode = 8;
			break;
		}
		case "DOUBLE" : {
			serialCode = 9;
			break;
		}
		case "DATETIME" : {
			serialCode = 10;
			break;
		}
		case "DATE" : {
			serialCode = 11;
			break;
		}
		case "TEXT" : {
			serialCode = 12;
			if(object2 != null)
			{
			String txtLength = (String)object2;
			serialCode += txtLength.length();
			}
			break;
		}
		}
		return serialCode;
	}
	private void writeRecord(List<ArrayList<String>> tabledets) throws IOException, ParseException {
		for(List<String> arrayList : tabledets)
		{
			String detStr =  (String)arrayList.get(1);
			switch (detStr){
			
			case "TINYINT" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeByte(0);
				}
				else
				{
				davisBaseTable.writeByte(Integer.parseInt(arrayList.get(0)));
				}
				break;
			case "SMALLINT" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeShort(0);
				}
				else
				{
				davisBaseTable.writeShort(Short.parseShort(arrayList.get(0)));
				}
				break;
			case "INT" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeInt(0);
				}
				else
				{
				davisBaseTable.writeInt(Integer.parseInt(arrayList.get(0)));
				}
				break;
			case "BIGINT" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeLong(0);
				}else
				{
				davisBaseTable.writeLong(Long.parseLong(arrayList.get(0)));
				}
				break;
			case "REAL" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeFloat(0);
				}else
				{
				davisBaseTable.writeFloat(Float.parseFloat(arrayList.get(0)));
				}
				break;
			case "DOUBLE" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeDouble(0);
				}else
				{
				davisBaseTable.writeDouble(Double.parseDouble(arrayList.get(0)));
				}
				break;
			case "DATETIME" :
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeLong(0);
				}else
				{
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
					Date d = formatter.parse(arrayList.get(0));
					long timestamp = d.getTime();
				davisBaseTable.writeLong(timestamp);
				}
				break;
			case "DATE" :
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeLong(0);
				}else
				{
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
					Date d = formatter.parse(arrayList.get(0));
					long timestamp = d.getTime();
					davisBaseTable.writeLong(timestamp);
				}
				break;
			case "TEXT" : 
				if("NULL".equalsIgnoreCase(arrayList.get(0)))
				{
					davisBaseTable.writeBytes("");
				}else
				{
					
					davisBaseTable.writeBytes(arrayList.get(0));
				}
				
				break;			
			
			}
			
			}
		}

	@SuppressWarnings("unused")
	public void getColDetails(String key,int pageNum,Map<Integer,ArrayList<ArrayList<Object>>> colsDets) throws IOException {
		davisBaseTable.seek((pageNum-1)*PAGE_SIZE);
		int pageType = davisBaseTable.readByte();
		int cellCount = davisBaseTable.readByte();
		int contentArea = davisBaseTable.readShort();
		
		if(pageType == 13)
		{
			for(int i = 1 ; i <= cellCount ; i++)
			{
				ArrayList<ArrayList<Object>> detsList = new ArrayList<ArrayList<Object>>();
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(i-1)*CELL_POINTER_SIZE);				
				int cellPointer = davisBaseTable.readShort();
				//int cellpointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer); // 2 for payload
				int payload = davisBaseTable.readShort();
				int rowId = davisBaseTable.readInt();
				int otherColCount = davisBaseTable.readByte();
				byte [] tabNameArr = new byte[key.length()];
				davisBaseTable.seek(davisBaseTable.getFilePointer()+otherColCount);
				davisBaseTable.read(tabNameArr);
				String tabName = new  String(tabNameArr);
				if(tabName.equals(key))
				{
					byte [] serialCodeArr = new byte[otherColCount];
					for(int j = 0; j < otherColCount ; j++)
					{
						davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
						serialCodeArr[j]=davisBaseTable.readByte();
					}
						for(int k = 0 ; k < serialCodeArr.length ; k++)
						{
							ArrayList<Object> dets = getCols(serialCodeArr[k]);
							detsList.add(dets);
						}
						colsDets.put(rowId, detsList);
					}
				}
			}
		else
		{
			int rightpointer = davisBaseTable.readInt();
			for(int i = 1 ; i <= cellCount ; i++)
			{
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+(i-1)*CELL_POINTER_SIZE);
				int cellPointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer);
				int lftptr = davisBaseTable.readInt();
				getColDetails(key,lftptr,colsDets);
			}
			getColDetails(key, rightpointer,colsDets);
		}
}
	private ArrayList<Object> getCols(int serialCode) throws IOException {
		ArrayList<Object> dets= new ArrayList<Object>();
		
		switch (serialCode){
		case 0 : 
			davisBaseTable.read(new byte[1]);
			dets.add(null);
			dets.add("NULL");
			break;
		case 1 :
			davisBaseTable.read(new byte[2]);
			dets.add(null);
			dets.add("NULL");
			break;
		case 2 :
			davisBaseTable.read(new byte[4]);
			dets.add(null);
			dets.add("NULL");
			break;
		case 3 :
			davisBaseTable.read(new byte[8]);
			dets.add(null);
			dets.add("NULL");
			break;
		case 4 :
			
			dets.add(davisBaseTable.readByte());
			dets.add("TINYINT");
			break;
		
		case 5 : 
			dets.add(davisBaseTable.readShort());
			dets.add("SMALLINT");
			break;
		
		case 6 : 
			dets.add(davisBaseTable.readInt());
			dets.add("INT");
			break;
		
		case 7 : 
			dets.add(davisBaseTable.readLong());
			dets.add("BIGINT");
			break;
		
		case 8 : 
			dets.add(davisBaseTable.readFloat());
			dets.add("REAL");
			break;
		
		case 9 : 
			dets.add(davisBaseTable.readDouble());
			dets.add("DOUBLE");
			break;
		
		case 10 : 
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
			Date date = new Date(davisBaseTable.readLong());
			dets.add(formatter.format(date));
			dets.add("DATETIME");
			break;
		
		case 11 : 
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
			Date date1 = new Date(davisBaseTable.readLong());
			dets.add(formatter1.format(date1));
			dets.add("DATE");
			break;
		
		default :
			byte [] bArr = new byte[serialCode-12];
			davisBaseTable.read(bArr);
			dets.add(new String(bArr));
			dets.add("TEXT");
			break;
		}
		return dets;
	}
	@SuppressWarnings("unused")
	/**
	 * 
	 * @param colPosList - columns position
	 * @param whereList - COLUMNNAME -0, operation -1, value - 2 , type of column - 3, position of column in table - 4
	 * @param pageNum
	 * @param colsDets - record details
	 * @throws IOException
	 * @throws ParseException
	 */
	public void searchForRecords(ArrayList<Integer> colPosList, ArrayList<String> whereList,int pageNum,Map<Integer,ArrayList<ArrayList<Object>>> colsDets) throws IOException, ParseException {
		davisBaseTable.seek((pageNum-1)*PAGE_SIZE);
		int pageType = davisBaseTable.readByte();
		int cellCount = davisBaseTable.readByte();
		int contentArea = davisBaseTable.readShort();
		
		if(pageType == 13)
		{
			for(int i = 1 ; i <= cellCount ; i++)
			{
				ArrayList<ArrayList<Object>> detsList = new ArrayList<ArrayList<Object>>();
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(i-1)*CELL_POINTER_SIZE);				
				int cellPointer = davisBaseTable.readShort();
				//int cellpointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer); // 2 for payload
				int payload = davisBaseTable.readShort();
				int rowId = davisBaseTable.readInt();
				int otherColCount = davisBaseTable.readByte();
				if(whereList.isEmpty())
				{
					ArrayList<Object> dets1 = new ArrayList<Object>();
					 if(colPosList.contains(1))
						{
						 davisBaseTable.seek(cellPointer+PAYLOAD_SIZE);
						 int id = davisBaseTable.readInt();
						 dets1.add(id);
						 dets1.add("INT");
						 detsList.add(dets1);
						}
					byte [] serialCodeArr = new byte[otherColCount];
					for(int j = 0; j < otherColCount ; j++)
					{
						davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
						serialCodeArr[j]=davisBaseTable.readByte();
					}
						for(int k = 0 ; k < serialCodeArr.length ; k++)
						{
							ArrayList<Object> dets = getCols(serialCodeArr[k]);
							if(colPosList.isEmpty())
							{
								detsList.add(dets);
							}
							else if(colPosList.contains(k+1)){
								detsList.add(dets);
								}
						}
						colsDets.put(rowId, detsList);
					} // end of whereList is empty if
				
				else
				{
					int position = Integer.parseInt(whereList.get(4));
					ArrayList<Object> keyList = null;
					boolean isSelected = false;
					if(position == 0)
					{
						keyList = new ArrayList<Object>();
						keyList.add(rowId);
						keyList.add("INT");
						isSelected = getcolDetsforWhere(keyList,whereList.get(2),whereList.get(1));
						
					}
					else
					{
					int offset = 0;
					davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+(position-1));
					int scode = davisBaseTable.readByte();
					for(int j = 0; j < position-1 ; j++)
					{
						davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
						int code = davisBaseTable.readByte();
						if(code == 0 || code == 4)
						{
							offset +=1;
						}
						else if(code == 1 || code == 5)
						{
							offset += 2;
						}else if(code == 2 || code == 6 || code == 8)
						{
							offset += 4;
						}else if(code == 3 || code == 7 || code == 10 || code == 11)
						{
							offset += 8;
						}else
						{
							offset += (code-12);
						}
					}
					 davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+otherColCount+offset);
					 keyList = getCols(scode);
					 isSelected =  getcolDetsforWhere(keyList,whereList.get(2),whereList.get(1));
					} 
					if(isSelected)
						{
						if(colPosList.contains(1))
						{
						 ArrayList<Object> dets1 = new ArrayList<Object>();
						 davisBaseTable.seek(cellPointer+PAYLOAD_SIZE);
						 int id = davisBaseTable.readInt();
						 dets1.add(id);
						 dets1.add("INT");
						 detsList.add(dets1);
						}
							byte [] serialCodeArr = new byte[otherColCount];
							for(int j = 0; j < otherColCount ; j++)
							{
								davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
								serialCodeArr[j]=davisBaseTable.readByte();
							}
								for(int k = 0 ; k < serialCodeArr.length ; k++)
								{
									ArrayList<Object> dets = getCols(serialCodeArr[k]);
									if(colPosList.isEmpty())
									{
										detsList.add(dets);
									}
									else if(colPosList.contains(k+1))
									{
									detsList.add(dets);
									}
								}
								if(!detsList.isEmpty())
								{
								colsDets.put(rowId, detsList);
								}
							}
						}
					}
		}
		else
		{
			int rightpointer = davisBaseTable.readInt();
			boolean flag = true;
			for(int i = 1 ; i <= cellCount ; i++)
			{
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+(i-1)*CELL_POINTER_SIZE);
				int cellPointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer);
				int lftptr = davisBaseTable.readInt();
				searchForRecords(colPosList,whereList,lftptr,colsDets);
			}
			searchForRecords(colPosList,whereList,rightpointer,colsDets);
		}
}
	/**
	 * Doesn't support string type and rowId update
	 * @param colPosition
	 * @param whereList
	 * @param pageNum
	 * @throws Exception 
	 */
	@SuppressWarnings("unused")
	void updateRecords(Integer colPosition, ArrayList<String> whereList,int pageNum,String value) throws Exception
	{
		davisBaseTable.seek((pageNum-1)*PAGE_SIZE);
		int pageType = davisBaseTable.readByte();
		int cellCount = davisBaseTable.readByte();
		int contentArea = davisBaseTable.readShort();
		if(pageType == 13)
		{
			for(int i = 1 ; i <= cellCount ; i++)
			{
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+(i-1)*CELL_POINTER_SIZE);				
				int cellPointer = davisBaseTable.readShort();
				//int cellpointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer); // 2 for payload
				int payload = davisBaseTable.readShort();
				int rowId = davisBaseTable.readInt();
				int otherColCount = davisBaseTable.readByte();
				if(colPosition == 0)
				{
					throw new Exception("Not handling rowId updation right now");
				}
				if(whereList.isEmpty())
				{
					
					byte [] serialCodeArr = new byte[otherColCount];
					for(int j = 0; j < otherColCount ; j++)
					{
						davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
						serialCodeArr[j]=davisBaseTable.readByte();
					}
					for(int k = 0 ; k < serialCodeArr.length ; k++)
						{
							if(k == colPosition-1)
							{
							updateCols(serialCodeArr[k],true,value);
							}
							else
							{
								updateCols(serialCodeArr[k],false,value);
							}
						}
					} // end of whereList is empty if
				else
				{
					ArrayList<Object> keyList = null;
					boolean isSelected = false;
					int conditionPosition = Integer.parseInt(whereList.get(4));
					if(conditionPosition == 1)
					{
						keyList = new ArrayList<Object>();
						keyList.add(rowId);
						keyList.add("INT");
						isSelected = getcolDetsforWhere(keyList,whereList.get(2),whereList.get(1));
					}
					else
					{
						int offset = 0;
						davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+(conditionPosition-1));
						int scode = davisBaseTable.readByte();
					for(int j = 0; j < conditionPosition-1 ; j++)
					{
						davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
						int code = davisBaseTable.readByte();
						if(code == 0 || code == 4)
						{
							offset +=1;
						}
						else if(code == 1 || code == 5)
						{
							offset += 2;
						}else if(code == 2 || code == 6 || code == 8)
						{
							offset += 4;
						}else if(code == 3 || code == 7 || code == 10 || code == 11)
						{
							offset += 8;
						}else
						{
							offset += (code-12);
						}
					}
					 davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+otherColCount+offset);
					 keyList = getCols(scode);
					 isSelected =  getcolDetsforWhere(keyList,whereList.get(2),whereList.get(1));
					} 
					if(isSelected)
						{
							byte [] serialCodeArr = new byte[otherColCount];
							for(int j = 0; j < otherColCount ; j++)
							{
								davisBaseTable.seek(cellPointer+PAYLOAD_SIZE+SIZE_INT+OTHER_COLUMNS_COUNT+j);
								serialCodeArr[j]=davisBaseTable.readByte();
							}
								for(int k = 0 ; k < serialCodeArr.length ; k++)
								{
									if(k == colPosition-2)
									{
									updateCols(serialCodeArr[k],true,value);
									}
									else
									{
										updateCols(serialCodeArr[k],false,value);
									}
								}
							}
						}
					}
		}
		else
		{
			int rightpointer = davisBaseTable.readInt();
			for(int i = 1 ; i <= cellCount ; i++)
			{
				davisBaseTable.seek((pageNum-1)*PAGE_SIZE+PAGE_TYPE_SIZE+CELL_COUNT_SIZE+CONTENT_AREA_SIZE+SIZE_INT+(i-1)*CELL_POINTER_SIZE);
				int cellPointer = davisBaseTable.readShort();
				davisBaseTable.seek(cellPointer);
				int lftptr = davisBaseTable.readInt();
				updateRecords(colPosition,whereList,lftptr,value);
			}
			updateRecords(colPosition,whereList,rightpointer,value);
		}

	}
	private void updateCols(byte serialCode, boolean isUpdate, String value) throws IOException, ParseException {
		long pointer = davisBaseTable.getFilePointer();
		switch (serialCode){
		case 0 : 
			davisBaseTable.read(new byte[1]);
			break;
		case 1 :
			davisBaseTable.read(new byte[2]);
			break;
		case 2 :
			davisBaseTable.read(new byte[4]);
			break;
		case 3 :
			davisBaseTable.read(new byte[8]);
			break;
		case 4 :
			davisBaseTable.readByte();
			if(isUpdate)
			{
			davisBaseTable.seek(pointer);
			davisBaseTable.writeByte(Byte.parseByte(value));
			}
			break;
		
		case 5 : 
			davisBaseTable.readShort();
			if(isUpdate)
			{
			davisBaseTable.seek(pointer);
			davisBaseTable.writeByte(Short.parseShort(value));
			}
			break;
		
		case 6 : 
			
			davisBaseTable.readInt();
			if(isUpdate)
			{
			davisBaseTable.seek(pointer);
			davisBaseTable.writeInt(Integer.parseInt(value));
			}
			break;
		
		case 7 : 
			davisBaseTable.readLong();
			if(isUpdate)
			{
			davisBaseTable.seek(pointer);
			davisBaseTable.writeLong(Long.parseLong(value));
			}
			break;
		
		case 8 : 
			davisBaseTable.readFloat();
			if(isUpdate)
			{
			davisBaseTable.seek(pointer);
			davisBaseTable.writeFloat(Float.parseFloat(value));
			}
			break;
		
		case 9 : 
			davisBaseTable.readDouble();
			if(isUpdate)
			{
			davisBaseTable.seek(pointer);
			davisBaseTable.writeDouble(Double.parseDouble(value));
			}
			break;
		
		case 10 : 
			davisBaseTable.readLong();
			if(isUpdate)
			{
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
			Date date = formatter.parse(value);
			davisBaseTable.seek(pointer);
			davisBaseTable.writeLong(date.getTime());
			}
			break;
		
		case 11 : 
			davisBaseTable.readLong();
			if(isUpdate)
			{
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
			Date date1 = formatter1.parse(value);
			davisBaseTable.seek(pointer);
			davisBaseTable.writeLong(date1.getTime());
			}
			break;
		
		default :
			byte [] bArr = new byte[serialCode-12];
			davisBaseTable.read(bArr);
			break;
		}
	}

	private boolean getcolDetsforWhere(ArrayList<Object> keyList, String value, String operator) throws ParseException {
		boolean flag = false;
		String type = (String) keyList.get(1);
		if(type.equalsIgnoreCase("DATETIME"))
		{
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
			Date d = formatter.parse(value);
			long timestamp = d.getTime(); 
			value = timestamp+"";
		}
		if(type.equalsIgnoreCase("DATE"))
		{
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date d = formatter.parse(value);
			long timestamp = d.getTime(); 
			value = timestamp+"";
		}
		if("TINYINT".equalsIgnoreCase(type) || "SMALLINT".equalsIgnoreCase(type) 
				|| "INT".equalsIgnoreCase(type))
		{
			switch(operator)
			{
			case "<":
					flag = (int) keyList.get(0) < Integer.parseInt(value) ? true :false;
					break;
			case ">":
					flag = (int) keyList.get(0) > Integer.parseInt(value) ? true :false;
					break;
			case "<=":
					flag = (int) keyList.get(0) <= Integer.parseInt(value) ? true :false;
					break;
			case ">=":
					flag = (int) keyList.get(0) >= Integer.parseInt(value) ? true :false;
					break;
			case "=":	
					flag = (int)keyList.get(0) == Integer.parseInt(value) ? true :false;
					break;
			case "<>":
				flag = (int) keyList.get(0) != Integer.parseInt(value) ? true :false;
				break;
			}
		}
		else if("TINYINT".equalsIgnoreCase(type) || "DATETIME".equalsIgnoreCase(type) || "DATE".equalsIgnoreCase(type))
		{
			switch(operator)
			{
			case "<":
					flag = (long)keyList.get(0) < Long.parseLong(value) ? true :false;
					break;
			case ">":
					flag = (long) keyList.get(0) > Long.parseLong(value) ? true :false;
					break;
			case "<=":
					flag = (long) keyList.get(0) <= Long.parseLong(value) ? true :false;
					break;
			case ">=":
					flag =(long) keyList.get(0) >= Long.parseLong(value) ? true :false;
					break;
			case "=":	
					flag = (long) keyList.get(0) == Long.parseLong(value) ? true :false;
					break;
			case "<>":
				flag = (long) keyList.get(0) != Long.parseLong(value) ? true :false;
				break;
			}
		}
		else if("REAL".equalsIgnoreCase(type))
		{
			float key = (float) keyList.get(0);
			float value1 = Float.parseFloat(value);
			switch(operator)
			{
			case "<":
					flag = key < value1 ? true :false;
					break;
			case ">":
					flag = key > value1 ? true :false;
					break;
			case "<=":
				flag = key <= value1 ? true :false;
					break;
			case ">=":
				flag = key >= value1 ? true :false;
					break;
			case "=":	
				flag = key  == value1 ? true :false;
					break;
			case "<>":
				flag = key != value1 ? true :false;
				break;
			}
			
		}
		else if("DOUBLE".equalsIgnoreCase(type))
		{
			double key = (double) keyList.get(0);
			double value1 = Double.parseDouble(value);
			switch(operator)
			{
			case "<":
					flag = key < value1 ? true :false;
					break;
			case ">":
					flag = key > value1 ? true :false;
					break;
			case "<=":
				flag = key <= value1 ? true :false;
					break;
			case ">=":
				flag = key >= value1 ? true :false;
					break;
			case "=":	
				flag = key  == value1 ? true :false;
					break;
			case "<>":
				flag = key != value1 ? true :false;
				break;
			}
			
		}
		else if("TEXT".equalsIgnoreCase(type))
		{
			String key = (String) keyList.get(0);
			switch(operator)
			{
			case "=":	
				flag = key.equals(value) ? true :false;
					break;
			case "<>":
				flag = key.equals(value) ? false :true;
				break;
			
			default :
				flag = false;
			
		}
		
	}
		return flag;
}

}