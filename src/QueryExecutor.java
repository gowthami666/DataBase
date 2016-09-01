import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.event.ListSelectionEvent;

public class QueryExecutor {

	@SuppressWarnings("resource")
	public void dropTable(ArrayList<String> tableNmLst) {
		//RandomAccessFile randomAccessFile = null;
		RandomAccessFile randomAccessFile2 = null;
		//ArrayList<String> tableNmLst = new ArrayList<String>();
		try {
				randomAccessFile2 = new RandomAccessFile("data/davisbase_deleteTbl.tbl", "rw");
				int count = 0;
				//davisbaseTable = new RandomAccessFile("data/davisbase_deleteTbl.tbl", "rw");
				BPlusTree davisTableTree = new BPlusTree(randomAccessFile2);
				int rowId = davisTableTree.getRowId(1);
				List<ArrayList<String>> suplist = new ArrayList<ArrayList<String>>();
				//tableNmLst.add(tableName);
				suplist.add(tableNmLst);
				davisTableTree.insertIntoLeaf(rowId, suplist);
			
			//randomAccessFile1.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		{
			try {
				//randomAccessFile.close();
				randomAccessFile2.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	@SuppressWarnings("resource")
	public void insertInTable(String tableName, ArrayList<Object> values) {
		RandomAccessFile randomAccessFile = null;
		RandomAccessFile randomAccessFile1 = null;
		try {
			randomAccessFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
			Map<Integer,ArrayList<ArrayList<Object>>> colsDets = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
			ArrayList<String> ColTablewhereList = new ArrayList<String>();
			ColTablewhereList.add("TABLENAME");
			ColTablewhereList.add("=");
			ColTablewhereList.add(tableName.toUpperCase());
			ColTablewhereList.add("TEXT");
			ColTablewhereList.add("1");// 2 - for tableName attribute in 
			bPlusTree.searchForRecords(new ArrayList<Integer>(),ColTablewhereList,1,colsDets);
			//bPlusTree.getColDetails(tableName,1,colsDets);
			//randomAccessFile.close();
			if(colsDets.isEmpty())
			{
				throw new Exception("NO TABLE EXISTS WITH NAME "+tableName);
			}
			
			randomAccessFile1 = new RandomAccessFile("data/"+tableName+".tbl", "rw");
			int count = 0;
			List<ArrayList<String>> suplist = new ArrayList<ArrayList<String>>();
			for( ArrayList<ArrayList<Object>> colsList :colsDets.values())
			{
				String nullValue = (String) (colsList.get(4)).get(0);
					if(nullValue.equalsIgnoreCase("yes") && values.get(count)== null)
					{
						throw new Exception("NOT NULL CONSTRAINT FAILED");
					}
					ArrayList<String> recordcolDet = createTableDetails(values.get(count),(String) ((ArrayList<Object>) colsList.get(2)).get(0));
					if(count != 0)
					{
					suplist.add(recordcolDet);
					}
					count++;
				}
			BPlusTree bPlusTree2 = new BPlusTree(randomAccessFile1);
			bPlusTree2.insertIntoLeaf(Integer.parseInt((String) values.get(0)), suplist);
			//randomAccessFile1.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		{
			try {
				randomAccessFile.close();
				randomAccessFile1.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		}

	private ArrayList<String> createTableDetails(Object object, String dataType) {
		ArrayList<String> recordcolDet = new ArrayList<String>();
		recordcolDet.add((String) object);
		recordcolDet.add(dataType);
		return recordcolDet;
	}

	public ArrayList<Map> getSelectedRecords(String tableName, ArrayList<String> columnList, ArrayList<String> whereList) {
		RandomAccessFile randomAccessFile = null;
		RandomAccessFile randomAccessFile1 = null;
		try {
			randomAccessFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
			Map<Integer,ArrayList<ArrayList<Object>>> colsDets = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
			Map<Integer,ArrayList<ArrayList<Object>>> recordDets = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
			ArrayList<String> ColTablewhereList = new ArrayList<String>();
			ColTablewhereList.add("TABLENAME");
			ColTablewhereList.add("=");
			ColTablewhereList.add(tableName.toUpperCase());
			ColTablewhereList.add("TEXT");
			ColTablewhereList.add("1");// 2 - for tableName attribute in 
			bPlusTree.searchForRecords(new ArrayList<Integer>(),ColTablewhereList,1,colsDets);
			//bPlusTree.getColDetails(tableName,1,colsDets);
			if(colsDets.isEmpty())
			{
				throw new Exception("NO TABLE EXISTS WITH NAME "+tableName);
			}
			/**
			 * if wild card * then - select the data type and position
			 * else - select the data type and position
			 * where clause operator - datatype, position to check, value
			 */
			ArrayList<Integer> colPosList = new ArrayList<Integer>(); 
			if(columnList.isEmpty())
			{
				for( ArrayList<ArrayList<Object>> colsList :colsDets.values())
				{
					//String key = (String) (colsList.get(2)).get(0); 
					//String position = (String) (colsList.get(2)).get(0);// dataType of column
					//ArrayList<Object> abc = colsList.get(3);
					//String def = (String) abc.get(0);
					
					byte abc =  (byte) (colsList.get(3)).get(0);
					int def = abc;
					//System.out.println("hi");
					colPosList.add(def); // dataType and position
				}
			}
			else
			{
				for( ArrayList<ArrayList<Object>> colsList :colsDets.values())
				{
					String columnName = (String) colsList.get(1).get(0);
					if(columnList.contains(columnName))
					{
						byte abc =  (byte) (colsList.get(3)).get(0);
						int def = abc;
						//System.out.println("hi");
						colPosList.add(def); 
					}
				}
			}	
			if(!whereList.isEmpty())
			{
				for( ArrayList<ArrayList<Object>> colsList :colsDets.values())
				{
					String columnName = (String) colsList.get(1).get(0); //columnName
					if(whereList.contains(columnName))
					{
						whereList.add((String) (colsList.get(2)).get(0)); //dataType

						byte abc =  (byte) (colsList.get(3)).get(0);
						int def = abc;
						whereList.add(def+""); // position
					}
				}
			}
			
				randomAccessFile1 = new RandomAccessFile("data/"+tableName+".tbl", "rw");
				BPlusTree bPlusTree2 = new BPlusTree(randomAccessFile1);
				Collections.sort(colPosList);
				bPlusTree2.searchForRecords(colPosList,whereList,1, recordDets);
				ArrayList<Map> result = new ArrayList<Map>();
				result.add(colsDets);
				result.add(recordDets);
				return result;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		{
			try {
				randomAccessFile.close();
				randomAccessFile1.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	public void updateRecords(String tableName, String columnName, ArrayList<String> whereList, String value)
	{
		RandomAccessFile randomAccessFile = null;
		RandomAccessFile randomAccessFile1 = null;
		try {
			randomAccessFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			BPlusTree bPlusTree = new BPlusTree(randomAccessFile);
			Map<Integer,ArrayList<ArrayList<Object>>> colsDets = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
			ArrayList<String> ColTablewhereList = new ArrayList<String>();
			ColTablewhereList.add("TABLENAME");
			ColTablewhereList.add("=");
			ColTablewhereList.add(tableName.toUpperCase());
			ColTablewhereList.add("TEXT");
			ColTablewhereList.add("1");// 2 - for tableName attribute in 
			bPlusTree.searchForRecords(new ArrayList<Integer>(),ColTablewhereList,1,colsDets);
			//bPlusTree.getColDetails(tableName,1,colsDets);
			if(colsDets.isEmpty())
			{
				throw new Exception("NO TABLE EXISTS WITH NAME "+tableName);
			}
			int colPosition = 0;
			for( ArrayList<ArrayList<Object>> colsList :colsDets.values())
				{
					String colName= (String) colsList.get(1).get(0);
					
					if(colName.equalsIgnoreCase(columnName))
					{
					 
						colPosition =  (byte) (colsList.get(3)).get(0);
					}
				}	
			if(!whereList.isEmpty())
			{
				for( ArrayList<ArrayList<Object>> colsList :colsDets.values())
				{
					String colName = (String) colsList.get(1).get(0); //columnName
					if(whereList.contains(colName))
					{
						whereList.add((String) (colsList.get(2)).get(0)); //dataType
						
						byte colPos = 		((byte) (colsList.get(3)).get(0)); 
								whereList.add(colPos+"");
					}
				}
			}
			
			randomAccessFile1 = new RandomAccessFile("data/"+tableName+".tbl", "rw");
			BPlusTree bPlusTree2 = new BPlusTree(randomAccessFile1);
			bPlusTree2.updateRecords(colPosition, whereList,1 , value);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void showTables() throws IOException, ParseException {
		//Show all records in davisbase_tables.tbl
		RandomAccessFile randomAccessFile1 = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
		BPlusTree bPlusTree2 = new BPlusTree(randomAccessFile1);
		Map<Integer,ArrayList<ArrayList<Object>>> recordDets = new HashMap<Integer, ArrayList<ArrayList<Object>>>();
		bPlusTree2.searchForRecords(new ArrayList<Integer>(),new ArrayList<String>(),1, recordDets);
		ArrayList<String> header = new ArrayList<String>();
		header.add("ROWID");
		header.add("TABLE NAME");
		displayColumns(header,recordDets);
		System.out.println("");
	}

	private void displayColumns(ArrayList<String> header, Map<Integer, ArrayList<ArrayList<Object>>> recordDets) {
		System.out.println();
		for(String headerCols : header)
		{
			System.out.print(headerCols+"\t");
		}
		System.out.println();
		for(Integer rowId: recordDets.keySet())
		{
			System.out.print(rowId+"\t");
			for(ArrayList<Object> rows : recordDets.get(rowId))
			{
				System.out.print(rows.get(0)+"\t");
			}
			System.out.println();
		}
		
	}
}
