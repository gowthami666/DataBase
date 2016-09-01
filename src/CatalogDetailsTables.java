import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class CatalogDetailsTables {

	
	public void InsertInCatalogTable(ArrayList<String> tableDetailsList) {
		
			// insertion in Davistable
			RandomAccessFile davisbaseTable = null;
			try {
				davisbaseTable = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
				BPlusTree davisTableTree = new BPlusTree(davisbaseTable);
				int rowId = davisTableTree.getRowId(1);
				List<ArrayList<String>> suplist = new ArrayList<ArrayList<String>>();
				suplist.add(tableDetailsList);
				davisTableTree.insertIntoLeaf(rowId, suplist);
				// 1 represents page-1
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
					davisbaseTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	}
	public void InsertInCatalogCols(ArrayList<String> colsDetails) throws IOException {
		RandomAccessFile davisbaseCol = null;
		try {
			davisbaseCol = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			BPlusTree davisColsTree = new BPlusTree(davisbaseCol);
			int rowId = davisColsTree.getRowId(1);
			List<ArrayList<String>> suplist = new ArrayList<ArrayList<String>>();
			for(int i = 0;i< colsDetails.size();i++)
			{
				ArrayList<String> cols = new ArrayList<String>();
				cols.add(colsDetails.get(i));
				cols.add(getDataType(i));
				suplist.add(cols);
			}
			davisColsTree.insertIntoLeaf(rowId, suplist);
			
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
			davisbaseCol.close();
		}
		
		
	}
	private String getDataType(int i) {
		String dataType = null;
		switch(i)
		{
		case 0 : dataType = "TEXT";
					break;
		case 1 : dataType = "TEXT";
					break;
		case 2 : dataType = "TEXT";
					break;
		case 3 : dataType = "TINYINT";
					break;
		case 4 : dataType = "TEXT";
					break;
		}
		return dataType;
	}

}
