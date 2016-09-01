import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


/**
 *
 * @author Chris Irwin Davis
 * @version 1.0
 */
public class DavisBaseBinaryFile {
	static int pageSize = 512;
	
	/* calculation = pleaf(recordPointer+recordsize)+4 <= 512
	 * record pointer = 2 bytes
	 * recordsize = 4(int) + text (max 115))+4(leafnode-1 byte,number of cells - 1 byte ,content area - 2 bytes)
	 * pleaf *(2+119) +4 < =512 , pleaf *(121) <= 508 , pleaf <= 4.1 , pleaf = 4
	 */
	 void initializeDataStore()
	{
		try {
			File dataDir = new File("data");
			if(!dataDir.exists())
			{
				dataDir.mkdir();
				
				try {
					RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
					/* Initially, the file is one page in length */
					davisbaseTablesCatalog.setLength(pageSize);
					/* Set file pointer to the beginnning of the file */
					davisbaseTablesCatalog.seek(0);
					/* Write 0x0D to the page header to indicate that it's a leaf page.  
					 * The file pointer will automatically increment to the next byte. */
					davisbaseTablesCatalog.write(0x0D);
					/* Write 0x00 (although its value is already 0x00) to indicate there 
					 * are no cells on this page */
					davisbaseTablesCatalog.write(0x00);
					davisbaseTablesCatalog.close();
				}
				catch (Exception e) {
					System.out.println("Unable to create the database_tables file");
					System.out.println(e);
				}

				/** Create davisbase_columns systems catalog */
				try {
					RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
					/** Initially the file is one page in length */
					davisbaseColumnsCatalog.setLength(pageSize);
					davisbaseColumnsCatalog.seek(0);       // Set file pointer to the beginnning of the file
					/* Write 0x0D to the page header to indicate a leaf page. The file 
					 * pointer will automatically increment to the next byte. */
					davisbaseColumnsCatalog.write(0x0D);
					/* Write 0x00 (although its value is already 0x00) to indicate there 
					 * are no cells on this page */
					davisbaseColumnsCatalog.write(0x00); 
					davisbaseColumnsCatalog.close();
				}
				catch (Exception e) {
					System.out.println("Unable to create the database_columns file");
					System.out.println(e);
				}
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}
	}
	
	
	 void deleteDataDir()
	 {
		 try {
				File dataDir = new File("data");
				dataDir.mkdir();
				String[] oldTableFiles;
				oldTableFiles = dataDir.list();
				for (int i=0; i<oldTableFiles.length; i++) {
					File anOldFile = new File(dataDir, oldTableFiles[i]); 
					anOldFile.delete();
				}
			}
			catch (SecurityException se) {
				System.out.println("Unable to create data container directory");
				System.out.println(se);
			}
	 }
	 
	 void createNewTableFile(String fileName)
	 {
		 RandomAccessFile tableFile;
		try {
			tableFile = new RandomAccessFile("data/"+fileName+".tbl", "rw");
			tableFile.setLength(pageSize);
			tableFile.seek(0);
			tableFile.writeByte(13);
			tableFile.writeByte(0);
			tableFile.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			/* Initially, the file is one page in length */ 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	 }
	
	/**
	 * <p>This method is used for debugging.
	 * @param ram is an instance of {@link RandomAccessFile}. 
	 * <p>This method will display the binary contents of the file to Stanard Out (stdout)
	 */
	 void displayBinaryHex(RandomAccessFile ram) {
		try {/*
			System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
			ram.seek(0);
			long size = ram.length();
			int row = 1;
			System.out.print("0000\t0x0000\t");
			while(ram.getFilePointer() < size) {
				System.out.print(String.format("%02X ", ram.readByte()));
				// System.out.print(ram.readByte() + " ");
				if(row % 16 == 0) {
					System.out.println();
					System.out.print(String.format("%04d\t0x%04X\t", row, row));
				}
				row++;
			}		
		*/}
		catch (Exception e) {
			System.out.println(e);
		}
	}
}