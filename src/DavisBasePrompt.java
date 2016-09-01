import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;



/**
 * @author Chris Irwin Davis
 * @version 1.0
 * <b>This is an example of how to read/write binary data files using RandomAccessFile class</b>
 *
 */
public class DavisBasePrompt {

	
	static String prompt = "davisql> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
    public static void main(String[] args) {

    	DavisBasePrompt basePrompt = new DavisBasePrompt();
		/* Initialize the datastore*/
    	DavisBaseBinaryFile baseBinaryFile = new DavisBaseBinaryFile();
    	baseBinaryFile.initializeDataStore();
    	/* Display the welcome screen */
    	basePrompt.splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 

		while(!userCommand.equals("exit")) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			basePrompt.parseUserCommand(userCommand,baseBinaryFile);
		}
		System.out.println("Exiting...");


	}

	/**
	 *  Display the splash screen
	 */
	public void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		// version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public void help() {
			System.out.println(line("*",80));
			System.out.println("SUPPORTED COMMANDS");
			System.out.println("All commands below are case insensitive");
			System.out.println();
			System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
			System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
			System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
			System.out.println("\tVERSION;                                         Show the program version.");
			System.out.println("\tHELP;                                            Show this help information");
			System.out.println("\tEXIT;                                            Exit the program");
			System.out.println();
			System.out.println();
			System.out.println(line("*",80));
		}

	/** Display the DavisBase version */
	public void version() {
		System.out.println("DavisBaseLite v1.0\n");
	}
	
	
	public void parseUserCommand (String userCommand, DavisBaseBinaryFile baseBinaryFile) {
		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		DavisBaseBinaryFile binaryFile = new DavisBaseBinaryFile();
		try {
		String[] commandTokens = userCommand.toLowerCase().split(" ");
		
		CommandParser parser = new CommandParser();
		CatalogDetailsTables catalogDetailsTables = new CatalogDetailsTables();
		
		switch (commandTokens[0]) {
			case "show":
				//System.out.println("DEBUG: Call your method to remove items");
				String[] commandParts = userCommand.split("\\s+");
				/*
				 * SHOW TABLES
				 */
				try {
					if(commandParts[0].toUpperCase().equals("SHOW") && commandParts[1].toUpperCase().equals("TABLES"))
					{
						QueryExecutor executor3 = new QueryExecutor();
						executor3.showTables();
						
					}else{
						throw new Exception("Error SQL Syntax");
					}
					
				}
				catch(Exception e)
				{
					throw e;
				}
				break;
				
			case "create":
				System.out.println("DEBUG: Call your method to process queries");
				List<ArrayList<String>> tableDetailsList = parser.create(userCommand);
				catalogDetailsTables.InsertInCatalogTable(tableDetailsList.get(0));
				binaryFile.createNewTableFile((String) tableDetailsList.get(0).get(0));
				tableDetailsList.remove(0);
				for(ArrayList<String> columns : tableDetailsList)
				{
					catalogDetailsTables.InsertInCatalogCols(columns);
				}
				
				break;
			case "insert":
				System.out.println("DEBUG: Call your method to process queries");
				Map<String, ArrayList<Object>> recordMap = parser.insert(userCommand);
				QueryExecutor queryexec = new QueryExecutor();
				for(String key : recordMap.keySet())
				{
					queryexec.insertInTable(key,recordMap.get(key));
				}
				break;
			case "update":
				System.out.println("DEBUG: Call your method to process queries");
				ArrayList<String> updateList = parser.update(userCommand);
				ArrayList<String> whereList = new ArrayList<String>();
				whereList.add(updateList.get(2));
				whereList.add(updateList.get(3));
				whereList.add(updateList.get(4));
				QueryExecutor queryExec = new QueryExecutor();
				//Pass updateList
				queryExec.updateRecords(updateList.get(0).toString(), updateList.get(1).toString(), whereList, updateList.get(5));
				break;
			case "select":
				System.out.println("DEBUG: Call your method to process queries");
				ArrayList<ArrayList<String>> selectLst = new ArrayList<ArrayList<String>>();
				selectLst = parser.select(userCommand);
				QueryExecutor executor = new QueryExecutor();
				ArrayList<String> tableList = (ArrayList<String>) selectLst.get(0);
				String tableName = tableList.get(0);
				
				//pass the selectList to the function
				ArrayList<Map> selectRecs = executor.getSelectedRecords(tableName, (ArrayList<String>) selectLst.get(1), (ArrayList<String>) selectLst.get(2));
				display(selectRecs);
				break;
			case "drop":
				System.out.println("DEBUG: Call your method to remove items");
				ArrayList<String> tableName1 = parser.drop(userCommand);
				QueryExecutor executor2 = new QueryExecutor();
				executor2.dropTable(tableName1);
				break;
			case "help":
				help();
				break;
			case "version":
				version();
				break;
			case "exit":
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@SuppressWarnings("unchecked")
	private void display(ArrayList<Map> selectRecs) {
		Map<Integer,ArrayList<ArrayList<String>>> colHeader = selectRecs.get(0);
		for(ArrayList<ArrayList<String>> colList : colHeader.values())
		{
			String columHeader = (colList.get(1)).get(0);
			System.out.print(columHeader+"\t");
		}
		//iterate over columns
		selectRecs.remove(0);
		if(!selectRecs.isEmpty())
		{
			Map<Integer,ArrayList<ArrayList<Object>>> cols = selectRecs.get(0);
			for(ArrayList<ArrayList<Object>> recordList : cols.values())
			{System.out.println("");
				recordList.remove(0);
				for(ArrayList<Object> colsList :recordList)
				{
					
					System.out.print(colsList.get(0)+"\t");
				}
				System.out.println("");
			}
		}
		
	}

	
	
}