import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandParser {

	public List< ArrayList<String>> create(String userCommand) throws Exception {
		String[] CommandParts = userCommand.split("\\s+");
		String tableName = null;
		 List<ArrayList<String>> columnsAList = new ArrayList<ArrayList<String>>();
		if(userCommand.contains("(") && userCommand.contains(")") && (userCommand.indexOf("(") < userCommand.lastIndexOf(")"))){
            if(CommandParts[2].contains("(")){
            	tableName = CommandParts[2].substring(0, CommandParts[2].indexOf("("));
            }
            else{
            	tableName =CommandParts[2].toUpperCase();
            }
            ArrayList<String> tableList = new ArrayList<String>();
            tableList.add(tableName.toUpperCase());
            tableList.add("TEXT");
            columnsAList.add(tableList);
            String tableAttributes = userCommand.substring(userCommand.indexOf("(")+1,userCommand.lastIndexOf(")"));
            String[] columnLine = tableAttributes.split(",");
           
            for(int i = 0 ; i < columnLine.length ; i++)
            {
            	ArrayList<String> columns = new ArrayList<String>();
            	columnLine[i]=columnLine[i].trim();
            	String[] columnsList = columnLine[i].split("\\s+");
            	if(columnsList.length < 2 || columnsList.length >4)
            	{
            		throw new Exception("Error in SQL Syntax");
            	}else
            	{
            		columns.add(tableName.toUpperCase());
            		columns.add(columnsList[0].trim().toUpperCase()); // column name
            		columns.add(columnsList[1].trim().toUpperCase()); // datatype
            		columns.add(Integer.toString(i+1)); // ordinal_position
            			/*if(columnsList[2].trim().toUpperCase().equals("PRIMARY") && columnsList[3].trim().toUpperCase().equals("KEY"))
            			{
            				columns.add("PRI"); // primary key
            			}*/
            		
            			if(columnsList.length > 2 && columnsList[2].trim().toUpperCase().equals("NOT") && columnsList[3].trim().toUpperCase().equals("NULL"))
            			{
            				columns.add("YES"); // null constraint
            			}
            			else
            			{
            				columns.add("NO");
            			}
            		}
            		columnsAList.add(columns);
            	}
		}
		return columnsAList;
	}
	public Map<String, ArrayList<Object>> insert(String userCommand) throws Exception {
		// TODO Auto-generated method stub
		String[] CommandParts = userCommand.split("\\s+");
		//Table t = new Table();
		
		Map<String,ArrayList<Object>> recordMap= new HashMap<String,ArrayList<Object>>();
		ArrayList<Object> recordDets = new ArrayList<Object>();
		String tableName = null;
		String[] AttributeList;
		try {
			
				if (CommandParts[0].toUpperCase().equals("INSERT") && CommandParts[1].toUpperCase().equals("INTO")
						&& CommandParts[3].toUpperCase().contains("VALUES")) {
					if (userCommand.contains("(") && userCommand.contains(")")
							&& (userCommand.indexOf("(") < userCommand.indexOf(")"))) {
						tableName= CommandParts[2].toUpperCase();
						String TableAttributes = userCommand.substring(userCommand.indexOf("(") + 1,
								userCommand.indexOf(")"));
						AttributeList = TableAttributes.split(",");
						for (int i = 0; i < AttributeList.length; i++) {
							AttributeList[i] = AttributeList[i].replaceAll("\'","");
							recordDets.add(AttributeList[i].trim());
						}
						recordMap.put(tableName, recordDets);
					} else {
						throw new Exception("Error in SQL Syntax");
					}
				} else {
					throw new Exception("Error in SQL Syntax");
				}
			
		} catch (Exception e) {
			throw e;
		}

		return recordMap;
		}

	public ArrayList<String> update(String userCommand) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
				String[] CommandParts = userCommand.split("\\s+");
				ArrayList<String> LstTableColumns = new ArrayList<String>();
				ArrayList<String> whereAtributeList = new ArrayList<String>();
				String opearator="";
				String tableName = null;
				/*
				 * UPDATE table_name
		SET column1=value1
		WHERE some_column=some_value;
				 */
				try {
					//Correct Format
					if (CommandParts[0].toUpperCase().equals("UPDATE") && CommandParts[2].toUpperCase().equals("SET") 
							&& CommandParts[6].toUpperCase().equals("WHERE")) {
						//Table Name
						 tableName = CommandParts[1].toUpperCase();
						String columnUpdate = CommandParts[3].trim();
						String updateValue = CommandParts[5].trim();
						//Capture Operator
						
							String ConditionClause = CommandParts[8].trim();
									
							//String operator = "";
							
						 if (ConditionClause.contains("<>")) {
								opearator = "!=";
							
						}else
						{
							opearator = ConditionClause;
						}
						//Fill array
						whereAtributeList.add(CommandParts[7]);
						whereAtributeList.add(opearator);
						whereAtributeList.add(CommandParts[9]);
					
					
					LstTableColumns.add(tableName);
					LstTableColumns.add(columnUpdate.toUpperCase());
					LstTableColumns.add(CommandParts[7].toUpperCase().trim());
					LstTableColumns.add(opearator);
					LstTableColumns.add(CommandParts[9].toUpperCase().trim());
					LstTableColumns.add(updateValue.toUpperCase());
					}
					
				}catch(Exception e)
				{
					try {
						throw new Exception("Error in SQL Syntax");
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				
				return LstTableColumns;
	}

	public ArrayList<ArrayList<String>> select(String userCommand) throws Exception {
	
		// TODO Auto-generated method stub
				String[] CommandParts = userCommand.split("\\s+");
				// List<Attribute> LstTableColumns = new ArrayList<>();
				ArrayList<ArrayList<String>> LstTableColumns = new ArrayList<ArrayList<String>> ();
				String tableName = "";
				ArrayList<String> selectWhereAttributeList= new ArrayList<String>();
				String[] wildCardList = new String[0];
				try {

					
						//No Wild Cards
						if (CommandParts[0].toUpperCase().equals("SELECT") && CommandParts[1].toUpperCase().equals("*")
								&& CommandParts[2].toUpperCase().equals("FROM")) {
							//Table name
							tableName = CommandParts[3];
							
						}
						//If Wildcards present
						else if( !CommandParts[1].toUpperCase().equals("*")){
							String wildCards = userCommand.substring(userCommand.toUpperCase().indexOf("SELECT") + 6,
									userCommand.toUpperCase().indexOf("FROM"));
							wildCardList = new String[wildCards.split(",").length];
							wildCardList = wildCards.toUpperCase().trim().split(",");
							//Set Table name
							tableName = CommandParts[3].toUpperCase();
						}else
						{
							throw new Exception("Error in SQL Syntax");
						}
						//Parse Where clause
						if (userCommand.toUpperCase().contains("WHERE")) {
							String ConditionClause = userCommand.substring(userCommand.toUpperCase().indexOf("WHERE") + 5,
									userCommand.length());
							String operator = "";
							if (ConditionClause.contains(">=")) {
								operator = ">=";
							} else if (ConditionClause.contains("<=")) {
								operator = "<=";
							} else if (ConditionClause.contains("<>")) {
								operator = "<>";
							} else if (ConditionClause.contains("=")) {
								operator = "=";
							} else if (ConditionClause.contains(">")) {
								operator = ">";
							} else if (ConditionClause.contains("<")) {
								operator = "<";
							}

							else {
								throw new Exception("Error in SQL Syntax");
							}

							if (ConditionClause.split(operator).length == 2) {
								//Where Column name
								selectWhereAttributeList.add(ConditionClause.split(operator)[0].trim().toUpperCase());
								//Operator & Where clause value
								selectWhereAttributeList.add(operator);
								selectWhereAttributeList.add(ConditionClause.split(operator)[1].trim().replaceAll("\'", "").toUpperCase());
								
							} else {
								throw new Exception("Error in SQL Syntax");
							}
							
						}
						
						//Add to array list
						ArrayList<String> tabNameList = new ArrayList<String>();
						ArrayList<String> wildCardAList = new ArrayList<String>();
						for(int i =0 ; i < wildCardList.length;i++)
						{
							wildCardAList.add(wildCardList[i]);
						}
						
						tabNameList.add(tableName);
						LstTableColumns.add(tabNameList);
						LstTableColumns.add(wildCardAList);
						LstTableColumns.add(selectWhereAttributeList);
						//****************************************************
						//TODO: Call method to select based
						// tableName, wildCardList, selectWhereAttributeList
						//*****************************************************
					}
					catch (Exception e) {
					throw e;
				}
				return LstTableColumns;
		// TODO Auto-generated method stub
		
	}

	public ArrayList<String> drop(String userCommand) throws Exception {
		// TODO Auto-generated method stub
		String[] CommandParts = userCommand.split("\\s+");
		// List<Attribute> LstTableColumns = new ArrayList<>();
		ArrayList LstTableColumns = new ArrayList();
		ArrayList<String> tableNameLst = new ArrayList<String>();
		ArrayList<String> selectWhereAttributeList= new ArrayList<String>();
		String[] wildCardList = new String[0];
		/*
		 * DROP TABLE table_name;
		 */
		try {
			if(CommandParts[0].toUpperCase().equals("DROP") && CommandParts[1].toUpperCase().equals("TABLE"))
			{
				tableNameLst.add(CommandParts[2].toUpperCase().trim());
				tableNameLst.add("TEXT");
				
			}else{
				throw new Exception("Error SQL Syntax");
			}
			
		}
		catch(Exception e)
		{
			throw e;
		}
		return tableNameLst;
	}

//	public ArrayList<String> showTables(String userCommand) throws Exception {
//		String[] CommandParts = userCommand.split("\\s+");
//		
//		/*
//		 * SHOW TABLES
//		 */
//		try {
//			if(CommandParts[0].toUpperCase().equals("SHOW") && CommandParts[1].toUpperCase().equals("TABLES"))
//			{
//				
//				
//			}else{
//				throw new Exception("Error SQL Syntax");
//			}
//			
//		}
//		catch(Exception e)
//		{
//			throw e;
//		}
//		return tableNameLst;
//		return null;
//	}

}
