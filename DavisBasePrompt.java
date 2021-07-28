import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.Arrays;
import static java.lang.System.out;

// @author Chris Irwin Davis
// @version 1.0

public class DavisBasePrompt {

	static String prompt = "davisql> ";
	static String version = "v1.0";
	static String copyright = "©2016 Chris Irwin Davis";
	static boolean isExit = false;

	// Page size for alll files is 512 bytes by default.
	static long pageSize = 512; 
 
	/*  The Scanner class is used to collect user commands from the prompt
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");


    public static void main(String[] args) {

		// Display the welcome screen
		splashScreen();

		//check to see if metadata exists, if not create it
		Path data = Paths.get("data");
		Path tables = Paths.get("data/davisbase_columns.tbl");
		Path columns = Paths.get("data/davisbase_columns.tbl");
		if(!Files.isDirectory(data) | !Files.exists(tables) | !Files.exists(columns))
		{
			initializeDataStore();
			System.out.println("Database metadata initialized");
		}

		// Variable to collect user input from the prompt
		String userCommand = ""; 

		while(!isExit) {
			System.out.print(prompt);
			// toLowerCase() renders command case insensitive
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}


	// Static method definitions
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite");
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
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
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
		//Help: Display supported commands
		public static void help() {
			out.println(line("*",80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			//printCmd("SELECT * FROM <table_name>;");
			//printDef("Display all records in the table <table_name>.");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("\tis <column_name> = <value>.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println("HEXDUMP <table_name>;");
			out.println("\tDisplays Hex dump of file of given table name.\n");
			out.println(line("*",80));
		}


	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement
		 */
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		// This switch handles a very small list of hardcoded commands of known syntax.
		switch (commandTokens.get(0)) {
			case "select":
				System.out.println("CASE: SELECT");
				parseQuery(userCommand);
				break;
			case "drop":
				System.out.println("CASE: DROP");
				dropTable(userCommand);
				break;
			case "create":
				System.out.println("CASE: CREATE");
				parseCreateTable(userCommand);
				break;
			case "update":
				System.out.println("CASE: UPDATE");
				parseUpdate(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			case "hexdump":
				hexDump(userCommand);
				break;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
	


	/*
	 *  Stub method for dropping tables
	 */
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: This is the dropTable method.");
		System.out.println("\tParsing the string:\"" + dropTableString + "\"");
	}
	


	/*
	 *  Stub method for executing queries
	 */
	public static void parseQuery(String queryString) {
		System.out.println("STUB: This is the parseQuery method");
		System.out.println("\tParsing the string:\"" + queryString + "\"");
	}


	/*
	 *  Stub method for updating records
	 */
	public static void parseUpdate(String updateString) {
		System.out.println("STUB: This is the dropTable method");
		System.out.println("Parsing the string:\"" + updateString + "\"");
	}

	
	/*
	 *  Stub method for creating new tables
	 */
	public static void parseCreateTable(String createTableString) {
		
		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		// new table in data directory
		String tableFileName = "data/" + createTableTokens.get(2) + ".tbl";
		Path tablePath = Paths.get(tableFileName);
		if(Files.exists(tablePath))
		{
			System.out.println(createTableTokens.get(2) + "already exists.");
			return;
		}

		try {
			RandomAccessFile tableFile = new RandomAccessFile(tableFileName, "rw");
			tableFile.setLength(pageSize);

			//1-byte b-tree flags:
			//	0x02 = index b-tree interior page
			//	0x05 = table b-tree interior page
			//	0x0A = index b-tree leaf page
			//	0x0D = table b-tree leaf page
			//TODO: properly initialize
			tableFile.seek(0);
			tableFile.writeByte(0x0D);

			//2-byte: num of records on page
			//	initialized to 0
			tableFile.seek(2);
			tableFile.writeShort(0);

			//2-byte: where record data begins (records are buttom -> top so it is 
			//	actually the end of the records)
			//	initialized to last spot on page
			tableFile.seek(4);
			tableFile.writeShort(0x01FF);

			//4-byte: page pointer
			//	Table or Index interior page - page number of rightmost child
			//	Table or Index leaf page - page number of sibling to the right
			//	initialized to 0
			tableFile.seek(6);
			tableFile.writeInt(0);

			//4-byte: page pointer references the page’s parent
			//	If this is a root page, then the special value 0xFFFFFFFF is used
			//	initialized to -1 = 0xFFFFFFFF
			tableFile.seek(10);
			tableFile.writeInt(-1);

			//2*(num records on page): array of 2-byte ints that indicate the 
			// location of each record on page maintained in key-sorted order
			//tableFile.seek(16);

			tableFile.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
		// Insert a row in the davisbase_tables
		// TODO: call insertRecord with following query
		// INSERT INTO davisbase_tables (rowid, table_name)
		// VALUES (TBD, createTableTokens.get(2));


		// Insert rows in the davisbase_columns for each column in the new table
		// TODO: call insertRecord with following query
		// INSERT INTO davisbase_columns (rowid, table_name, column_name, data_type, ordinal_position, is_nullable)
		// VALUES (TBD, createTableTokens.get(2), iterate_through_table_tokens, iterate_through_table_tokens, ???, iterate_through_table_tokens)

		//TODO: create index file
		// <table_name>.<column_name>.ndx

	}


	/*
	 *  Stub method for inserting record into table
	 */
	public static void insertRecord(String insertString) {

		// insertString is in the form:
		// INSERT INTO <table_name> (column1, column2, ...)
		// VALUES (value1, value2, ...);
		insertString.toLowerCase();
		insertString = insertString.replace("(", " ").replace(")", " ").replace(",", " ").replace(";", "");
		ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(insertString.trim().split(" ")));
		String tableName = insertTokens.get(2);
		int valuesIndex = insertTokens.indexOf("values");

		//TODO: retrieve data type of columns from davisbase_columns table
		String[] header = new String[insertTokens.size() - valuesIndex]; // number of columns
		
		//place column values into array
		String[] body = new String[insertTokens.size() - valuesIndex];
		for (int i=valuesIndex+1; i<insertTokens.size(); i++)
		{
			int temp = 0;
			body[temp] = insertTokens.get(i);
			temp++;
		}

		//TODO: create Record object

		try {
			RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName + ".tbl", "rw");
			//last page of table
			tableFile.seek(6);
			int lastPage = tableFile.readInt();
			//number of records on last page
			tableFile.seek(pageSize*lastPage + 2);
			int numRecords = tableFile.readInt();
			//page offset for the start of record data
			tableFile.seek(4);
			int recordStart = tableFile.readInt();
			//pointer to added record goes here
			tableFile.seek(pageSize*lastPage + 16 + (numRecords+1)*2);

			//TODO: get record size from Record object and check if there is enough room on page for record
			//		recordStart - (16 + (numRecords+1)*2) > (payload + 6)

			//TODO: increment stored value of number of records on page
			//		(value of)(tableFile.seek(pageSize*lastPage + 2))++
			//	add the pointer to the start of the new record to array of record pointers
			//		(valueof)(tableFile.seek(4))-(payload + 6) to tableFile.seek(pageSize*lastPage + 16 + (numRecords)*2);
			//	also set this value to the page offset for the start of record data
			//		tableFile.seek(4) = (valueof)(tableFile.seek(4))-(payload + 6)
			//	insert values of Record object to this index
			


			tableFile.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}


	}





















	/* This static method creates the DavisBase data storage container
	 * and then initializes two .tbl files to implement the two 
	 * system tables, davisbase_tables and davisbase_columns
	 *
	 * WARNING! Calling this method will destroy the system database
	 * catalog files if they already exist.
	 */
	static void initializeDataStore() {
		// Create data directory at the current OS location to hold
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
			out.println("Unable to create data container directory");
			out.println(se);
		}


		// Create davisbase_tables system catalog
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			// Initially, the file is one page in length
			davisbaseTablesCatalog.setLength(pageSize);
			// Set file pointer to the beginnning of the file
			davisbaseTablesCatalog.seek(0);
			// Write 0x0D to the page header to indicate that it's a leaf page.  
			davisbaseTablesCatalog.write(0x0D);
			// Write 0x00 to indicate there 
			// are no cells on this page initially
			davisbaseTablesCatalog.write(0x00);
			davisbaseTablesCatalog.close();
		}
		catch (Exception e) {
			out.println("Unable to create the database_tables file");
			out.println(e);
		}

		// Create davisbase_columns systems catalog
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			// Initially the file is one page in length
			davisbaseColumnsCatalog.setLength(pageSize);
			// Set file pointer to the beginnning of the file
			davisbaseColumnsCatalog.seek(0);
			// Write 0x0D to the page header to indicate a leaf page. The file 
			davisbaseColumnsCatalog.write(0x0D);
			// Write 0x00 to indicate there 
			// are no cells on this page initially
			davisbaseColumnsCatalog.write(0x00); 
			davisbaseColumnsCatalog.close();
		}

		catch (Exception e) {
			out.println("Unable to create the database_columns file");
			out.println(e);
		}
	}





	/* This static method will format a given table name
	 * and send to HexDump.java to display a hexdump of
	 * the file of the given table
	 */
	static void hexDump(String queryString)
	{
		ArrayList<String> queryTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
		String fileName = "data/" + queryTokens.get(1) + ".tbl";
		String[] args2 = new String[1];
		args2[0] = fileName;
		HexDump.main(args2);
	}
}