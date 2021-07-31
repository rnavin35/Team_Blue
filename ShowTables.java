import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ShowTables {

    public static void showTable(String userString) {

        // userString is in the form:
		//
		// SHOW TABLES;

        //insert table name into davisbase_tables
        
        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
            Map<String, ArrayList<String>> displayList = new HashMap<String, ArrayList<String>>();
            ArrayList<String> tableNames = new ArrayList<String>();

            //number of pages in tables table
            int maxPg = (int)(tableFile.length()/DavisBasePrompt.pageSize - 1);
            for (int i=0; i<=maxPg; i++)
            {
                //number of records on page
                tableFile.seek(DavisBasePrompt.pageSize*i + 2);
                short numRecords = tableFile.readShort();
                //pointers to all records on page
                short[] recordPointers = new short[numRecords];
                tableFile.seek(DavisBasePrompt.pageSize*i + 16);
                for (int j=0; j<numRecords; j++)
                {
                    recordPointers[j] = tableFile.readShort();
                }

                /*
                recordPointers[i] + 0  = payload
                recordPointers[i] + 2  = rowid
                ---------record header start---------
                recordPointers[i] + 6  = number of columns
                recordPointers[i] + 7  = table_name.size() + 0x0C
                ----------record header end----------
                recordPointers[i] + 8 = table_name start
                */

                //iterating through all records on page
                for (int j=0; j<numRecords; j++)
                {
                    //get table_name string length
                    tableFile.seek(DavisBasePrompt.pageSize*i + recordPointers[j] + 7);

                    int tableStringSize = (tableFile.readByte() & 0xFF) - 0x0C; //have to do it like this to get right number because of padding

                    //read table_name
                    tableFile.seek(DavisBasePrompt.pageSize*i + recordPointers[j] + 8);
                    byte[] readBytes = new byte[tableStringSize];
                    tableFile.read(readBytes, 0, tableStringSize);
                    String readTableName = new String(readBytes);

                    //save table_name
                    tableNames.add(readTableName);
                }

            }

            displayList.put("table_name", tableNames);
            DavisBasePrompt.displayResults(displayList);

            tableFile.close();
        }
        catch(Exception e) {
			System.out.println(e);
		}

    }

}
