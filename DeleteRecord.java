import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;

public class DeleteRecord {

    public static void deleteRecord(String deleteString) {
        //deleteString is in the form:
        // DELETE FROM TABLE <table_name> [WHERE condition];

        deleteString = deleteString.replace("\t", " ");
		deleteString = deleteString.replaceAll("\\s{2,}", " ");

        ArrayList<String> deleteTokens = new ArrayList<String>(Arrays.asList(deleteString.split(" ")));
        //check if query syntax is valid
        if(!deleteTokens.get(0).equals("delete")
            | !deleteTokens.get(1).equals("from")
            | !deleteTokens.get(2).equals("table")
            | !deleteTokens.get(4).equals("where")
            | !deleteTokens.get(5).equals("rowid"))
        {
            System.out.println("Delete record query must be in the format: DELETE FROM TABLE <table_name> [WHERE rowid comparison_operator int];");
            return;
        }

        //check if table_name is valid
        String tableName = deleteTokens.get(3);
        Path table = Paths.get("data/" + tableName + ".tbl");
        if(!Files.exists(table))
        {
            System.out.println("Table: " + tableName + " does not exist.");
            return;
        }

        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName + ".tbl", "rw");

            String id = deleteTokens.get(7).trim();
            int rowid = Integer.parseInt(id);
            int maxId = DavisBasePrompt.getRowid(tableFile, (int)(tableFile.length()/DavisBasePrompt.pageSize-1));
            
            //determine comparison operator used
            switch(deleteTokens.get(6).trim()){
                case "<=":
                    for(int i=1; i<=rowid; i++)
                        deleteRecordById(i, tableFile);
                    break;
                case "<":
                    for(int i=1; i<rowid; i++)
                        deleteRecordById(i, tableFile);
                    break;
                case ">=":
                    for(int i=rowid; i<=maxId; i++)
                        deleteRecordById(i, tableFile);
                    break;
                case ">":
                    for(int i=rowid+1; i<=maxId; i++)
                        deleteRecordById(i, tableFile);
                    break;
                default:
                    deleteRecordById(rowid, tableFile);
            }

            tableFile.close();
        }
        catch(IOException e)
        {
            System.out.println("IOException: " + e);
        }

        
    }



    public static void deleteRecordById(int rowid, RandomAccessFile tableFile)
    {
        try
        {
            boolean found = false;
            int numPages = (int)(tableFile.length()/DavisBasePrompt.pageSize);
            short spot;
            short numRecords;
            int recId;
            
            //iterate through all pages
            for(int i=0; i<numPages; i++)
            {
                //number of records on page
                tableFile.seek(i*DavisBasePrompt.pageSize + 2);
                numRecords = tableFile.readShort();
                //first record start
                int recordPointer = (int)(16 + i*DavisBasePrompt.pageSize);

                for(short j=0; j<numRecords; j++)
                {
                    tableFile.seek(recordPointer);
                    spot = tableFile.readShort();
                    tableFile.seek(i*DavisBasePrompt.pageSize + spot + 2);
                    recId = tableFile.readInt();

                    //record to be deleted
                    if(rowid == recId)
                    {
                        found = true;

                        //delete pointer and shift remaining to the left
                        for (int k=j; k<numRecords-1; k++)
						{
							//save next record to current value
							tableFile.seek(i*DavisBasePrompt.pageSize + recordPointer + 2);
							short temp = tableFile.readShort();

							tableFile.seek(i*DavisBasePrompt.pageSize + recordPointer);
                        	tableFile.writeShort(temp);

							recordPointer+=2;
						}
                        
                        //zero out last pointer
						tableFile.seek(i*DavisBasePrompt.pageSize + recordPointer);
                        tableFile.writeShort(0);

                        //decrement stored number of records on page
						numRecords--;
                        tableFile.seek(i*DavisBasePrompt.pageSize + 2);
                        tableFile.writeShort(numRecords);
                        
                        //set last record pointer to be start of record area
						// if there are no more records, zero it out
						if(numRecords == 0)
						{
							tableFile.seek(i*DavisBasePrompt.pageSize + 4);
							tableFile.writeShort(0);
						}
						else
						{
							tableFile.seek(i*DavisBasePrompt.pageSize + 16 + 2*(numRecords-1));
							short temp = tableFile.readShort();
							tableFile.seek(i*DavisBasePrompt.pageSize + 4);
							tableFile.writeShort(temp);
						}

                        break;
                    }
                    if(found == true){break;}
                    recordPointer += 2;
                }
            }
        }
        catch(IOException e)
        {
            System.out.println("IOException: " + e);
        }
    }
}
