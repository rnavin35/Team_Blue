import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class InsertRecord {

    public static void insertRecord(String insertString) {

		// insertString is in the form:
		//
		// INSERT INTO <table_name> (column1, column2, ...)
		// VALUES (value1, value2, ...);

		insertString = insertString.replace("(", " ").replace(")", " ").replace(",", " ").replace("\t", "");
		insertString = insertString.replaceAll("\\s{2,}", " ");
		
		ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(insertString.trim().split(" ")));
		String tableName = insertTokens.get(2);
		int valuesIndex = insertTokens.indexOf("values");
		
		//place column names into array
		String[] column = new String[insertTokens.size() - valuesIndex -1];
		int temp = 0;
		for (int i=3; i<valuesIndex; i++)
		{
			column[temp] = new String(insertTokens.get(i));
			temp++;
		}
		
		//place column values into array
		temp = 0;
		String[] body = new String[insertTokens.size() - valuesIndex -1];
		for (int i=valuesIndex+1; i<insertTokens.size(); i++)
		{
			body[temp] = new String(insertTokens.get(i));
			temp++;
		}

		
		try {
			RandomAccessFile tableFile = new RandomAccessFile("data/" + tableName + ".tbl", "rw");
			//last page of table
			int lastPage = (int)(tableFile.length()/DavisBasePrompt.pageSize - 1);
			//number of records on last page
			tableFile.seek(DavisBasePrompt.pageSize*lastPage + 2);
			int numRecs = tableFile.readShort();
			//page offset for the start of record data
			int recordStart = tableFile.readShort();
			//getting last inserted record_id and incrementing
			int rowid = DavisBasePrompt.getRowid(tableFile, lastPage);

			//create Record object to insert
			Record newRecord = new Record(tableName, rowid, body, column);
			newRecord.setHeaderAndPayload();

			//get record size from Record object and check if there is enough room on page for record
			if(numRecs == 0 | recordStart - (16 + (numRecs+1)*2) > (newRecord.payload + 6))
			{
				//increment stored value of number of records on page and store new value
				numRecs++;
				tableFile.seek(lastPage*DavisBasePrompt.pageSize + 0x02);
				tableFile.writeShort(numRecs);
				
				if(numRecs == 1)
				{
					//new record area start
                    recordStart = (short)(DavisBasePrompt.pageSize - (newRecord.payload+6));
				}
				else
				{
                    //new record area start
                    recordStart = (short)(recordStart - (newRecord.payload+6));
				}
				//save value
				tableFile.writeShort(recordStart);

				//add the new record pointer to array of pointers
				tableFile.seek(lastPage*DavisBasePrompt.pageSize + 16 + (numRecs-1)*2);
				tableFile.writeShort(recordStart);
				//write record here
				tableFile.seek(lastPage*DavisBasePrompt.pageSize + recordStart);

				//save values of Record pointer and write to database
				newRecord.page = lastPage;
				newRecord.location = recordStart;
				newRecord.writeRecord(tableFile);

			}
			//not enough room for record on this page, make a new one
			else
			{
				//increment number of pages
				lastPage++;
				//set new length
				tableFile.setLength(tableFile.length() + DavisBasePrompt.pageSize);
				//set record start
				recordStart = (short)(DavisBasePrompt.pageSize - (newRecord.payload + 6));

				//NEW PAGE INITIALIZATION

				//b-tree flag
				//TODO: check if properly initialized
				tableFile.seek(DavisBasePrompt.pageSize*lastPage + 0);
				tableFile.writeByte(0x0D);

				//num of records on page
				tableFile.seek(DavisBasePrompt.pageSize*lastPage + 2);
				tableFile.writeShort(0x01);

				//where record data begins
				tableFile.seek(DavisBasePrompt.pageSize*lastPage + 4);
				tableFile.writeShort(recordStart);

				//b-tree page pointer
				//TODO: check if properly initialized
				tableFile.seek(DavisBasePrompt.pageSize*lastPage + 6);
				tableFile.writeInt(0);

				//page pointer references the page’s parent
				//TODO: check if properly initialized
				tableFile.seek(DavisBasePrompt.pageSize*lastPage + 10);
				tableFile.writeInt(-1);

				//new record pointer
				tableFile.seek(DavisBasePrompt.pageSize*lastPage + 16);
				tableFile.writeShort(recordStart);

				//save values of Record pointer and write to database
				newRecord.page = lastPage;
				newRecord.location = recordStart;
				newRecord.writeRecord(tableFile);

			}


			tableFile.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}


	}

}
