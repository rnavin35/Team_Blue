import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

public class DropTable {

    public static void dropTable(String dropTableString) {
		// tableInfo is in the form:
		// DROP TABLE <table_name>;

        dropTableString = dropTableString.replace("\t", "");
        dropTableString = dropTableString.replaceAll("\\s{2,}", " ");
        ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(dropTableString.trim().split(" ")));
		String tableName = dropTableTokens.get(2);
        
		//do not let them delete metadata
		if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns"))
        {
            System.out.println("Cannot delete metadata file: " + tableName);
            return;
        }

        // check if table file already exists
		String tableFileName = "data/" + tableName + ".tbl";
		Path tablePath = Paths.get(tableFileName);

		// delete table file
		try
        {
            Files.deleteIfExists(tablePath);
        }
        catch(NoSuchFileException e)
        {
            System.out.println(tableName + "does not exist.");
            return;
        }
		catch(DirectoryNotEmptyException e)
        {
            System.out.println("Directory not empty.");
        }
        catch(IOException e)
        {
            System.out.println("IOException: " + e);
        }


		//remove table info from davisbase_tables
        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
            boolean found = false;
            int numPages = (int)(tableFile.length()/DavisBasePrompt.pageSize);
            short spot;
            byte [] tableNameBytes;

			//iterate through all pages
            for(int i=0; i<numPages; i++)
            {
                //number of records on page
                tableFile.seek(i*DavisBasePrompt.pageSize + 2);
                short numRecords = tableFile.readShort();
                //first record start
                int recordPointer = (int)(16 + i*DavisBasePrompt.pageSize);

                for(short j=0; j<numRecords; j++)
                {
                    tableFile.seek(recordPointer);
                    spot = tableFile.readShort();

					/*
						recordPointers[i] + 0  = payload
						recordPointers[i] + 2  = rowid
						---------record header start---------
						recordPointers[i] + 6  = number of columns
						recordPointers[i] + 7  = table_name.size() + 0x0C
						----------record header end----------
						recordPointers[i] + 8 = table_name start
					*/

					// read table_name
                    tableFile.seek(spot + 7 + i*DavisBasePrompt.pageSize);
                    int length = (tableFile.readByte() & 0xFF) - 0x0C; 
                    tableNameBytes = new byte[length];
                    tableFile.read(tableNameBytes, 0, length);
                    String readName = new String(tableNameBytes);

                    //if readName == tableName lazy delete by removing pointer
                    if(readName.equals(tableName))
                    {
                        found = true;
						//remove pointer
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
                    recordPointer = recordPointer + 2;
                }
                
            }

            tableFile.close();
        }
        catch(IOException e)
        {
			System.out.println("IOException: " + e);
        }
        










        //delete table information from davisbase_columns
        try
        {
            RandomAccessFile columnsTable = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
            int numPages = (int)(columnsTable.length()/DavisBasePrompt.pageSize);
            byte [] tableNameBytes;
            short spot;

			//iterate through all pages
            for(int i=0; i<numPages; i++)
            {
				//number of records on page
                columnsTable.seek(i*DavisBasePrompt.pageSize + 2);
                short numRecords = columnsTable.readShort();
				//first record start
                int recordPointer = (int)(16 + i*DavisBasePrompt.pageSize);

                for(short j=0; j<numRecords; j++)
                {
                    columnsTable.seek(recordPointer);
                    spot = columnsTable.readShort();

					/*
						recordPointers[i] + 0  = payload
						recordPointers[i] + 2  = rowid
						---------record header start---------
						recordPointers[i] + 6  = number of columns
						recordPointers[i] + 7  = table_name.size() + 0x0C
						recordPointers[i] + 8  = column_name.size() + 0x0C
						recordPointers[i] + 9  = data_type.size() + 0x0C  note: data_type is stored in string value
						recordPointers[i] + 10 = 0x01 (tinyInt ordinal_position)
						recordPointers[i] + 11 = is_nullable + 0x0C
						----------record header end----------
						recordPointers[i] + 12 = table_name start
					*/

                    // read table_name
                    columnsTable.seek(spot + 7 + i*DavisBasePrompt.pageSize);
                    int length = (columnsTable.readByte()  & 0xFF) - 0x0C;
                    tableNameBytes = new byte[length];
                    columnsTable.seek(spot + 12 + i*DavisBasePrompt.pageSize);
                    columnsTable.read(tableNameBytes, 0, length);
                    String readname = new String(tableNameBytes);

                    //if readName == tableName lazy delete by removing pointer
                    if(readname.equals(tableName))
                    {
                        //remove pointer
						int tempRecordPointer = recordPointer;
                        for (int k=j; k<numRecords; k++)
						{
							//save next record to current value
							columnsTable.seek(i*DavisBasePrompt.pageSize + tempRecordPointer + 2);
							short temp = columnsTable.readShort();

							columnsTable.seek(i*DavisBasePrompt.pageSize + tempRecordPointer);
                        	columnsTable.writeShort(temp);

							tempRecordPointer+=2;
						}

                        //zero out last pointer
						//columnsTable.seek(i*DavisBasePrompt.pageSize + tempRecordPointer);
                        //columnsTable.writeShort(0);

                        //decrement stored number of records on page
						numRecords--;
                        columnsTable.seek(i*DavisBasePrompt.pageSize + 2);
                        columnsTable.writeShort(numRecords);

						//zero out last pointer
						columnsTable.seek(i*DavisBasePrompt.pageSize + 16 + 2*(numRecords));
                        columnsTable.writeShort(0);
                        
                        //set last record pointer to be start of record area
						// if there are no more records, zero it out
						if(numRecords == 0)
						{
							columnsTable.seek(i*DavisBasePrompt.pageSize + 4);
							columnsTable.writeShort(0);
						}
						else
						{
							columnsTable.seek(i*DavisBasePrompt.pageSize + 16 + 2*(numRecords-1));
							short temp = columnsTable.readShort();
							columnsTable.seek(i*DavisBasePrompt.pageSize + 4);
							columnsTable.writeShort(temp);
						}
						j--;
                    }
					else
					{
						//only iterate if you haven't deleted a pointer
						recordPointer = recordPointer + 2;
					}

                }
               
            }

            columnsTable.close();
        }
        catch(IOException e)
        {
			System.out.println("IOException: " + e);
        }








	}
    
}
