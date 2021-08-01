import java.io.RandomAccessFile;
import java.nio.file.*;
import java.util.*;
import java.io.IOException;

public class CreateTable 
{
    public static void createTable(String tableInfo)
    {
        // tableInfo is in the form:
		//
		// CREATE TABLE <table_name> (
		// 		column1 dataType1,
		//		column2 dataType2
		// );

        tableInfo = tableInfo.replace("\t", "");
        tableInfo = tableInfo.replaceAll("\\s{2,}", " ");
        String tableName = tableInfo.substring(tableInfo.indexOf("table") + 6, tableInfo.indexOf('(')).trim();
        
        // check if table file already exists
		String tableFileName = "data/" + tableName + ".tbl";
		Path tablePath = Paths.get(tableFileName);
		if(Files.exists(tablePath))
		{
			System.out.println(tableName + "already exists.");
			return;
		}
        
        //holds all values related to columns
        String columns = tableInfo.substring(tableInfo.indexOf('(') + 1, tableInfo.lastIndexOf(')') ).trim();
        //splits columns based on "," or ", " (column name is still grouped with related info)
        ArrayList<String> createColumnTokens;
        createColumnTokens = new ArrayList<String>(Arrays.asList(columns.split(", |,")));

        //split column from related info
        Column [] savedData = new Column[createColumnTokens.size()+1];
        for(int i =0; i < createColumnTokens.size(); i++)
        {
            ArrayList<String> columnDetails = new ArrayList<String>(Arrays.asList(createColumnTokens.get(i).split(" ")));
            //columnDetails(0) = column name
            //columnDetails(1) = column data type
            //if columnDetails(2) exists, it is not null (or maybe primary key?)

            //make sure column data type is valid
            if(!Record.checkType(columnDetails.get(1)) )
            {
                System.out.println(columnDetails.get(0) + " has invalid data type: " + columnDetails.get(1));
                return;
            }

            //null flag = "no" if > 2
            if(columnDetails.size() > 2)
            {
                savedData[i] = new Column(tableName, columnDetails.get(0),columnDetails.get(1),"no",i+2);
            }
            else
            {
                savedData[i] = new Column(tableName, columnDetails.get(0),columnDetails.get(1),"yes",i+2);
            }
        }
        savedData[savedData.length -1] = new Column(tableName, "rowid", "int", "no", 1);





        //insert table name into davisbase_tables
        try
        {
            RandomAccessFile tableFile = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
            short payload = (short)(2 + tableName.length()); // 1 for num columns 1 for the column
            short recSize = (short)(payload + 6);
            
            int lastPage = (int)(tableFile.length()/DavisBasePrompt.pageSize - 1);
            
            tableFile.seek(lastPage*DavisBasePrompt.pageSize + 0x02);
            short numRecs = tableFile.readShort();
            short lastRecord = tableFile.readShort();
            //get last rowid and increemnt
            int rowid = DavisBasePrompt.getRowid(tableFile, lastPage) + 1;

            //check if there is room on page
            if(numRecs == 0 | lastRecord - (16 + (numRecs+1)*2) > recSize)
            {
                //increment numrecs and save value
                numRecs++;
                tableFile.seek(lastPage*DavisBasePrompt.pageSize + 0x02);
                tableFile.writeShort(numRecs);
                
                if(numRecs == 1)
                {
                    //new record area start
                    lastRecord = (short)(DavisBasePrompt.pageSize - recSize);
                }
                else
                {
                    //new record area start
                    lastRecord = (short)(lastRecord - recSize);
                }

                tableFile.writeShort(lastRecord);
                //last pointer spot
                tableFile.seek(lastPage*DavisBasePrompt.pageSize + 16 + (numRecs-1)*2);
                tableFile.writeShort(lastRecord);
                //write record here
                tableFile.seek(lastPage*DavisBasePrompt.pageSize + lastRecord);
                
                //start writing record
                tableFile.writeShort(payload);
                tableFile.writeInt(rowid);
                //header
                tableFile.writeByte(1);
                tableFile.writeByte(tableName.length()+0x0C);
                //body
                tableFile.writeBytes(tableName);

            }
            //add new page
            else
            {
                tableFile.setLength(tableFile.length() + DavisBasePrompt.pageSize);
                lastPage++;
                //initializes page
                setupPage(lastPage, tableFile);
                //num records
                tableFile.seek(lastPage*DavisBasePrompt.pageSize + 2);
                tableFile.writeShort(1);
                //new record pointer
                tableFile.writeShort((short)(DavisBasePrompt.pageSize - recSize));
                tableFile.seek(lastPage*DavisBasePrompt.pageSize + 16);
                tableFile.writeShort((short)(DavisBasePrompt.pageSize - recSize));
                //start writing record
                tableFile.seek(lastPage*DavisBasePrompt.pageSize + DavisBasePrompt.pageSize - recSize);
                tableFile.writeShort(payload);
                tableFile.writeInt(rowid);
                //header
                tableFile.writeByte(1);
                tableFile.writeByte(tableName.length()+0x0C);
                //body
                tableFile.writeBytes(tableName);

            }
            tableFile.close();
        }
        catch(Exception e) {
			System.out.println(e);
		}










        //TODO:insert table columns into davisbase_columns
        try {
			RandomAccessFile tableFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			
            for (int i=0; i<savedData.length; i++)
            {
                //last page of table
                int lastPage = (int)(tableFile.length()/DavisBasePrompt.pageSize - 1);
                //number of records on last page
                tableFile.seek(DavisBasePrompt.pageSize*lastPage + 2);
                int numRecs = tableFile.readShort();
                //page offset for the start of record data
                tableFile.seek(DavisBasePrompt.pageSize*lastPage + 4);
                int recordStart = tableFile.readShort();
                //getting last inserted record_id and incrementing
                int rowid = DavisBasePrompt.getRowid(tableFile, lastPage) + 1;
                //calculate payload
                int payload = (short)(6 + savedData[i].table.length()
                                              + savedData[i].name.length()
                                              + savedData[i].type.length()
                                              + 1
                                              + savedData[i].isnull.length());

                //get record size from Record object and check if there is enough room on page for record
                if(numRecs == 0 | recordStart - (16 + (numRecs+1)*2) > (payload + 6))
                {
                    //increment stored value of number of records on page and store new value
                    numRecs++;
                    tableFile.seek(lastPage*DavisBasePrompt.pageSize + 0x02);
                    tableFile.writeShort(numRecs);
                    
                    if(numRecs == 1)
                    {
                        //new record area start
                        recordStart = (short)(DavisBasePrompt.pageSize - (payload+6));
                    }
                    else
                    {
                        //new record area start
                        recordStart = (short)(recordStart - (payload+6));
                    }
                    //save value
                    tableFile.writeShort(recordStart);

                    //add the new record pointer to array of pointers
                    tableFile.seek(lastPage*DavisBasePrompt.pageSize + 16 + (numRecs-1)*2);
                    tableFile.writeShort(recordStart);
                }
                //not enough room for record on this page, make a new one
                else
                {
                    //increment number of pages
                    lastPage++;
                    //set new length
                    tableFile.setLength(tableFile.length() + DavisBasePrompt.pageSize);
                    //set record start
                    recordStart = (short)(DavisBasePrompt.pageSize - (payload + 6));

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
                }

                //writing record to database
                tableFile.seek(DavisBasePrompt.pageSize*lastPage + recordStart);
                tableFile.writeShort(payload);
                tableFile.writeInt(rowid);

                //RECORD HEADER START
                //numColumns: 5
                tableFile.writeByte(0x05);
                //table name: text
                tableFile.writeByte((byte)(0x0C + savedData[i].table.length()));
                //column name: text
                tableFile.writeByte((byte)(0x0C + savedData[i].name.length()));
                //data type: text
                tableFile.writeByte((byte)(0x0C + savedData[i].type.length()));
                //ordinal: tinyint
                tableFile.writeByte(0x01);
                //data type: text
                tableFile.writeByte((byte)(0x0C + savedData[i].isnull.length()));

                //RECORD BODY START
                //table name
                tableFile.writeBytes(savedData[i].table);
                //column name
                tableFile.writeBytes(savedData[i].name);
                //data type
                tableFile.writeBytes(savedData[i].type);
                //ordinal
                tableFile.writeByte(savedData[i].ordinal);
                //is nullable
                tableFile.writeBytes(savedData[i].isnull);

            }


			tableFile.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}















        //create table file
        try {
			RandomAccessFile tableFile = new RandomAccessFile(tableFileName, "rw");
			tableFile.setLength(DavisBasePrompt.pageSize);

			//1-byte b-tree flags:
			//	0x02 = index b-tree interior page
			//	0x05 = table b-tree interior page
			//	0x0A = index b-tree leaf page
			//	0x0D = table b-tree leaf page
			//TODO: check if properly initialized
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
			tableFile.writeShort(0);

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


    }



    public static void setupPage(int page, RandomAccessFile file) throws IOException
    {
        file.seek(page*DavisBasePrompt.pageSize);
        file.writeByte(0x0D);
        file.seek(page*DavisBasePrompt.pageSize + 2);
        file.writeShort(0);
        file.writeShort(0);
        file.writeInt(-1);
        file.writeInt(-1);
    }



    


}



class Column {

    String table;
    String name;
    String type;
    String isnull;
    byte ordinal;

    Column(String table,String name, String type, String isnull, int ordinal)
    {
        this.table = table;
        this.name = name;
        this.type = type;
        this.isnull = isnull;
        this.ordinal = (byte)ordinal ;
    }

}