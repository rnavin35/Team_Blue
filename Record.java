import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class Record {

    short payload;          // payload size
    int rowid;
    short[] header;         // data type of columns
    String[] body;          // column values
    String tableFileName;   // table record is in
    int page;               // page record is on
    int location;           // location of record on page

    public Record(String fn, int page, int location, int rowid) {

        this.tableFileName = fn;
        this.page = page;
        this.location = location;
        this.rowid = rowid;

        //TODO: complete initialization

    }

    //TODO: write data to database
    //      get column datatypes from davisbase_columns.tbl using columnInfo(tableName)
    //      calculate payload
    //      write to database


    // get all column names and datatypes from davisbase_columns associated with a given table
    // used for inserting a new record (to make header)
    public static Map<String, String> columnInfo(String tableName) throws Exception{

        // <column name, column data type>
        Map<String, String> nameAndType = new HashMap<String, String>();
        RandomAccessFile columnsFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");

        //number of pages in columns table
        int maxPg = (int)(columnsFile.length()/DavisBasePrompt.pageSize);
        for (int i=0; i<maxPg; i++)
        {
            //number of records on page
            columnsFile.seek(DavisBasePrompt.pageSize*i + 2);
            short numRecords = columnsFile.readShort();
            //pointers to all records on page
            short[] recordPointers = new short[numRecords];
            columnsFile.seek(DavisBasePrompt.pageSize*i + 16);
            for (int j=0; j<numRecords; j++)
            {
                recordPointers[i] = columnsFile.readShort();
            }

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

            //iterating through all records on page
            for (int j=0; j<numRecords; j++)
            {
                //get string lengths
                columnsFile.seek(recordPointers[j] + 7);
                int tableStringSize = (columnsFile.readByte() & 0xFF) - 0x0C; //have to do it like this to get right number because of padding
                int columnStringSize = (columnsFile.readByte() & 0xFF) - 0x0C;
                int dataStringSize = (columnsFile.readByte() & 0xFF) - 0x0C;

                columnsFile.seek(DavisBasePrompt.pageSize*i + 12);
                byte[] readBytes = new byte[tableStringSize];
                columnsFile.read(readBytes, 0, tableStringSize);
                String readTableName = new String(readBytes);

                if (readTableName.equalsIgnoreCase(tableName))
                {
                    //get column name
                    readBytes = new byte[columnStringSize];
                    columnsFile.read(readBytes, 0, columnStringSize);
                    String readColumnName = new String(readBytes);

                    //get data type name
                    readBytes = new byte[dataStringSize];
                    columnsFile.read(readBytes, 0, dataStringSize);
                    String readDataString = new String(readBytes);
                    nameAndType.put(readColumnName, readDataString);
                }
            }



        }
        

		
        columnsFile.close();
        return nameAndType;
    }



    public byte typeToInt(String dataType, String value) {
        dataType.toLowerCase();
        switch (dataType) {
			case "null":
				return 0x00;
            case "tinyint":
				return 0x01;
            case "smallint":
				return 0x02;
            case "int":
                return 0x03;
            case "long":
                return 0x04;
            case "float":
                return 0x05;
            case "double":
                return 0x06;
            case "year":
                return 0x08;
            case "time":
                return 0x09;
            case "datetime":
                return 0x0A;
            case "date":
                return 0x0B;
            default: //string: 0x0C + n
				return (byte)(value.length() + 0x0C);
        }
    }



    public String IntToType(byte dataType, String value) {
        switch (dataType) {
			case 0x00:
				return "null";
            case 0x01:
				return "tinyint";
            case 0x02:
				return "smallint";
            case 0x03:
                return "int";
            case 0x04:
                return "long";
            case 0x05:
                return "float";
            case 0x06:
                return "double";
            case 0x08:
                return "year";
            case 0x09:
                return "time";
            case 0x0A:
                return "datetime";
            case 0x0B:
                return "date";
            default: //string:  0x0C + n
				return "text";
        }
    }
}