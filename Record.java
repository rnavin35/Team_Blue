import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class Record {

    short payload;      // payload size = header.length + length of record body (tbd based on data type)
    int rowid;          // set in DavisBasePrompt
    byte[] header;      // data type of columns
    String[] body;      // values of columns excluding rowid
    String[] column;    // names of columns excluding rowid (used for insert record)

    String tableName;   // table record is in
    int page;           // page record is on
    int location;       // location of record on page

    public Record(String table, int rowid, String[] body, String[] column) {
        this.tableName = table;
        //this.page = page;         // can't be determined before payload
        //this.location = location; // can't be determined before payload
        this.rowid = rowid;
        this.body = body;
        this.column = column;
    }


    // writing record to database:
    //      DavisBasePrompt calls setHeaderAndPayload() to create header based on data type and set payload
    //          setHeaderAndPayload() calls columnInfo() to check davisbase_columns to make sure column names are valid and get data type info
    //      DavisBasePrompt makes sure there is enough room on page for record and sets values for page and location
    //      DavisBasePrompt calls writeRecord()


    // BEFORE CALLING THIS FUNCTION: page and location must be set
    public int writeRecord(RandomAccessFile recordFile) throws Exception {

        //start of record on page
        recordFile.seek(DavisBasePrompt.pageSize*page + location);
        recordFile.writeInt(payload);
        recordFile.writeInt(rowid);
        for (int i=0; i<header.length; i++)
        {
            recordFile.writeByte(header[i]);
        }
        //start of writing record body
        for (int i=0; i<body.length; i++)
        {
            switch (header[i+1]) {
                case 0x00:
                    break;
                case 0x01:
                    recordFile.writeByte(body[i].charAt(0));
                    break;
                case 0x02:
                    recordFile.writeShort(Short.parseShort(body[i]));
                    break;
                case 0x03:
                    recordFile.writeInt(Integer.parseInt(body[i]));
                    break;
                case 0x04:
                    recordFile.writeLong(Long.parseLong(body[i]));
                    break;
                case 0x05:
                    recordFile.writeFloat(Float.parseFloat(body[i]));
                    break;
                case 0x06:
                    recordFile.writeDouble(Double.parseDouble(body[i]));
                    break;
                case 0x08:
                    recordFile.writeInt(Integer.parseInt(body[i]));
                    break;
                case 0x09:
                    recordFile.writeInt(Integer.parseInt(body[i]));
                    break;
                case 0x0A:
                    recordFile.writeLong(Long.parseLong(body[i]));
                    break;
                case 0x0B:
                    // TODO: find better way to write date and validate its format is correct
                    recordFile.writeBytes(body[i].substring(0, 8));
                    break;
                default:
                    recordFile.writeBytes(body[i]);
            }
        }

        return 0;
    }


    // use datatype of columns and length of strings to set the header and payload
    // BEFORE CALLING THIS FUNCTION: column and body must be set
    public int setHeaderAndPayload() throws Exception {
        Map<String, String> nameAndType = columnInfo(tableName);
        header = new byte[nameAndType.size()];

        //first spot is number of columns (excluding rowid)
        header[0] = (byte)(nameAndType.size() - 1);
        
        //payload size = header.length + length of record body (tbd based on data type)
        payload = (byte)header.length;

        //getting correct byte per data type
        for (int i=1; i<header.length; i++)
        {
            // check if column name is valid for table
            if (!nameAndType.containsKey(column[i-1]))
            {
                System.out.println("Column: " + column[i-1] + " does not exist in table: " + tableName);
                return -1;
            }

            // column[i-1]              = name of column
            // body[i-1]                = value of column
            // nameAndType.get(column)  = returns string of data type
            // typeToInt(column, value) = converts string of data type to equivalent byte
            header[i] = typeToByte(nameAndType.get(column[i-1]), body[i-1]);

            //increase payload count depending on data type 
            switch (header[i]) {
                case 0x00:
                    break;
                case 0x01:
                    payload += 0x01;
                    break;
                case 0x02:
                    payload += 0x02;
                    break;
                case 0x03:
                    payload += 0x04;
                    break;
                case 0x04:
                    payload += 0x08;
                    break;
                case 0x05:
                    payload += 0x04;
                    break;
                case 0x06:
                    payload += 0x08;
                    break;
                case 0x08:
                    payload += 0x01;
                    break;
                case 0x09:
                    payload += 0x04;
                    break;
                case 0x0A:
                    payload += 0x08;
                    break;
                case 0x0B:
                    payload += 0x08;
                    break;
                default: //string:  0x0C + n
                    payload += (header[i] - 0x0C);
            }
        }

        return 0;
    }


    // get all column names and datatypes from davisbase_columns associated with a given table
    // used for inserting a new record (to make header)
    public static Map<String, String> columnInfo(String tableName) throws Exception {

        // <column name, column data type>
        Map<String, String> nameAndType = new HashMap<String, String>();
        RandomAccessFile columnsFile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");

        //number of pages in columns table
        int maxPg = (int)(columnsFile.length()/DavisBasePrompt.pageSize - 1);
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



    public static byte typeToByte(String dataType, String value) {
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



    public static String byteToType(byte dataType, String value) {
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