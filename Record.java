public class Record {

    // payload size
    short payload;
    int rowid;
    // data type of columns
    short[] header;
    // column values
    String[] body;

    // page record is located on, maybe not necessary
    int page;
    // location of record on page, maybe not necessary
    int location;


    //TODO: make constructor

    //TODO: calculate payload

    //TODO: convert record data to hex

    //TODO: convert hex to record data


    public int typeToInt(String dataType, String value) {
        dataType.toLowerCase();
        switch (dataType) {
			case "null":
				return 0;
            case "tinyint":
				return 1;
            case "smallint":
				return 2;
            case "int":
                return 3;
            case "long":
                return 4;
            case "float":
                return 5;
            case "double":
                return 6;
            case "year":
                return 8;
            case "time":
                return 9;
            case "datetime":
                return 10;
            case "date":
                return 11;
            default: //string:  0x0C + n
				return value.length() + 12;
        }
    }

    public String IntToType(int dataType, String value) {
        switch (dataType) {
			case 0:
				return "null";
            case 1:
				return "tinyint";
            case 2:
				return "smallint";
            case 3:
                return "int";
            case 4:
                return "long";
            case 5:
                return "float";
            case 6:
                return "double";
            case 8:
                return "year";
            case 9:
                return "time";
            case 10:
                return "datetime";
            case 11:
                return "date";
            default: //string:  0x0C + n
				return "text";
        }
    }
}