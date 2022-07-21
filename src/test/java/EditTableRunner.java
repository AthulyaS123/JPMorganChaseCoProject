import java.io.IOException;


public class EditTableRunner {
    public static void main(String args[]) throws IOException {
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "datacenter1");
//        EditTable table = new EditTable(connector.getSession(), "tutorialspoint");
//        TableMethods tableMethods = new TableMethods(connector.getSession());
        AccessKeySpace urMOM = new AccessKeySpace(connector.getSession());
        System.out.println(urMOM.clusterName());

//        System.out.println(urMOM.getTableSizes());
//        ArrayList<String> labels = new ArrayList<>();
//        labels.add("name");
//        labels.add("grade");
//        Scanner scan = new Scanner(System.in);
//        int row=1; String data=""; ArrayList<String>rows=new ArrayList<String>();
//        while(true){
//            System.out.println("Enter the values to be entered in row "+row+" as a single line with values separated by a comma. Type end after entering all data.");
//            System.out.println("Ex. 777777, '', 0, 'hello'");
//            data=scan.nextLine();
//            if(data.equals("end"))break;
//            rows.add(data);
//            row++;
//        }
//        tableMethods.createData("tutorialspoint", "athulya", labels, rows);
//        table.deleteRow("athulya", "name", "\'athulya\'");
//        List<Row> hi = table.readTable(true, "athulya");
//        hi.forEach(x -> hi.add(x.get("col3")));

//        urMOM.getTableSizes();

//        Map<String, Object> old = new HashMap<>();
//        old.put("name", "\'homie\'" );
//        Map<String, Object> newValues = new HashMap<>();
//        newValues.put("grade", 86);
//        table.updateData("athulya",old, newValues);
//        System.out.println(table.getPrimaryKeyLabels("athulya"));
        connector.close();
    }
}

//j unit test cases - use this instead
