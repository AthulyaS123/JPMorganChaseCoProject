import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.io.IOException;
import java.util.*;

public class ModifierMethodsRunner {
    public static void main(String args[]) throws IOException {
        String keyspaceName,tableName;
        int line;
        Map<CqlIdentifier, DataType>colDefs;
        ArrayList<String>labels;

        Scanner scan = new Scanner(System.in);
        CassandraConnector connector = new CassandraConnector();
        connector.connect("127.0.0.1", 9042, "datacenter1");
        ModifierMethods MM = new ModifierMethods(connector.getSession());
        KeyspaceRepository KS=new KeyspaceRepository(connector.getSession());

        //Test inputs for deleteRow method
        ///*
        System.out.println("Enter the name of your keyspace.");
        keyspaceName= scan.nextLine();
        System.out.println("Enter the name of your table.");
        tableName=scan.nextLine();
        System.out.println("Enter the line number of the row you want to delete.");
        line= scan.nextInt();
        colDefs=KS.getColDefs(keyspaceName,tableName);
        labels=new ArrayList<>();
        colDefs.forEach((key,value) -> {
            labels.add(key.toString());
        });
        ArrayList<Object>pkValues=new ArrayList<>();
        pkValues=KS.getPKValues(keyspaceName,tableName, labels.get(0));


        //MM.deleteRow(keyspaceName,tableName, CTV);
        //*/

        //Test inputs for editRow method
        /*
        System.out.println("Enter the name of your keyspace.");
        keyspaceName= scan.nextLine();
        System.out.println("Enter the name of your table.");
        tableName=scan.nextLine();
        colDefs=KS.getColDefs(keyspaceName,tableName);
        colDefs.forEach((key,value) -> {
            System.out.println("Enter the value in "+key+" of the row you want to delete.");
            String input = scan.nextLine();
            CTV.put(key.toString(),input);
        });
        MM.deleteRow(keyspaceName,tableName, CTV);
        */



        connector.close();
    }
}
