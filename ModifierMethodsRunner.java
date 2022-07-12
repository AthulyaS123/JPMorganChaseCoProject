import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import java.io.IOException;
import java.util.*;

/*
ModifierMethodsRunner (Run this to insert, edit, and delete from a table)
Created by Tommy, Athulya, and Vikas 7/11/22
 */

public class ModifierMethodsRunner {
    public static void main(String args[]) throws IOException {
        String keyspaceName,tableName;
        Map<CqlIdentifier, DataType>colDefs;
        Map<String, String>CTV=new HashMap<>();
        Map<String, String>PKV=new HashMap<>();
        List<String>PK;

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
        colDefs=KS.getColDefs(keyspaceName,tableName);
        colDefs.forEach((key,value) -> {
            System.out.println("Enter the value in "+key+" for the row you want to delete.");
            String input=scan.nextLine();
            CTV.put(key.toString(), input);
        });
        PK=KS.getPrimaryKeyLabels(keyspaceName,tableName);
        PK.forEach(label->{
            PKV.put(label, CTV.getOrDefault(label,"You messed up."));
        });
        MM.deleteRow(keyspaceName,tableName, PKV);
        //*/

        //Test inputs for insertRow method
        ///*
        System.out.println("Enter the name of your keyspace.");
        keyspaceName= scan.nextLine();
        System.out.println("Enter the name of your table.");
        tableName=scan.nextLine();
        colDefs=KS.getColDefs(keyspaceName,tableName);
        colDefs.forEach((key,value) -> {
            System.out.println("Enter the value you want to enter in "+key+".");
            String input=scan.nextLine();
            CTV.put(key.toString(), input);
        });
        MM.insertRow(keyspaceName,tableName, CTV);
        //*/

        //Test inputs for editRow method
        ///*
        System.out.println("Enter the name of your keyspace.");
        keyspaceName= scan.nextLine();
        System.out.println("Enter the name of your table.");
        tableName=scan.nextLine();
        Map<String,String>arb=new HashMap<>();
        colDefs=KS.getColDefs(keyspaceName,tableName);
        colDefs.forEach((key,value) -> {
            System.out.println("Enter the current value in "+key+".");
            String input=scan.nextLine();
            System.out.println("Enter the new value you want to enter in "+key+".");
            String add=scan.nextLine();
            CTV.put(key.toString(), input);
            arb.put(key.toString(), add);
        });
        PK=KS.getPrimaryKeyLabels(keyspaceName,tableName);
        PK.forEach(label->{
            PKV.put(label, CTV.getOrDefault(label,"You messed up."));
        });
        CTV.replaceAll((key,value)->(arb.get(key)));
        MM.editRow(keyspaceName,tableName, PKV,CTV);
        //*/


        connector.close();
    }
}
