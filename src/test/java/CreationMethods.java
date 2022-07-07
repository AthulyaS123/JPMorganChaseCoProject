//import com.datastax.oss.driver.api.core.CqlSession;
//import com.datastax.oss.driver.api.core.cql.SimpleStatement;
//import java.util.*;
//
//import com.datastax.oss.driver.api.core.type.DataType;
//import com.datastax.oss.driver.api.core.type.DataTypes;
//import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
//import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
//import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
//import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
//import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumn;
//
//
//
///*
//CreationMethods (Create keyspace, table, and data)
//Created by Tommy Fang 7/3/2022       :)
//
//Notes for createKeyspace method:
//    Error will appear if you create a keyspace that already exists
//
//Notes for createTable method:
//    Must have PRIMARY KEY after a data type for at least one of the labels (preferably the first which should be an id label)
//    Must type 'end' once there are no more labels to be added
//
//Notes for createData method:
//    Don't put '.' in the table labels or this method may not work
//    Any other character is allowed (including space and special characters)
//    Make sure there are no duplicate labels or this method may not work
//    You need to enter a value. Default for null is '' and 0 for string and int respectively
//    Make sure you enter a different value for each row for labels with PRIMARY KEY
// */
//
//public class CreationMethods {
//    private CqlSession session;
//
//    //constructor
//    public CreationMethods(CqlSession session){
//        this.session=session;
//    }
//
//    public void createKeyspace(String keyspace){
//        CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(keyspace).ifNotExists()
//                .withSimpleStrategy(1);
//        SimpleStatement create = createKeyspace.build();
//        session.execute(create);
//    }
//
//    public void createTable(String keyspace, String table, Map<String,String>labels){
//        CreateTableStart createTable= SchemaBuilder.createTable(keyspace,table).ifNotExists();
//        for(Map.Entry<String, String> label : labels.entrySet()){
//            SimpleStatement statement = SchemaBuilder.alterTable(table).addColumn();
//            //alterTable(keyspace,table).addColumn(label.getKey(), label.getValue());
//        }
//
//
//        /*
//        String query="CREATE TABLE "+ name+"(";
//        for(Map.Entry<String, String> label : labels.entrySet()){
//            query=query+label.getKey()+" "+label.getValue()+", ";
//        }
//        query=query+");";
//        session.execute(query);
//        */
//    }
//
//    public void createData(ArrayList<String>tableLabels,ArrayList<String>rows,String tableName) {
//        String query = "INSERT INTO " + tableName + " (";
//        int iter = 0;
//        while (iter != tableLabels.size() - 1) {
//            query = query + tableLabels.get(iter) + ", ";
//            iter++;
//        }
//        query = query + tableLabels.get(iter) + ")";
//
//        for (String values : rows) {
//            String q = query + " VALUES(" + values + ");";
//            session.execute(q);
//        }
//    }
//}