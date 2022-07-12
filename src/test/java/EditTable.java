import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

import java.lang.reflect.Member;
import java.util.*;

/*
Created by Athulya Saravanakumr 7/5/2022
- Thanks for the help Tommy & Vikas

Functionalities:
- Edit Table
    ~ insert
    ~ edit
    ~ delete
- Copy of Table Data
- TTL
 */

public class EditTable {

    private CqlSession session;
    private String keyspace;
    private String tableName;
    private int tu, ti = 0;


    public EditTable(CqlSession session, String ks){
        this.session=session;
        this.keyspace = ks;
        //pass keyspace paramter, 1 keyspace only
        //list of tabel
        //multiple tables 100+
        //enter keyspace, tables available, click on tabel, next screen specific table (for each tabel, have modify methods), each row - edit button, goes to next screen (input - pass primary key/id) click - editabel data, modify data, save tehn input what tabel it came through and what primayr key and what the new values are - update/set/column names

    }

    public String getKeySpace(){
        return keyspace;
    }

    public List<String> getPrimaryKeyLabels(String tableName){
        List<ColumnMetadata> urMom =  session.getMetadata().getKeyspace(keyspace).get().getTable(tableName).get().getPrimaryKey();
        List<String> labels = new ArrayList<>();
        urMom.forEach(x-> labels.add(x.getName().asCql(true)));
        return labels;
    }

    public void setTTL(int timeUpdate, int timeInsert, int timeDelete){
        //defualt = 0
        //max = 630720000 (20 years)
        tu = timeUpdate;
        ti = timeInsert;
    }


    public void updateData(String tableName, Map<String, Object>labelsandValues, Map<String, Object>labelsandnewValues){
        List<String> labels = getPrimaryKeyLabels(tableName);
        StringBuilder query =  new StringBuilder("UPDATE " + keyspace + "." + tableName);
        if(tu != 0){
            query.append(" USING TTL " + tu);
        }
        query.append(" SET ");
        labelsandnewValues.forEach((key,value)-> query.append(key + "=" + value.toString() + ", "));
        query.deleteCharAt(query.length()-2).append(" WHERE ");
        labelsandValues.forEach((key, value)-> query.append(key + "=" + value + " and "));
        query.delete(query.length()-5,query.length()).append(";");
        System.out.println(query);
        session.execute(String.valueOf(query));
    }

    public void deleteRow(String keyspace,String table, Map<String,String>CTV){
        StringBuilder query = new StringBuilder("DELETE FROM " + keyspace+"."+table + " WHERE ");
        CTV.forEach((key,value)->{
            query.append(key+"="+ value);
            query.append(" and ");
        });
        query.delete(query.length()-5,query.length());
        query.append(";");
        session.execute(String.valueOf(query));
    }

    public void insertData(String keyspace, String tableName, ArrayList<String>labels, ArrayList<String>rows) {
        String use="USE "+keyspace+";";
        session.execute(use);
        StringBuilder query = new StringBuilder("INSERT INTO " + tableName + " (");
        for(int i=0;i<labels.size()-1;i++){
            query.append(labels.get(i) +", ");
        }
        query.append(labels.get(labels.size()-1) + ")");
        for (String values : rows) {
            StringBuilder q = new StringBuilder(query + " VALUES(" + values + ")" +" USING TTL" + ti +";");
            session.execute(String.valueOf(q));
        }
    }

    public List<Row> getPrimaryKeyValue(boolean entireTable, String tableName){
            List<String> primaryKeyLabel = getPrimaryKeyLabels(tableName);
            StringBuilder query = new StringBuilder("SELECT ");
            primaryKeyLabel.forEach(x->query.append(x+", "));
            query.delete(query.length()-2, query.length());
            query.append(" FROM " + keyspace + "." + tableName);
        return session.execute(String.valueOf(query)).all();
    }

    public List<Row> readTable(boolean entireTable, String tableName){
        StringBuilder query;
        if(entireTable == true) {
            query = new StringBuilder("SELECT * FROM " + keyspace + "." + tableName);
        }
        else{
            List<String> primaryKeyLabel = getPrimaryKeyLabels(tableName);
            Scanner sc = new Scanner(System.in);
            System.out.println("What specific label do you want to read?");
            String label = sc.next();
            query = new StringBuilder("SELECT " + label + " FROM " + keyspace + "." + tableName);
            session.execute(String.valueOf(query));
        }
        return session.execute(String.valueOf(query)).all();
    }


    //insert, modify, drop - use better terms

    public void dropTable(String tableName){
        StringBuilder query;
        query = new StringBuilder("DROP TABLE " + tableName + ";");
        session.execute(String.valueOf(query));
    }
//-----------------------------------------------------

    public Map<CqlIdentifier, DataType> getColDefs(String keyspace, String table)
    {
        try {
            Map<CqlIdentifier, ColumnMetadata> map = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getColumns();
            Set<CqlIdentifier> set = map.keySet();
            Map<CqlIdentifier,DataType> joe = new HashMap<>();
            map.forEach((key,value) -> joe.put(key,value.getType()));
            return joe;
        }catch (Exception e)
        {
            return null;
        }
    }

    public void copyTable(String ks, String table) {
        Select select = QueryBuilder.selectFrom(ks, table).all();
        ResultSet rs = session.execute(select.build());
        ArrayList<Row>result=(ArrayList<Row>)rs.all();
        for(Row x:result){
            System.out.println(x); //-> @120398120941, @120948120, @1209821905  # of rows
        }
    }

//    public void getPrimaryKey(String tableName){
//        List<ColumnMetadata> map = session.getMetadata().getKeyspace(keyspace).get().getTable(tableName).get().getPrimaryKey();;
//
//    }
}

    /*
    col[0]=austin
    col[1][1]=930
    StringBuilder query="where".toString();
    for(x:col){
        query=query+x+"and";
    }
    //query=where austin and 930

     */



    /*
    delete from q where col2='austin' and col3=930 and col1=0!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ArrayList<String>


    VIKASSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS

    create table tableName (col1 text, col2 int, col3 text , col4 int, //primary key((col1,col2),(col3,col4)));


            austin 1030, dallas 1030, austin 930, dallas 730
            col1=[austin , dallas, austin, dallas]
            col1=[austin, austin, dallas, dallas]
            col2=[930, 1030, 730, 1030]
    */
