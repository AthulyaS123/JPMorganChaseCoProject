import com.datastax.oss.driver.api.core.CqlSession;
import java.util.Map;

/*
ModifierMethods (allows insert, edit, and delete capabilities for a table)
Created by Tommy, Athulya, and Vikas 7/11/22
 */

public class ModifierMethods {
    private CqlSession session;

    public ModifierMethods(CqlSession session){
        this.session=session;
    }
    public void deleteRow(String keyspace,String table, Map<String, String>PKV){
        StringBuilder query = new StringBuilder("DELETE FROM " + keyspace+"."+table + " WHERE ");
        PKV.forEach((key,value)->{
            query.append(key+"="+ value);
            query.append(" and ");
        });
        query.delete(query.length()-5,query.length());
        query.append(";");
        session.execute(String.valueOf(query));
    }

    public void insertRow(String keyspace,String table, Map<String, String>CTV){
        StringBuilder query = new StringBuilder("INSERT INTO " + keyspace+"."+table + "(");
        CTV.forEach((key,value)->{
            query.append(key+", ");
        });
        query.delete(query.length()-2,query.length());
        query.append(") VALUES(");
        CTV.forEach((key,value)->{
            query.append(value+", ");
        });
        query.delete(query.length()-2,query.length());
        query.append(");");
        session.execute(String.valueOf(query));
    }

    public void editRow(String keyspace,String table, Map<String, String>PKV,Map<String, String>CTV){
        deleteRow(keyspace,table, PKV);
        insertRow(keyspace, table, CTV);
    }
}
