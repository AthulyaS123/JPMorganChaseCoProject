import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
ModifierMethods (allows insert, edit, and delete capabilities for a table)
Created by Tommy, Athulya, and Vikas 7/11/22
 */

public class ModifierMethods {
    private CqlSession session;
    public ModifierMethods(CqlSession session){
        this.session=session;
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

    public List<Object> getFirstColumn(ArrayList<String>first){
        Select select = QueryBuilder.selectFrom("letsdothis", "test").all();
        ResultSet rs = session.execute(select.build());
        List<Object> result = new ArrayList<>();

        System.out.println(first.get(0));

        //rs.forEach(x -> result.add(x.getString(first.get(0))));


        rs.forEach(x -> result.add(x.getObject("col3")));
        return result;
    }


}
