import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.util.List;

//Athulya

public class EditTable {
    private CqlSession session;
    public EditTable(CqlSession session){
        this.session=session;
    }
    public List<ColumnMetadata> getPrimaryKey(String tableName, String ks){
        return session.getMetadata().getKeyspace(ks).get().getTable(tableName).get().getPrimaryKey();
    }
 /* Future Methods*/
}
