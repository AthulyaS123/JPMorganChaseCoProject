import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.io.IOException;
import java.util.*;

//Created by Vikas in 2021
//UPDATED BY ATHULYA, AND TOMMY 7/20/22

public class AccessKeySpace {
    private CqlSession session;
    private ArrayList<String> xd = new ArrayList<>();
    public AccessKeySpace(CqlSession session) {
        this.session = session;
        xd.add("B");
        xd.add("KB");
        xd.add("MB");
        xd.add("GB");
        xd.add("TB");
        xd.add("PB");
        xd.add("EB");
        xd.add("ZB");
    }

    public List<String> getKeyspaceList() {
        Select select = QueryBuilder.selectFrom("system_schema", "keyspaces").all();
        ResultSet rs = session.execute(select.build());
        List<String> result = new ArrayList<>();
        rs.forEach(x -> result.add(x.getString("keyspace_name")));
        return result;
    }

    public List<String> getTableList(String keyspace)
    {
        Select select = QueryBuilder.selectFrom("system_schema", "tables").all();
        if (keyspace != null) {
            keyspace = keyspace.toLowerCase();
            select = select.where(Relation.column("keyspace_name").isEqualTo(QueryBuilder.literal(keyspace)));
        }
        ResultSet rs = session.execute(select.build());
        List<String> result = new ArrayList<>();
        rs.forEach(x -> result.add(x.getString("table_name")));
        return result;
    }

    public List<List<String>> getRows(String keyspace, String table){
        Select select=QueryBuilder.selectFrom(keyspace, table).all();
        ResultSet rs=session.execute(select.build());
        List<List<String>>result=new ArrayList<>();
        Map<CqlIdentifier,DataType>cd=getColDefs(keyspace,table);
        rs.forEach(princesslya->{
            List<String>vikasnanna=new ArrayList<>();
            cd.forEach((key,value)->{
                vikasnanna.add(princesslya.getObject(key).toString());
            });
            result.add(vikasnanna);
        });
        return result;
    }

    public Map<CqlIdentifier,DataType> getColDefs(String keyspace, String table)
    {
        try {
            Map<CqlIdentifier, ColumnMetadata> map = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getColumns();
            Set<CqlIdentifier> set = map.keySet();

            //System.out.println(map);
            Map<CqlIdentifier,DataType>joe=new HashMap<>();
            map.forEach((key,value) -> joe.put(key,value.getType()));

            return joe;
        }
        catch (Exception e){
            return null;
        }
    }

    public List<String> getPartitionVarList(String keyspace, String table)
    {
        try {
            List<ColumnMetadata> ace = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getPartitionKey();
            List<String> colNames = new ArrayList<>();
            for (ColumnMetadata base : ace) {
                colNames.add(base.getName().toString());
            }
            return colNames;
        }
        catch (Exception e) {
            return(new ArrayList<String>());
        }
    }

    public List<String> getPrimaryKeyLabels(String keyspace, String table){
        List<ColumnMetadata> data =  session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getPrimaryKey();
        List<String> labels = new ArrayList<>();
        data.forEach(x-> labels.add(x.getName().asCql(true)));
        return labels;
    }

    public List<DataType> getPartitionVarTypeList(String keyspace, String table)
    {
        try {
            List<ColumnMetadata> ace = session.getMetadata().getKeyspace(keyspace).get().getTable(table).get().getPartitionKey();
            List<DataType> colTypes = new ArrayList<>();
            for (ColumnMetadata base : ace) {
                colTypes.add(base.getType());
            }
            return colTypes;
        }
        catch (Exception e) {
            return(new ArrayList<DataType>());
        }
    }
    public List<String> getPartitionList(String keyspace, String table)
    {
        List<String> colNames = getPartitionVarList(keyspace, table);
        List<DataType> colTypes = getPartitionVarTypeList(keyspace, table);
        if(colNames.isEmpty())
            return new ArrayList<>();
        keyspace = keyspace.toLowerCase();
        table = table.toLowerCase();
        Select select = QueryBuilder.selectFrom(keyspace, table).columns(colNames);
        ResultSet y = session.execute(select.build());
        Set<String> diffStrings = new TreeSet<>();
        y.forEach(x -> {StringBuilder sb = new StringBuilder();
            sb.append('(');
            for(int i=0; i < colNames.size(); i++)
            {
                sb.append(conversion(colNames.get(i), colTypes.get(i), x) + ", ");
            }
            sb.delete(sb.length()-2, sb.length());
            sb.append(')');
            diffStrings.add(sb.toString());
        });
        List<String> parts = new ArrayList<>();
        for(String x: diffStrings)
        {
            parts.add(x);
        }
        return parts;
    }
    public Map<String, Integer> getRowsPerPartition(String keyspace, String table)
    {
        List<String> colNames = getPartitionVarList(keyspace, table);
        List<DataType> colTypes = getPartitionVarTypeList(keyspace, table);
        if(colNames.isEmpty())
            return new TreeMap<String, Integer>();
        keyspace = keyspace.toLowerCase();
        table = table.toLowerCase();
        Select select = QueryBuilder.selectFrom(keyspace, table).columns(colNames);
        ResultSet y = session.execute(select.build());
        Map<String, Integer> partitionKeysTONumInPartition = new HashMap<>();
        y.forEach(x -> {StringBuilder sb = new StringBuilder();
            sb.append('(');
            for(int i=0; i < colNames.size(); i++)
            {
                sb.append(conversion(colNames.get(i), colTypes.get(i), x) + ", ");
            }
            sb.delete(sb.length()-2, sb.length());
            sb.append(')');
            if(partitionKeysTONumInPartition.containsKey(sb.toString()))
            {
                partitionKeysTONumInPartition.put(sb.toString(), partitionKeysTONumInPartition.get(sb.toString())+1);
            }
            else
            {
                partitionKeysTONumInPartition.put(sb.toString(), 1);
            }
        });
        return partitionKeysTONumInPartition;
    }

    public String clusterName() throws IOException
    {
        String query = "SELECT cluster_name FROM system.local;";
        //list of rows
        ResultSet resultSet = session.execute(query);
        String cluster = "";
        for(Row x:resultSet){
            cluster = x.getString("cluster_name");
        }
        return cluster;
    }

    public String statsTable(Map<String, Integer> rPP)
    {
        if(rPP.size()==0)
            return "";
        Set<String> partitions = rPP.keySet();
        int min = Integer.MAX_VALUE, max = -1;
        double avg = 0.0;
        for(String part : partitions)
        {
            int num = rPP.get(part);
            min = Math.min(num, min);
            max = Math.max(num, max);
            avg+= num;
        }
        avg/=rPP.size();
        return "Min: " + min + " | Max: " + max + " | Average: " + avg;
    }
    public String statsPart(Map<String, Integer> rPP, String size)
    {
        int i = size.indexOf("B");
        int maxRows = -1;
        int totalRows = 0;
        Set<String> keyset = rPP.keySet();
        for(String bababoi : keyset)
        {
            maxRows = Math.max(maxRows, rPP.get(bababoi));
            totalRows += rPP.get(bababoi);
        }
        int d;
        double bites;
        if(size.charAt(i-2)!=' ')
        {
            i++;
        }
        d = xd.indexOf(size.substring(i-1));
        totalRows = Math.max(totalRows, 1);
        if(rPP.size()==0)
        {
            bites = Double.parseDouble(size.substring(0, i-2));
        }
        else {
            bites = Double.parseDouble(size.substring(0, i - 2)) * maxRows / totalRows;
        }
        while(bites>1)
        {
            bites /= 1000;
            d++;
        }
        if(d>0&&bites<1)
        {
            bites *= 1000;
            d--;
        }
        bites = Math.max(bites, 0);
        return "Max Partition Size: " + String.format("%.3f %s", bites, xd.get(d));
    }

    public Map<String,String> getTableSizes() throws IOException
    {
        Map<String, String> tommysMother = new TreeMap<>();
        StringBuilder query = new StringBuilder();
        query.append("SELECT mean_partition_size, partitions_count FROM system.size_estimates");
        List<String> ksl = getKeyspaceList();
        ksl.forEach(x->{
            List<String> gtl = getTableList(x);
            gtl.forEach(i->{
                String hello = " WHERE keyspace_name=\'" + x + "\' and table_name=\'" + i + "\';";
                query.append(hello);
                ResultSet resultSet = session.execute(query.toString());
                Long xd = 0L;
                for(Row j: resultSet){
                    xd += (Long)(j.getObject("mean_partition_size")) * (Long)(j.getObject("partitions_count"));
                }
                double bites = (double) xd;

                long d = 0l;
                while(bites>1)
                {
                    bites /= 1000;
                    d++;
                }
                if(d>0&&bites<1)
                {
                    bites *= 1000;
                    d--;
                }
                bites = Math.max(bites, 0);
                tommysMother.put(i, String.format("%.3f %s", bites, this.xd.get((int)d)));
                query.delete(query.length()-hello.length(), query.length());
            });
        });
        return tommysMother;
    }

    private String conversion(String col, DataType a, Row b){
        if(a.equals(DataTypes.ASCII))
        {
            return b.getString(col);
        }
        else if(a.equals(DataTypes.BIGINT))
        {
            return String.valueOf(b.getLong(col));
        }
        else if(a.equals(DataTypes.BLOB))
        {
            return "String.valueOf(b.getLong(col))";// TO BE IMPLEMENTED
        }
        else if(a.equals(DataTypes.BOOLEAN))
        {
            return String.valueOf(b.getBoolean(col));
        }
        else if(a.equals(DataTypes.COUNTER))
        {
            return "String.valueOf(b.getLong(col))";// TO BE IMPLEMENTED
        }
        else if(a.equals(DataTypes.BIGINT))
        {
            return String.valueOf(b.getLong(col));
        }
        else if(a.equals(DataTypes.DATE))
        {
            return "String.valueOf(b.getString(col))"; // TO BE IMPLEMENTED
        }
        else if(a.equals(DataTypes.DECIMAL))
        {
            return b.getBigDecimal(col).toString();
        }
        else if(a.equals(DataTypes.DOUBLE))
        {
            return String.valueOf(b.getDouble(col));
        }
        else if(a.equals(DataTypes.FLOAT))
        {
            return String.valueOf(b.getFloat(col));
        }
        else if(a.equals(DataTypes.INET))
        {
            return b.getInetAddress(col).toString();
        }
        else if(a.equals(DataTypes.INT))
        {
            return String.valueOf(b.getInt(col));
        }
        else if(a.equals(DataTypes.TIMESTAMP))
        {
            return b.getInstant(col).toString();
        }
        else if(a.equals(DataTypes.TEXT))
        {
            return b.getString(col);
        }
        // SOME TYPES YET TO BE IMPLEMENTED
        return "";
    }
}