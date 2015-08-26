package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuilder;
import info.smart_tools.smartactors.actors.db_accessor.storage.StorageDriver;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryExecutor;

public class Driver implements StorageDriver {
    private QueryBuilderImpl queryBuilder = null/*new QueryBuilderImpl()*/;
    private QueryExecutorImpl queryExecutor = null/*new QueryExecutorImpl()*/;

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }

    public String getJDBCDriverName() {
        return "org.postgresql.Driver";
    }
}
