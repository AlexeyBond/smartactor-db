package info.smart_tools.smartactors.actors.db_accessor.storage;

public interface StorageDriver {
    QueryBuilder getQueryBuilder();
    QueryExecutor getQueryExecutor();

    String getJDBCDriverName();
}
