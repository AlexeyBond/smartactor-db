package info.smart_tools.smartactors.actors.db_accessor.storage;

/**
 *  Contains methods specific for some storage implementation.
 */
public interface StorageDriver {
    /**
     *  @return QueryBuilder building queries for this storage implementation.
     */
    QueryBuilder getQueryBuilder();

    /**
     *  @return QueryExecutor executing queries for this storage implementation.
     */
    QueryExecutor getQueryExecutor();

    /**
     *  @return name of JDBC driver class used in this implementation.
     */
    String getJDBCDriverName();
}
