package info.smart_tools.smartactors.actors.db_accessor.storage;

import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.DeletionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;

import java.sql.PreparedStatement;

public interface QueryExecutor {
    void executeSearchQuery(PreparedStatement statement,SearchQueryMessage message)
            throws QueryExecutionException;
    void executeDeleteQuery(PreparedStatement statement,DeletionQueryMessage message)
            throws QueryExecutionException;
    void executeCollectionCreationQuery(PreparedStatement statement,CreateCollectionQueryMessage message)
            throws QueryExecutionException;
    void executeUpdateQuery(PreparedStatement statement,UpsertQueryMessage message)
            throws QueryExecutionException;
    void executeInsertionQuery(PreparedStatement statement,UpsertQueryMessage message)
            throws QueryExecutionException;
}
