package info.smart_tools.smartactors.actors.db_accessor.storage;

import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.DeletionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;

public interface QueryBuilder {
    QueryStatement buildSearchQuery(SearchQueryMessage message)
            throws QueryBuildException;
    QueryStatement buildDeleteQuery(DeletionQueryMessage message)
            throws QueryBuildException;
    QueryStatement buildCollectionCreationQuery(CreateCollectionQueryMessage message)
            throws QueryBuildException;
    QueryStatement buildUpdateQuery(UpsertQueryMessage message)
            throws QueryBuildException;
    QueryStatement buildInsertionQuery(UpsertQueryMessage message)
            throws QueryBuildException;
}
