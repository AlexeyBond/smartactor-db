package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryStatement;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuilder;

import java.io.IOException;

abstract class QueryBuilderImpl implements QueryBuilder {
    private ConditionsWriterResolverImpl conditionsWriterResolver = new ConditionsWriterResolverImpl();

    public QueryStatement buildSearchQuery(SearchQueryMessage message)
            throws QueryBuildException {
        QueryStatement query = new QueryStatement();

        try {
            query.getBodyWriter().write(String.format("SELECT * FROM %s WHERE",message.getCollectionName()));

            conditionsWriterResolver.resolve(null)
                    .write(query, conditionsWriterResolver, null, message.getQuery());

            query.getBodyWriter().write("LIMIT ? OFFSET ?");

            query.pushParameterSetter((statement, index) -> {
                statement.setInt(index++,message.getPageSize());
                statement.setInt(index++,message.getPageSize()*(message.getPageNumber()-1));
                return index;
            });
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing search query statement.",e);
        }

        return query;
    }
}
