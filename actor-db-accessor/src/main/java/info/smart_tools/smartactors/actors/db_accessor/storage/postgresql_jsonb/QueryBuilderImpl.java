package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.DeletionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.CollectionName;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryStatement;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuilder;
import info.smart_tools.smartactors.core.IObject;

import java.io.IOException;
import java.io.Writer;

class QueryBuilderImpl implements QueryBuilder {
    private ConditionsWriterResolverImpl conditionsWriterResolver = new ConditionsWriterResolverImpl();

    private static final int MAX_PAGE_SIZE = 10000;
    private static final int MIN_PAGE_SIZE = 1;

    public QueryStatement buildSearchQuery(SearchQueryMessage message)
            throws QueryBuildException {
        QueryStatement query = new QueryStatement();

        try {
            query.getBodyWriter().write(String.format("SELECT * FROM %s WHERE",
                    CollectionName.fromString(message.getCollectionName()).toString()));

            conditionsWriterResolver.resolve(null)
                    .write(query, conditionsWriterResolver, null, message.getQuery());

            query.getBodyWriter().write("LIMIT(?)OFFSET(?)");

            query.pushParameterSetter((statement, index) -> {
                int pageSize = message.getPageSize();
                int pageNumber = message.getPageNumber() - 1;

                pageNumber = (pageNumber < 0)?0:pageNumber;
                pageSize = (pageSize > MAX_PAGE_SIZE)?MAX_PAGE_SIZE:((pageSize < MIN_PAGE_SIZE)?MIN_PAGE_SIZE:pageSize);

                statement.setInt(index++,pageSize);
                statement.setInt(index++,pageSize*pageNumber);
                return index;
            });
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing search query statement.",e);
        }

        return query;
    }

    public QueryStatement buildDeleteQuery(DeletionQueryMessage message)
            throws QueryBuildException {
        throw new QueryBuildException("Not implemented.");
    }

    public QueryStatement buildCollectionCreationQuery(CreateCollectionQueryMessage message)
            throws QueryBuildException {
        throw new QueryBuildException("Not implemented.");
    }

    public QueryStatement buildUpdateQuery(UpsertQueryMessage message)
            throws QueryBuildException {
        throw new QueryBuildException("Not implemented.");
    }

    public QueryStatement buildInsertionQuery(UpsertQueryMessage message)
            throws QueryBuildException {
        QueryStatement query = new QueryStatement();

        try {
            Writer writer = query.getBodyWriter();

            writer.write(String.format("INSERT INTO %s (%s) VALUES",
                    CollectionName.fromString(message.getCollectionName()).toString(),
                    Schema.DOCUMENT_COLUMN_NAME));

            for (int i = message.getDocuments().size(); i > 0; --i) {
                writer.write(String.format("VALUES(?::jsonb)%s",(i==1)?"":","));
            }

            writer.write(String.format(" RETURNING %s AS id;",Schema.ID_COLUMN_NAME));

            query.pushParameterSetter((statement, index) -> {
                for(IObject document : message.getDocuments()) {
                    /*TODO: Is it a correct way to get document as JSON?*/
                    statement.setString(index++,document.toString());
                }
                return index;
            });
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing insertion query statement.",e);
        }

        return query;
    }
}
