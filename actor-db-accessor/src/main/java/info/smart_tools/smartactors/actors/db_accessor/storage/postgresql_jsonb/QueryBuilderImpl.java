package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.DeletionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.*;
import info.smart_tools.smartactors.core.FieldName;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.ReadValueException;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

class QueryBuilderImpl implements QueryBuilder {
    private ConditionsWriterResolverImpl conditionsWriterResolver = new ConditionsWriterResolverImpl();

    private static final int MAX_PAGE_SIZE = 10000;
    private static final int MIN_PAGE_SIZE = 1;

    private static Map<String,String> indexCreationTemplates = new HashMap<String,String>() {{
        put("ordered","CREATE INDEX ON %s USING BTREE (%s);\n");
        put("tags","CREATE INDEX ON %s USING GIN (%s);\n");
    }};

    private static void writeIndexCreationStatement(QueryStatement queryStatement,CollectionName collectionName,
                                                    String indexType,FieldPath fieldPath)
            throws QueryBuildException {
        String tpl = indexCreationTemplates.get(indexType);

        if(tpl == null) {
            throw new QueryBuildException("Invalid index type: "+indexType);
        }

        try {
            queryStatement.getBodyWriter().write(String.format(tpl,
                    collectionName.toString(), fieldPath.getSQLRepresentation()));
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing index creation statement.",e);
        }
    }

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
        QueryStatement query = new QueryStatement();
        CollectionName collectionName = CollectionName.fromString(message.getCollectionName());

        try {
            Writer writer = query.getBodyWriter();
            writer.write(String.format("CREATE TABLE %s (%s %s PRIMARY KEY, %s JSONB NOT NULL);\n",
                    collectionName.toString(),
                    Schema.ID_COLUMN_NAME,Schema.ID_COLUMN_SQL_TYPE,
                    Schema.DOCUMENT_COLUMN_NAME));

            for(Map.Entry<String,String> entry : message.getIndexes().entrySet()) {
                FieldPath field = FieldPathImpl.fromString(entry.getKey());
                writeIndexCreationStatement(query,collectionName,entry.getValue(),field);
            }
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing collection creation statement.",e);
        }

        return query;
    }

    public QueryStatement buildUpdateQuery(UpsertQueryMessage message)
            throws QueryBuildException {
        QueryStatement query = new QueryStatement();

        if (message.getDocuments().size() == 0) {
            throw new QueryBuildException("Documents list should not be empty for update query.");
        }

        try {
            Writer writer = query.getBodyWriter();

            writer.write(String.format("UPDATE %s AS tab SET %s = docs.document FROM (VALUES",
                    CollectionName.fromString(message.getCollectionName()).toString(),
                    Schema.DOCUMENT_COLUMN_NAME));

            for (int i = message.getDocuments().size(); i > 0; --i) {
                writer.write("(?,?::jsonb)"+((i==1)?"":","));
            }

            writer.write(String.format(") AS docs (id, document) WHERE tab.%s = docs.id;",Schema.ID_COLUMN_NAME));

            query.pushParameterSetter((statement, index) -> {
                for(IObject document : message.getDocuments()) {
                    try {
                        statement.setLong(index++, Long.parseLong(document.getValue(new FieldName("id")).toString()));
                    } catch (ReadValueException | NullPointerException | NumberFormatException e) {
                        throw new QueryBuildException("Error while writing update query statement: could not read document's id.",e);
                    }
                    /*TODO: Is it a correct way to get document as JSON?*/
                    statement.setString(index++,document.toString());
                }
                return index;
            });
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing update query statement.",e);
        }

        return query;
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
                writer.write(String.format("(?::jsonb)%s",(i==1)?"":","));
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
