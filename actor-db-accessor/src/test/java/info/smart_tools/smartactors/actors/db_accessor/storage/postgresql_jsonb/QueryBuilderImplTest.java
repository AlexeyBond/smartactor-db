package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.CollectionName;
import info.smart_tools.smartactors.actors.db_accessor.storage.FieldPath;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryStatement;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.testng.Assert.*;

import static org.mockito.Mockito.*;

public class QueryBuilderImplTest {
    private Connection connectionMock;
    private QueryBuilderImpl queryBuilder;
    private Map<Integer,Object> paramsSet = new HashMap<>();

    private DataProviderForQueryBuilderImplTest<SearchQueryMessage> searchQueryMessageDataProvider;
    private DataProviderForQueryBuilderImplTest<CreateCollectionQueryMessage> createCollectionQueryMessageDataProvider;
    private DataProviderForQueryBuilderImplTest<UpsertQueryMessage> insertQueryMessageDataProvider;
    private DataProviderForQueryBuilderImplTest<UpsertQueryMessage> updateQueryMessageDataProvider;

    @BeforeMethod
    public void setUp() throws Exception {
        PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
        connectionMock = mock(Connection.class);

        Answer<Void> setParamAnswer = invocationOnMock -> {
            paramsSet.put((Integer)invocationOnMock.getArguments()[0],invocationOnMock.getArguments()[1]);
            return null;
        };

        paramsSet.clear();

        when(connectionMock.prepareStatement(any())).thenReturn(preparedStatementMock);
        doAnswer(setParamAnswer).when(preparedStatementMock).setObject(anyInt(),anyObject());
        doAnswer(setParamAnswer).when(preparedStatementMock).setInt(anyInt(),anyInt());
        doAnswer(setParamAnswer).when(preparedStatementMock).setLong(anyInt(), anyLong());
        doAnswer(setParamAnswer).when(preparedStatementMock).setString(anyInt(), anyString());

        queryBuilder = new QueryBuilderImpl();
    }

    protected void testStatement(QueryStatement statement, String expectedSQL, Object[] expectedParams)
        throws Exception {
        statement.compile(connectionMock);

        verify(connectionMock).prepareStatement(expectedSQL);

        assertEquals(paramsSet.size(), expectedParams.length);

        for (int i = 0; i < expectedParams.length; i++) {
            assertEquals(paramsSet.get(i+1),expectedParams[i]);
        }
    }

    @DataProvider(name = "valid-search-queries-provider")
    public Iterator<Object[]> validSearchQueriesProvider() {
        return searchQueryMessageDataProvider.provideValidQueries();
    }

    @DataProvider(name = "invalid-search-queries-provider")
    public Iterator<Object[]> invalidSearchQueriesProvider() {
        return searchQueryMessageDataProvider.provideInvalidQueries();
    }

    @Test(dataProvider = "valid-search-queries-provider")
    public void testValidSearchQuery(SearchQueryMessage msg, String expectedSQL, Object[] expectedParams)
            throws Exception {
        testStatement(queryBuilder.buildSearchQuery(msg), expectedSQL, expectedParams);
    }

    @Test(
            expectedExceptions = QueryBuildException.class,
            dataProvider = "invalid-search-queries-provider")
    public void testInvalidSearchQueries (SearchQueryMessage msg)
            throws Exception{
        queryBuilder.buildSearchQuery(msg).compile(connectionMock);
    }

    @DataProvider(name = "valid-insert-queries-provider")
    public Iterator<Object[]> validInsertQueriesProvider() {
        return insertQueryMessageDataProvider.provideValidQueries();
    }

    @DataProvider(name = "invalid-insert-queries-provider")
    public Iterator<Object[]> invalidInsertQueriesProvider() {
        return insertQueryMessageDataProvider.provideInvalidQueries();
    }

    @Test(dataProvider = "valid-insert-queries-provider")
    public void testValidInsertQuery(UpsertQueryMessage msg, String expectedSQL, Object[] expectedParams)
            throws Exception {
        testStatement(queryBuilder.buildInsertionQuery(msg), expectedSQL, expectedParams);
    }

    @Test(
            expectedExceptions = QueryBuildException.class,
            dataProvider = "invalid-insert-queries-provider")
    public void testInvalidInsertQueries (UpsertQueryMessage msg)
            throws Exception{
        queryBuilder.buildInsertionQuery(msg).compile(connectionMock);
    }

    @DataProvider(name = "valid-update-queries-provider")
    public Iterator<Object[]> validUpdateQueriesProvider() {
        return updateQueryMessageDataProvider.provideValidQueries();
    }

    @DataProvider(name = "invalid-update-queries-provider")
    public Iterator<Object[]> invalidUpdateQueriesProvider() {
        return updateQueryMessageDataProvider.provideInvalidQueries();
    }

    @Test(dataProvider = "valid-update-queries-provider")
    public void testValidUpdateQuery(UpsertQueryMessage msg, String expectedSQL, Object[] expectedParams)
            throws Exception {
        testStatement(queryBuilder.buildUpdateQuery(msg), expectedSQL, expectedParams);
    }

    @Test(
            expectedExceptions = QueryBuildException.class,
            dataProvider = "invalid-update-queries-provider")
    public void testInvalidUpdateQueries (UpsertQueryMessage msg)
            throws Exception{
        queryBuilder.buildUpdateQuery(msg).compile(connectionMock);
    }

    @DataProvider(name = "valid-collection-creation-queries-provider")
    public Iterator<Object[]> validCollectionCreationQueriesProvider() {
        return createCollectionQueryMessageDataProvider.provideValidQueries();
    }

    @DataProvider(name = "invalid-collection-creation-queries-provider")
    public Iterator<Object[]> invalidCollectionCreationQueriesProvider() {
        return createCollectionQueryMessageDataProvider.provideInvalidQueries();
    }

    @Test(dataProvider = "valid-collection-creation-queries-provider")
    public void testValidCollectionCreationQuery(CreateCollectionQueryMessage msg, String expectedSQL, Object[] expectedParams)
            throws Exception {
        testStatement(queryBuilder.buildCollectionCreationQuery(msg), expectedSQL, expectedParams);
    }

    @Test(
            expectedExceptions = QueryBuildException.class,
            dataProvider = "invalid-collection-creation-queries-provider")
    public void testInvalidCollectionCreationQueries (CreateCollectionQueryMessage msg)
            throws Exception{
        queryBuilder.buildCollectionCreationQuery(msg).compile(connectionMock);
    }

    @BeforeClass
    public void setUpQueryProviders() throws Exception {
        searchQueryMessageDataProvider = new DataProviderForQueryBuilderImplTest<SearchQueryMessage>() {
            @Override
            protected void initQueries() throws Exception {
                addValidQuery(makeSearchQueryMessage("test", "{\"asd\":{\"$eq\":42}}", 15, 2),
                        String.format(
                                "SELECT * FROM %s WHERE((((%s)=to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString(),
                                FieldPathImpl.fromString("asd").getSQLRepresentation()),
                        new Object[]{42,15,15});

                addValidQuery(makeSearchQueryMessage("test", "{\"asd\":{\"$lt\":96}}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((%s)<to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString(),
                                FieldPathImpl.fromString("asd").getSQLRepresentation()),
                        new Object[]{96,10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"asd\":{\"$gt\":\"lorem ipsum\"}}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((%s)>to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString(),
                                FieldPathImpl.fromString("asd").getSQLRepresentation()),
                        new Object[]{"lorem ipsum",10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"asd\":{\"$gte\":18}}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((%s)>=to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString(),
                                FieldPathImpl.fromString("asd").getSQLRepresentation()),
                        new Object[]{18,10,0});

                addValidQuery(makeSearchQueryMessage("test", "{}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE(true)LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString()),
                        new Object[]{10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"$and\":[]}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((true))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString()),
                        new Object[]{10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"body.tags\":{\"$hasTag\":\"someSuperTag\"}}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((%s)??(?))))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString(),
                                FieldPathImpl.fromString("body.tags").getSQLRepresentation()),
                        new Object[]{"someSuperTag",10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"$not\":[{\"$and\":[]},{\"$and\":[]}]}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((NOT(((true))AND((true)))))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString()),
                        new Object[]{10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"$or\":[{\"$and\":[]},{\"$and\":[]}]}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((true))OR((true))))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString()),
                        new Object[]{10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"$and\":[{\"$and\":[]},{\"$and\":[]}]}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((true))AND((true))))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString()),
                        new Object[]{10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"$and\":[],\"$or\":[]}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((true)AND(true))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString()),
                        new Object[]{10,0});

                addValidQuery(makeSearchQueryMessage("test", "{\"fieldA\":{\"$in\":[1,2,\"foo\"]}}", 10, 1),
                        String.format(
                                "SELECT * FROM %s WHERE((((%s)in(to_json(?)::jsonb,to_json(?)::jsonb,to_json(?)::jsonb))))LIMIT(?)OFFSET(?)",
                                CollectionName.fromString("test").toString(),
                                FieldPathImpl.fromString("fieldA").getSQLRepresentation()),
                        new Object[]{1,2,"foo",10,0});

                /*Should fail when field operators are used outside of field context.*/
                addInvalidQuery(makeSearchQueryMessage("test", "{\"$eq\":100}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("test", "{\"$lt\":100}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("test", "{\"$gt\":100}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("test", "{\"$gte\":100}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("test", "{\"$lte\":100}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("test", "{\"$hasTag\":100}", 10, 1));

                addInvalidQuery(makeSearchQueryMessage("test", "{\"$notTheOperator\":[]}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("test", "{\"inv@lidField\":[]}", 10, 1));
                addInvalidQuery(makeSearchQueryMessage("inv@lidCollectionN@me", "{}", 10, 1));

                addInvalidQuery(makeSearchQueryMessage("test", "{\"$and\":\"notACollection\"}", 10, 1));
        }};

        /*COLLECTION CREATION tests*/
        createCollectionQueryMessageDataProvider = new DataProviderForQueryBuilderImplTest<CreateCollectionQueryMessage>() {
            @Override
            protected void initQueries() throws Exception {
                /*create table for collection with NO INDEXES*/
                addValidQuery(
                        makeCollectionCreationMessage("test", new HashMap<String, String>() {{
                        }}),
                        String.format("CREATE TABLE %s (%s %s PRIMARY KEY, %s JSONB NOT NULL);\n",
                                "test", Schema.ID_COLUMN_NAME, Schema.ID_COLUMN_SQL_TYPE, Schema.DOCUMENT_COLUMN_NAME),
                        new Object[]{});
                /*create table for collection with TAGS INDEX*/
                addValidQuery(
                        makeCollectionCreationMessage("test", new HashMap<String, String>() {{
                            put("meta.tags", "tags");
                        }}),
                        String.format("CREATE TABLE %s (%s %s PRIMARY KEY, %s JSONB NOT NULL);\n" +
                                        "CREATE INDEX ON %s USING GIN (%s);\n",
                                "test", Schema.ID_COLUMN_NAME, Schema.ID_COLUMN_SQL_TYPE, Schema.DOCUMENT_COLUMN_NAME,
                                "test", FieldPathImpl.fromString("meta.tags").getSQLRepresentation()),
                        new Object[]{});
                /*create table for collection with ORDERED INDEX*/
                addValidQuery(
                        makeCollectionCreationMessage("test", new HashMap<String, String>() {{
                            put("price", "ordered");
                        }}),
                        String.format("CREATE TABLE %s (%s %s PRIMARY KEY, %s JSONB NOT NULL);\n" +
                                        "CREATE INDEX ON %s USING BTREE (%s);\n",
                                "test", Schema.ID_COLUMN_NAME, Schema.ID_COLUMN_SQL_TYPE, Schema.DOCUMENT_COLUMN_NAME,
                                "test", FieldPathImpl.fromString("price").getSQLRepresentation()),
                        new Object[]{});
                /*NOT create table with UNKNOWN INDEX TYPE*/
                addInvalidQuery(
                        makeCollectionCreationMessage("test",new HashMap<String,String>(){{
                            put("some.field","badIndexType");
                        }}));
            }
        };

        insertQueryMessageDataProvider = new DataProviderForQueryBuilderImplTest<UpsertQueryMessage>() {
            @Override
            protected void initQueries() throws Exception {
                String[] jsons1 = new String[] {"{\"name\":\"Crazy document\"}","{}"};

                addValidQuery(
                        makeUpsertMessage("testCollection",jsons1),
                        String.format("INSERT INTO %s (%s) VALUES(?::jsonb),(?::jsonb) RETURNING %s AS id;",
                                CollectionName.fromString("testCollection").toString(),
                                Schema.DOCUMENT_COLUMN_NAME,Schema.ID_COLUMN_NAME),
                        jsons1);
            }
        };

        updateQueryMessageDataProvider = new DataProviderForQueryBuilderImplTest<UpsertQueryMessage>() {
            @Override
            protected void initQueries() throws Exception {
                String[] jsonsOk = new String[] {"{\"id\":100500}","{\"id\":45003}"};
                String[] jsonsNoId = new String[] {"{\"name\":\"Crazy document\"}"};
                String[] jsonsBadId = new String[] {"{\"id\":\"notId\"}"};

                addValidQuery(
                        makeUpsertMessage("test",jsonsOk),
                        String.format("UPDATE %s AS tab SET %s = docs.document FROM (VALUES" +
                                "(?,?::jsonb),(?,?::jsonb)) AS docs (id, document) WHERE tab.%s = docs.id;",
                                CollectionName.fromString("test").toString(),
                                Schema.DOCUMENT_COLUMN_NAME,Schema.ID_COLUMN_NAME),
                        new Object[] {100500l,jsonsOk[0],45003l,jsonsOk[1]});

                addInvalidQuery(makeUpsertMessage("test",jsonsNoId));
                addInvalidQuery(makeUpsertMessage("test",jsonsBadId));
            }
        };
    }
}
