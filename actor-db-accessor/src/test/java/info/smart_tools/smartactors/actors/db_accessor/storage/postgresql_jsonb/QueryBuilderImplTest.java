package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryStatement;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

import static org.mockito.Mockito.*;

public class QueryBuilderImplTest {
    private Connection connectionMock;
    private QueryBuilderImpl queryBuilder;
    private Map<Integer,Object> paramsSet = new HashMap<>();

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

        queryBuilder = new QueryBuilderImpl();
    }

    @Test(dataProvider = "valid-queries-provider", dataProviderClass = DataProviderForQueryBuilderImpl_SearchQueryBuildTest.class)
    public void testValidSearchQuery(SearchQueryMessage msg, String expectedSQL, Object[] queryParams)
        throws Exception {
        QueryStatement stmt = queryBuilder.buildSearchQuery(msg);
        stmt.compile(connectionMock);

        verify(connectionMock).prepareStatement(expectedSQL);

        assertEquals(paramsSet.size(), queryParams.length);

        for (int i = 0; i < queryParams.length; i++) {
            assertEquals(paramsSet.get(i+1),queryParams[i]);
        }
    }

    @Test(
            expectedExceptions = QueryBuildException.class,
            dataProvider = "invalid-queries-provider",
            dataProviderClass = DataProviderForQueryBuilderImpl_SearchQueryBuildTest.class)
    public void testInvalidSearchQueries (SearchQueryMessage msg)
        throws Exception{
        queryBuilder.buildSearchQuery(msg);
    }
}
