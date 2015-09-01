package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryExecutionException;
import info.smart_tools.smartactors.core.FieldName;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.impl.SMObject;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.testng.Assert.*;

import static org.mockito.Mockito.*;

public class QueryExecutorImpl_InsertionQueryExecutionTest {

    @Test
    public void Should_setIdentifiersSuccessfully_When_countsOfIdentifiersAndDocumentsAreEqual() throws Exception {
        tryExecuteInsertQuery(10,10);
    }

    @Test(expectedExceptions = QueryExecutionException.class)
    public void Should_Fail_When_ThereIsTooMuchIdentifiersReturned() throws Exception {
        tryExecuteInsertQuery(6,10);
    }

    @Test(expectedExceptions = QueryExecutionException.class)
    public void Should_Fail_When_ThereIsNotEnoughIdentifiersReturned() throws Exception {
        tryExecuteInsertQuery(10,6);
    }

    void tryExecuteInsertQuery(int nObjects, int nIds)
        throws Exception {

        List<IObject> lst = new LinkedList<IObject>();

        for (int i = 0; i < nObjects; i++) {
            lst.add(new SMObject());
        }

        List<Long> ids = new LinkedList<Long>();

        for (int i = 0; i < nIds; i++) {
            ids.add((long)i);
        }

        Iterator<Long> idIter = ids.iterator();

        UpsertQueryMessage message = mock(UpsertQueryMessage.class);
        when(message.getDocuments()).thenReturn(lst);

        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.next()).then(invocationOnMock -> idIter.hasNext());
        when(resultSet.getLong("id")).then(invocationOnMock -> idIter.next());

        PreparedStatement statement = mock(PreparedStatement.class);

        when(statement.executeQuery()).thenReturn(resultSet);

        new QueryExecutorImpl().executeInsertionQuery(statement,message);

        for (int i = 0; i < lst.size(); i++) {
            assertEquals((Long) (lst.get(i).getValue(new FieldName("id"))), ids.get(i));
        }
    }
}