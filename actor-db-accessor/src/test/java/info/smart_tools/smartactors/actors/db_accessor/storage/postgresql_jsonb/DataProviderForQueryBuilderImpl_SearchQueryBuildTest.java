package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.storage.CollectionName;
import org.testng.annotations.DataProvider;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataProviderForQueryBuilderImpl_SearchQueryBuildTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    private static List<Object[]> validQueries = new LinkedList<>();
    private static List<Object[]> invalidQueries = new LinkedList<>();

    private static SearchQueryMessage makeSearchQuery(String collection, String queryJSON, Integer pageSize, Integer pageNumber)
            throws Exception {
        SearchQueryMessage msg = mock(SearchQueryMessage.class);
        Map<String,Object> queryObject = objectMapper.reader(new TypeReference<Map<String, Object>>() {}).readValue(queryJSON);

        when(msg.getCollectionName()).thenReturn(collection);
        when(msg.getPageNumber()).thenReturn(pageNumber);
        when(msg.getPageSize()).thenReturn(pageSize);
        when(msg.getQuery()).thenReturn(queryObject);

        return msg;
    }

    private static void addValidQuery(SearchQueryMessage msg, String expectedSQL, Object[] expectedParams) {
        validQueries.add(new Object[] {msg,expectedSQL,expectedParams});
    }

    private static void addInvalidQuery(SearchQueryMessage msg) {
        invalidQueries.add(new Object[] {msg});
    }

    @DataProvider(name = "valid-queries-provider")
    public static Iterator<Object[]> validQueriesProvider() {
        return validQueries.iterator();
    }

    @DataProvider(name = "invalid-queries-provider")
    public static Iterator<Object[]> invalidQueriesProvider() {
        return invalidQueries.iterator();
    }

    static
    {
        try {
            addValidQuery(makeSearchQuery("test", "{\"asd\":{\"$eq\":42}}", 15, 2),
                    String.format(
                            "SELECT * FROM %s WHERE((((%s)=to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString(),
                            FieldPathImpl.fromString("asd").getSQLRepresentation()),
                    new Object[]{42,15,15});

            addValidQuery(makeSearchQuery("test", "{\"asd\":{\"$lt\":96}}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((((%s)<to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString(),
                            FieldPathImpl.fromString("asd").getSQLRepresentation()),
                    new Object[]{96,10,0});

            addValidQuery(makeSearchQuery("test", "{\"asd\":{\"$gt\":\"lorem ipsum\"}}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((((%s)>to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString(),
                            FieldPathImpl.fromString("asd").getSQLRepresentation()),
                    new Object[]{"lorem ipsum",10,0});

            addValidQuery(makeSearchQuery("test", "{\"asd\":{\"$gte\":18}}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((((%s)>=to_json(?)::jsonb)))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString(),
                            FieldPathImpl.fromString("asd").getSQLRepresentation()),
                    new Object[]{18,10,0});

            addValidQuery(makeSearchQuery("test", "{}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE(true)LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString()),
                    new Object[]{10,0});

            addValidQuery(makeSearchQuery("test", "{\"$and\":[]}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((true))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString()),
                    new Object[]{10,0});

            addValidQuery(makeSearchQuery("test", "{\"body.tags\":{\"$hasTag\":\"someSuperTag\"}}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((((%s)??(?))))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString(),
                            FieldPathImpl.fromString("body.tags").getSQLRepresentation()),
                    new Object[]{"someSuperTag",10,0});

            addValidQuery(makeSearchQuery("test", "{\"$not\":[{\"$and\":[]},{\"$and\":[]}]}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((NOT(((true))AND((true)))))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString()),
                    new Object[]{10,0});

            addValidQuery(makeSearchQuery("test", "{\"$or\":[{\"$and\":[]},{\"$and\":[]}]}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((((true))OR((true))))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString()),
                    new Object[]{10,0});

            addValidQuery(makeSearchQuery("test", "{\"$and\":[{\"$and\":[]},{\"$and\":[]}]}", 10, 1),
                    String.format(
                            "SELECT * FROM %s WHERE((((true))AND((true))))LIMIT(?)OFFSET(?)",
                            CollectionName.fromString("test").toString()),
                    new Object[]{10,0});

            /*Should fail when field operators are used outside of field context.*/
            addInvalidQuery(makeSearchQuery("test","{\"$eq\":100}",10,1));
            addInvalidQuery(makeSearchQuery("test","{\"$lt\":100}",10,1));
            addInvalidQuery(makeSearchQuery("test","{\"$gt\":100}",10,1));
            addInvalidQuery(makeSearchQuery("test","{\"$gte\":100}",10,1));
            addInvalidQuery(makeSearchQuery("test","{\"$lte\":100}",10,1));
            addInvalidQuery(makeSearchQuery("test","{\"$hasTag\":100}",10,1));

            addInvalidQuery(makeSearchQuery("test","{\"$notTheOperator\":[]}",10,1));
            addInvalidQuery(makeSearchQuery("test","{\"inv@lidField\":[]}",10,1));
            addInvalidQuery(makeSearchQuery("inv@lidCollectionN@me","{}",10,1));
        } catch (Exception e) {
            fail("Initialization error",e);
        }
    }
}
