package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.smart_tools.smartactors.actors.db_accessor.messages.CreateCollectionQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.SearchQueryMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.impl.SMObject;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class DataProviderForQueryBuilderImplTest<MType> {
    private List<Object[]> validQueries;
    private List<Object[]> invalidQueries;

    DataProviderForQueryBuilderImplTest() throws Exception {
        validQueries = new LinkedList<>();
        invalidQueries = new LinkedList<>();

        initQueries();
    }

    protected void addValidQuery(MType message, String expectedSQL, Object[] expectedParams) {
        validQueries.add(new Object[] {message, expectedSQL, expectedParams});
    }

    protected void addInvalidQuery(MType message) {
        invalidQueries.add(new Object[] {message});
    }

    public Iterator<Object[]> provideValidQueries() {
        return validQueries.iterator();
    }

    public Iterator<Object[]> provideInvalidQueries() {
        return invalidQueries.iterator();
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    protected static SearchQueryMessage makeSearchQueryMessage(String collection, String queryJSON, Integer pageSize, Integer pageNumber)
            throws Exception {
        SearchQueryMessage msg = mock(SearchQueryMessage.class);
        Map<String,Object> queryObject = objectMapper.reader(new TypeReference<Map<String, Object>>() {}).readValue(queryJSON);

        when(msg.getCollectionName()).thenReturn(collection);
        when(msg.getPageNumber()).thenReturn(pageNumber);
        when(msg.getPageSize()).thenReturn(pageSize);
        when(msg.getQuery()).thenReturn(queryObject);

        return msg;
    }

    protected static CreateCollectionQueryMessage makeCollectionCreationMessage(String collectionName, Map<String,String> indexes) {
        CreateCollectionQueryMessage message = mock(CreateCollectionQueryMessage.class);

        when(message.getCollectionName()).thenReturn(collectionName);
        when(message.getIndexes()).thenReturn(indexes);

        return message;
    }

    protected static UpsertQueryMessage makeUpsertMessage(String collectionName, String[] documentsAsJSON) {
        UpsertQueryMessage message = mock(UpsertQueryMessage.class);

        List<IObject> documentsAsIObjects = new LinkedList<>();

        for(String json : documentsAsJSON) {
            documentsAsIObjects.add(new SMObject(json));
        }

        when(message.getCollectionName()).thenReturn(collectionName);
        when(message.getDocuments()).thenReturn(documentsAsIObjects);

        return message;
    }

    protected abstract void initQueries() throws Exception;
}
