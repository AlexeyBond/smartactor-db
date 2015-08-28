package info.smart_tools.smartactors.actors.db_accessor.messages;

import java.util.Map;

public interface CreateCollectionQueryMessage {
    String  getCollectionName();

    /**
     *  What indexes to create on collection.
     *  @return map filedName->indexType
     *
     *  Index types:
     *  ordered     - for sortable fields (numeric or strings).
     *  tags        - for search by tags (tags field should be an JSON array).
     *
     */
    Map<String,String> getIndexes();
}
