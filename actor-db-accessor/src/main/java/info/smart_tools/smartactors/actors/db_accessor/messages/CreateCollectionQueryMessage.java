package info.smart_tools.smartactors.actors.db_accessor.messages;

import java.util.Map;

public interface CreateCollectionQueryMessage {
    String  getCollectionName();

    Map<String,String> getIndexes();
}
