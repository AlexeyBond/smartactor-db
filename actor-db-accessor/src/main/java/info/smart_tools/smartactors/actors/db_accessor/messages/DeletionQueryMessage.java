package info.smart_tools.smartactors.actors.db_accessor.messages;

import java.util.List;

public interface DeletionQueryMessage {
    String  getCollectionName();

    List<Long> getDocumentIds();
}
