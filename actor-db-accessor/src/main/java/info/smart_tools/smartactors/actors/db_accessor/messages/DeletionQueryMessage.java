package info.smart_tools.smartactors.actors.db_accessor.messages;

public interface DeletionQueryMessage {
    String  getCollectionName();

    /*TODO: Use document ID or use query like in search request?*/
}
