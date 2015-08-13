package info.smart_tools.smartactors.actors.db_accessor.messages;

public interface SearchQueryMessage {
    String  getCollectionName();
    String  getTargetField();

    /*TODO: Define search query format.*/
}
