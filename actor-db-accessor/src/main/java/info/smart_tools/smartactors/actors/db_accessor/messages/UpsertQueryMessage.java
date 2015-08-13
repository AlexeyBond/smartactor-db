package info.smart_tools.smartactors.actors.db_accessor.messages;

import info.smart_tools.smartactors.core.IObject;

public interface UpsertQueryMessage {
    String  getCollectionName();

    /*TODO: How to use arrays/lists in messages? (Operations with multiple documents m.b. more efficient)*/
    IObject getDocument();
}
