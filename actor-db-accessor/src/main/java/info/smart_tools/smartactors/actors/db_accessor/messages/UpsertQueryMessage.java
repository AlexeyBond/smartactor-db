package info.smart_tools.smartactors.actors.db_accessor.messages;

import info.smart_tools.smartactors.core.IObject;

import java.util.List;

public interface UpsertQueryMessage {
    String  getCollectionName();

    /**
     * Returns a list of objects to be upserted.
     *
     * @return a list of objects to be upserted.
     *
     * If a document in this list has "id" field and document with this identifier is exist in database the document
     * in database will be updated, else the document will be inserted and id will be generated (if not provided in query).
     * The generated id will be placed into document in response message.
     */
    List<IObject> getDocuments();
}
