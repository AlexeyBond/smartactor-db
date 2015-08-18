package info.smart_tools.smartactors.actors.db_accessor.messages;

/**
 * Interface for message containing incoming search request.
 *
 * Query language ("query" field contents) rules:
 * <pre>
 *     query ::= queryGroup
 *
 *     // queryGroup which has no compositionType at left side is interpreted as a "$and" query group.
 *     queryGroup ::= [queryGroup, ...]
 *     queryGroup ::= {queryPair, ...}
 *
 *     // Works only outside a field context.
 *     // Creates a field context with field fieldName for nested queryGroup.
 *     queryPair ::= fieldName : queryGroup
 *
 *     // Works only inside of field context.
 *     queryPair ::= constraintName : constraintParams
 *
 *     // Works everywhere.
 *     queryPair ::= compositionType : queryGroup
 *
 *     constraintName ::= "$eq" | "$gt" | "$lt" | "$tagSet" | "$tagAll" | "$tagAny"
 *
 *     // "$not" actually works as NAND (because right part should always be a queryGroup.
 *     compositionType ::= "$or" | "$and" | "$not"
 * </pre>
 */
public interface SearchQueryMessage {
    String  getCollectionName();
    String  getTargetField();

    Object getQuery();
}
