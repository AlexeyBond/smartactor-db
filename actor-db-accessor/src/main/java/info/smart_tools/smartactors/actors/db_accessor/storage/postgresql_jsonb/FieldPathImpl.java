package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.storage.FieldPath;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;

class FieldPathImpl implements FieldPath {
    private String path;

    public FieldPathImpl(String[] parts) {
        this.path = String.format("%s#>\'{%s}\'",
                Schema.DOCUMENT_COLUMN_NAME,
                String.join(",",parts));
    }

    public String getSQLRepresentation() {
        return this.path;
    }

    public static FieldPathImpl fromString(String path)
            throws QueryBuildException {
        if(!FieldPath.isValid(path)) {
            throw new QueryBuildException("Invalid field path: "+path);
        }

        return new FieldPathImpl(FieldPath.splitParts(path));
    }
}
