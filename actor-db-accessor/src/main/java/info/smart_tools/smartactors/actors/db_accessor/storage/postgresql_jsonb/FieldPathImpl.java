package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.storage.FieldPath;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;

class FieldPathImpl implements FieldPath {
    private String path;

    private FieldPathImpl(String[] parts) {
        this.path = String.format("%s#>\'{%s}\'",
                Schema.DOCUMENT_COLUMN_NAME,
                String.join(",",parts));
    }

    private FieldPathImpl(String column,String castFunction) {
        this.path = String.format("%s(%s)",castFunction,column);
    }

    public String getSQLRepresentation() {
        return this.path;
    }

    public static FieldPathImpl fromString(String path)
            throws QueryBuildException {
        if(!FieldPath.isValid(path)) {
            throw new QueryBuildException("Invalid field path: "+path);
        }

        if (path.equals("id")) {
            return new FieldPathImpl(Schema.ID_COLUMN_NAME,Schema.ID_TO_JSONB_CAST_FUNCTION);
        }

        return new FieldPathImpl(FieldPath.splitParts(path));
    }
}
