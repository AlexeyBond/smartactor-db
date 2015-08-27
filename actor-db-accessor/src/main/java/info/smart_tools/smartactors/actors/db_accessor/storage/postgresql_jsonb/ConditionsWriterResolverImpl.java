package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.storage.FieldPath;
import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import info.smart_tools.smartactors.actors.db_accessor.storage.common.ConditionsResolverBase;

class ConditionsWriterResolverImpl extends ConditionsResolverBase {
    public FieldPath resolveFieldName(String name) throws QueryBuildException {
        return FieldPathImpl.fromString(name);
    }

    {
        Operators.addAll(this);
    }
}
