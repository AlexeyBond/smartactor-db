package info.smart_tools.smartactors.actors.db_accessor.storage.common;

import info.smart_tools.smartactors.actors.db_accessor.storage.*;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Conditions {
    private Conditions(){}

    private static void writeCompositeCondition(String prefix,String postfix,String delimiter,
        QueryStatement query,QueryConditionWriterResolver resolver,
        FieldPath contextFieldPath,Object queryParameter)
        throws QueryBuildException {
        Writer writer = query.getBodyWriter();

        try {
            writer.write(prefix);

            if (Map.class.isAssignableFrom(queryParameter.getClass())) {
                Map<Object, Object> paramAsMap = (Map<Object, Object>) queryParameter;

                if (paramAsMap.size() == 0) {
                    throw new QueryBuildException("Error: parameter object should not be empty.");
                }

                Iterator<Map.Entry<Object, Object>> iterator = paramAsMap.entrySet().iterator();
                Map.Entry<Object, Object> entry = iterator.next();

                while (entry != null) {
                    String key = (String) entry.getKey();
                    resolver.resolve(key).write(query, resolver, contextFieldPath, entry.getValue());

                    entry = iterator.next();

                    if (entry != null) {
                        writer.write(delimiter);
                    }
                }
            } else if (List.class.isAssignableFrom(queryParameter.getClass())) {
                List<Object> paramAsList = (List<Object>) queryParameter;

                if (paramAsList.size() == 0) {
                    throw new QueryBuildException("Error: parameter array should not be empty.");
                }

                QueryConditionWriter resolved = resolver.resolve(null);

                Iterator<Object> iterator = paramAsList.iterator();
                Object entry = iterator.next();

                while (entry != null) {
                    resolved.write(query, resolver, contextFieldPath, entry);

                    entry = iterator.next();

                    if (entry != null) {
                        writer.write(delimiter);
                    }
                }
            } else {
                throw new QueryBuildException("Error: composite node value should be an object or an array.");
            }

            writer.write(postfix);
        } catch (IOException e) {
            throw new QueryBuildException("Error while writing a query string.",e);
        }
    }

    public static void writeAndCondition(QueryStatement query,QueryConditionWriterResolver resolver,
                                         FieldPath contextFieldPath,Object queryParameter)
            throws QueryBuildException {
        writeCompositeCondition("(",")","AND",query,resolver,contextFieldPath,queryParameter);
    }

    public static void writeOrCondition(QueryStatement query,QueryConditionWriterResolver resolver,
                                         FieldPath contextFieldPath,Object queryParameter)
            throws QueryBuildException {
        writeCompositeCondition("(",")","OR",query,resolver,contextFieldPath,queryParameter);
    }

    public static void writeNotCondition(QueryStatement query,QueryConditionWriterResolver resolver,
                                         FieldPath contextFieldPath,Object queryParameter)
            throws QueryBuildException {
        writeCompositeCondition("(NOT(","))","AND",query,resolver,contextFieldPath,queryParameter);
    }
}
