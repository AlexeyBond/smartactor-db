package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.storage.*;
import info.smart_tools.smartactors.actors.db_accessor.storage.common.ConditionsResolverBase;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

class Operators {
    private Operators(){}

    private static void writeFieldCheckCondition(String format,QueryStatement query,
                                                 QueryConditionWriterResolver resolver,
                                                 FieldPath contextFieldPath,Object queryParameter)
            throws QueryBuildException {

        if(contextFieldPath == null) {
            throw new QueryBuildException("Field check conditions not allowed outside of field context.");
        }

        try {
            query.getBodyWriter().write(String.format(format, contextFieldPath.getSQLRepresentation()));

            query.pushParameterSetter((statement, index) -> {
                statement.setObject(index++,queryParameter);
                return index;
            });
        } catch (IOException e) {
            throw new QueryBuildException("Query search conditions write failed because of exception.",e);
        }
    }

    private static void writeFieldInArrayCheckCondition(QueryStatement query, QueryConditionWriterResolver resolver,
                                                        FieldPath contextFieldPath, Object queryParameter)
        throws QueryBuildException {

        if(contextFieldPath == null) {
            throw new QueryBuildException("Operator \"$in\" not allowed outside of field context.");
        }

        if(!List.class.isAssignableFrom(queryParameter.getClass())) {
            throw new QueryBuildException("Parameter of \"$in\" operator should be an JSON array.");
        }

        List<Object> paramAsList = (List<Object>)queryParameter;
        Writer writer = query.getBodyWriter();

        try {
            writer.write(String.format("((%s)in(",contextFieldPath.getSQLRepresentation()));

            for (int i = paramAsList.size(); i > 0; --i) {
                writer.write(String.format("to_json(?)::jsonb%s",(i==1)?"":","));
            }

            writer.write("))");

            query.pushParameterSetter((statement, index) -> {
                for(Object obj : paramAsList) {
                    statement.setObject(index++,obj);
                }
                return index;
            });
        } catch (IOException e) {
            throw new QueryBuildException("Query search conditions write failed because of exception.",e);
        }
    }

    private static QueryConditionWriter formattedCheckWriter(String format) {
        return (query, resolver, contextFieldPath, queryParameter) ->
            writeFieldCheckCondition(format,query,resolver,contextFieldPath,queryParameter);
    }

    public static void addAll(ConditionsResolverBase resolver) {
        // Basic field comparison operators
        resolver.addOperator("$eq", formattedCheckWriter("((%s)=to_json(?)::jsonb)"));
        resolver.addOperator("$ne", formattedCheckWriter("((%s)!=to_json(?)::jsonb)"));
        resolver.addOperator("$lt", formattedCheckWriter("((%s)<to_json(?)::jsonb)"));
        resolver.addOperator("$gt", formattedCheckWriter("((%s)>to_json(?)::jsonb)"));
        resolver.addOperator("$lte", formattedCheckWriter("((%s)<=to_json(?)::jsonb)"));
        resolver.addOperator("$gte", formattedCheckWriter("((%s)>=to_json(?)::jsonb)"));

        // ISO 8601 date/time operators
        /*TODO: Find a way to build an index on date/time field.*/
        resolver.addOperator("$date-from", formattedCheckWriter("((%s)::text::timestamp>=(?)::timestamp)"));
        resolver.addOperator("$date-to", formattedCheckWriter("((%s)::text::timestamp<=(?)::timestamp)"));

        // Value in list check
        resolver.addOperator("$in",Operators::writeFieldInArrayCheckCondition);

        // Tags operators
        resolver.addOperator("$hasTag", formattedCheckWriter("((%s)??(?))"));
    }
}
