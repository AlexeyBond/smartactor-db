package info.smart_tools.smartactors.actors.db_accessor.storage.postgresql_jsonb;

import info.smart_tools.smartactors.actors.db_accessor.storage.QueryBuildException;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class FieldPathImplTest {
    @Test
    public void Should_CreateCorrectFieldPath_When_CorrectFieldNameGiven()
        throws Exception {
        assertEquals(FieldPathImpl.fromString("fieldA").getSQLRepresentation(),
                String.format("%s#>'{fieldA}'",Schema.DOCUMENT_COLUMN_NAME));

        assertEquals(FieldPathImpl.fromString("fieldA.fieldB.fieldC").getSQLRepresentation(),
                String.format("%s#>'{fieldA,fieldB,fieldC}'",Schema.DOCUMENT_COLUMN_NAME));

        assertEquals(FieldPathImpl.fromString("fieldA.fieldB[42].fieldD").getSQLRepresentation(),
                String.format("%s#>'{fieldA,fieldB,42,fieldD}'",Schema.DOCUMENT_COLUMN_NAME));
    }

    @Test(expectedExceptions = QueryBuildException.class)
    public void Should_ThrowException_When_NotAllowedCharacterUsed()
            throws Exception {
        FieldPathImpl.fromString("fieldЫ");
    }

    @Test(expectedExceptions = QueryBuildException.class)
    public void Should_ThrowException_When_PathStartsWithArrayIndex()
            throws Exception {
        FieldPathImpl.fromString("[13]");
    }

    @Test(expectedExceptions = QueryBuildException.class)
    public void Should_ThrowException_When_PathStartsWithSubDocumentFieldSelector()
            throws Exception {
        FieldPathImpl.fromString(".fieldG");
    }

    @Test(expectedExceptions = QueryBuildException.class)
    public void Should_ThrowException_When_EmptyPathGiven()
            throws Exception {
        FieldPathImpl.fromString("");
    }
}
