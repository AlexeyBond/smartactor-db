package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.core.FieldName;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.impl.SMObject;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseAccessActorInsertionBenchmark extends DatabaseAccessActorBenchmarkBase {
    protected List<IObject> _10Documents = new LinkedList<>();
    protected List<IObject> _100Documents = new LinkedList<>();
    protected List<IObject> _1000Documents = new LinkedList<>();
    protected List<IObject> _10000Documents = new LinkedList<>();

    @Setup
    public void setUp() throws Exception {
        super.setUp();
        produceDocuments();
    }

    @Benchmark
    @Measurement(iterations = 100)
    public void insertBy1() {
        LinkedList<IObject> docs = new LinkedList<>();
        docs.add(_10Documents.get(0));
        insertDocuments(docs);
    }

    @Benchmark
    @Measurement(iterations = 100)
    public void insertBy10() {
        insertDocuments(_10Documents);
    }

    @Benchmark
    @Measurement(iterations = 100)
    public void insertBy100() {
        insertDocuments(_100Documents);
    }

    @Benchmark
    @Measurement(iterations = 100)
    public void insertBy1000() {
        insertDocuments(_1000Documents);
    }

    @Benchmark
    @Measurement(iterations = 100)
    public void insertBy10000() {
        insertDocuments(_10000Documents);
    }

    private void produceDocuments() {
        FieldName[] fieldNames = new FieldName[]{new FieldName("fieldA"),new FieldName("fieldB"),new FieldName("fieldC"),new FieldName("fieldD")};
        for (int i = 0; i < 10000; i++) {
            IObject obj = new SMObject();

            try {
                for(FieldName f : fieldNames) {
                    obj.setValue(f, UUID.randomUUID().toString());
                }
            } catch (Exception e) {}

            if(i < 10) {
                _10Documents.add(obj);
            }

            if(i < 100) {
                _100Documents.add(obj);
            }

            if(i < 1000) {
                _1000Documents.add(obj);
            }

            _10000Documents.add(obj);
        }
    }


    public void insertDocuments(List<IObject> docs) {
        UpsertQueryMessage upsertQueryMessage = mock(UpsertQueryMessage.class);

        when(upsertQueryMessage.getCollectionName()).thenReturn("testCollection");
        when(upsertQueryMessage.getDocuments()).thenReturn(docs);

        databaseAccessActor.insertDocuments(upsertQueryMessage);
    }

    public static void main(String[] args) throws RunnerException, IOException {
        runBenchmarks(DatabaseAccessActorInsertionBenchmark.class);
    }
}
