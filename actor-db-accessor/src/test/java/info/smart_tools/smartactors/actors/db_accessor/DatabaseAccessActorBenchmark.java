package info.smart_tools.smartactors.actors.db_accessor;

import info.smart_tools.smartactors.actors.db_accessor.messages.ConnectMessage;
import info.smart_tools.smartactors.actors.db_accessor.messages.UpsertQueryMessage;
import info.smart_tools.smartactors.core.FieldName;
import info.smart_tools.smartactors.core.IObject;
import info.smart_tools.smartactors.core.impl.SMObject;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Measurement(iterations = 10)
@Fork(1)
@Threads(1)
public class DatabaseAccessActorBenchmark {
    protected DatabaseAccessActor databaseAccessActor;
    protected List<IObject> _10Documents = new LinkedList<>();
    protected List<IObject> _100Documents = new LinkedList<>();
    protected List<IObject> _1000Documents = new LinkedList<>();
    protected List<IObject> _10000Documents = new LinkedList<>();

    @Setup
    public void setUp() throws Exception {
        databaseAccessActor = new DatabaseAccessActor();

        ConnectMessage connectMessage = mock(ConnectMessage.class);

        when(connectMessage.getDriver()).thenReturn("org.postgresql.Driver");
        when(connectMessage.getUrl()).thenReturn("jdbc:postgresql://localhost:5433/postgres");
        when(connectMessage.getUsername()).thenReturn("test_user");
        when(connectMessage.getPassword()).thenReturn("password");

        databaseAccessActor.connect(connectMessage);

        produceDocuments();
    }

    public void insertDocuments(List<IObject> docs) {
        UpsertQueryMessage upsertQueryMessage = mock(UpsertQueryMessage.class);

        when(upsertQueryMessage.getCollectionName()).thenReturn("testCollection");
        when(upsertQueryMessage.getDocuments()).thenReturn(docs);

        databaseAccessActor.insertDocuments(upsertQueryMessage);
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
        FieldName fieldNameA = new FieldName("fieldA");
        for (int i = 0; i < 10000; i++) {
            IObject obj = new SMObject();

            try {
                obj.setValue(fieldNameA, "someStringValue");
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

    public static void main(String[] args) throws RunnerException, IOException {
        Main.main(new String[]{DatabaseAccessActorBenchmark.class.getSimpleName()});
    }
}
