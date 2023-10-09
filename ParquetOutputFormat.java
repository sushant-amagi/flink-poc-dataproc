// ParquetSink.java

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.OutputFileConfig;
import org.apache.flink.api.common.serialization.BulkWriter;

public class ParquetSink implements OutputFormat<Row> {

    private transient BulkWriter<Row> writer;
    private String path;

    public ParquetSink(String path) {
        this.path = path;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        ParquetWriterFactory<Row> factory = ParquetWriterFactory.createRowParquetWriterFactory(...); // add necessary parameters
        this.writer = factory.createWriter(new Path(path), new OutputFileConfig(...)); // add necessary parameters
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        writer.write(record);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
