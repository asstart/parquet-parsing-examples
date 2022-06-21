package dev.asstart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;

import java.io.IOException;
import java.util.ArrayList;


public class ParquetParser {

    private final Path path;

    public ParquetParser(String path) {
        this.path = new Path(path);
    }

    public void printParquetDicts() throws IOException {
        var conf = new Configuration(true);
        var pf = getParquetFooter();
        var schema = pf.getFileMetaData().getSchema();
        var mio = new ColumnIOFactory().getColumnIO(schema);
        PageReadStore rg;
        try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            rg = reader.readNextRowGroup();
            if (rg == null) {
                return;
            }
        }
        var sb = new StringBuilder("Row Group metadata\n");
        for(var col: schema.getColumns()) {
            var pr = rg.getPageReader(col);
            var dp = pr.readDictionaryPage();
            if (dp != null) {
                sb.append("Dict: %s\n".formatted(dp.toString()));
                var dict = dp.getEncoding().initDictionary(col, dp);
                sb.append("Dict enc: %s\n".formatted(dict));
            }

        }
        System.out.println(sb);
    }

    public void printParquetData() throws IOException {
        printParquetData(20);
        System.out.println("...\n");
    }

    public void printParquetData(int rows) throws IOException {
        var sb = new StringBuilder("Parquet data:\n");
        var conf = new Configuration(true);
        var pf = getParquetFooter();
        var schema = pf.getFileMetaData().getSchema();
        var mio = new ColumnIOFactory().getColumnIO(schema);
        try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            var currRG = reader.readNextRowGroup();
            do {
                var rc = currRG.getRowCount();
                var recordReader = mio.getRecordReader(currRG, new GroupRecordConverter(schema));
                for (int i = 0; i < rc && i < rows; i++) {
                    var group = recordReader.read();
                    sb.append("%s\n".formatted(group.toString()));
                }
                rows -= rc;
                currRG = reader.readNextRowGroup();
            } while (currRG != null);
        }
        System.out.println(sb.toString());
    }

    private ParquetMetadata getParquetFooter() throws IOException {
        Configuration conf = new Configuration(true);
        ParquetMetadata pm;
        try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            pm = reader.getFooter();
        }
        return pm;
    }

    public void printParquetFooter() throws IOException {
        var pm = getParquetFooter();
        System.out.println(getFormattedFileMetadata(pm));
        System.out.println(getFormattedBlocksMetadata(pm));
    }
    private String getFormattedFileMetadata(ParquetMetadata pm) {
        var sb = new StringBuilder("Parquet File Metadata\n");
        var fm = pm.getFileMetaData();
        sb.append("Created By: %s\n".formatted(fm.getCreatedBy()));
        sb.append("Schema:\n");
        var schema = fm.getSchema();
        schema.writeToStringBuilder(sb, ";");
        sb.append("Key value metadata:\n");
        var kv = fm.getKeyValueMetaData();
        kv.forEach((k,v) -> sb.append("%s -> %s\n".formatted(k, v)));
        return sb.toString();
    }

    private String getFormattedBlocksMetadata(ParquetMetadata pm) {
        var sb = new StringBuilder("Parquet File Blocks Metadata\n");
        var bms = pm.getBlocks();
        for(var bm: bms) {
            sb.append("Row count: %s\n".formatted(bm.getRowCount()));
            sb.append("Compressed size: %s\n".formatted(bm.getCompressedSize()));
            sb.append("Block path: %s\n".formatted(bm.getPath()));
            sb.append("Ordinal: %s\n".formatted(bm.getOrdinal()));
            sb.append("Starting position: %s\n".formatted(bm.getStartingPos()));
            sb.append("Row index offset: %s\n".formatted(bm.getRowIndexOffset()));
            sb.append("Total size: %s\n".formatted(bm.getTotalByteSize()));
            var cols = bm.getColumns();
            for (var col: cols) {
                sb.append("Col: %s - %s\n".formatted(col.getPath(), col.toString()));
            }
        }
        return sb.toString();
    }

    //PargeReadStore actually is RowGroup
    public void printRowGroupsRowsCount() throws IOException {
        Configuration conf = new Configuration(true);
        var rgs = new ArrayList<PageReadStore>();
        try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            var curr = reader.readNextRowGroup();
            do {
                rgs.add(curr);
                curr = reader.readNextRowGroup();
            } while (curr != null);
        }
        for (var rg:rgs) {
            System.out.println("RG row count: " + rg.getRowCount());
        }
    }

}
