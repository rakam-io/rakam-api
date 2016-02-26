package org.rakam.ui.user;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.analysis.metadata.Metastore;
import org.rakam.plugin.user.AbstractUserService;
import org.rakam.util.IgnorePermissionCheck;
import org.rakam.plugin.user.UserStorage;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.lang.String.format;

@Path("/ui/user")
@IgnoreApi
public class UserUtilHttpService extends HttpService {
    private final SqlParser sqlParser = new SqlParser();
    private final AbstractUserService service;
    private final Metastore metastore;

    @Inject
    public UserUtilHttpService(Metastore metastore, AbstractUserService service) {
        this.service = service;
        this.metastore = metastore;
    }

    public static class FilterQuery {
        public final String project;
        public final String apiKey;
        public final String filter;
        public final List<UserStorage.EventFilter> event_filter;
        public final UserStorage.Sorting sorting;

        @JsonCreator
        public FilterQuery(@ApiParam(name = "project") String project,
                           @ApiParam(name = "api_key") String apiKey,
                           @ApiParam(name = "filter", required = false) String filter,
                           @ApiParam(name = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                           @ApiParam(name = "sorting", required = false) UserStorage.Sorting sorting) {
            this.project = project;
            this.apiKey = apiKey;
            this.filter = filter;
            this.event_filter = event_filter;
            this.sorting = sorting;
        }
    }

    @GET
    @Path("/export")
    @IgnorePermissionCheck
    public void export(RakamHttpRequest request) {
        final Map<String, List<String>> params = request.params();
        final List<String> query = params.get("query");
        if (query.isEmpty()) {
            HttpServer.returnError(request, BAD_REQUEST.reasonPhrase(), BAD_REQUEST);
            return;
        }

        String body = query.get(0);
        final ExportQuery read;
        try {
            read = JsonHelper.readSafe(body, ExportQuery.class);
        } catch (IOException e) {
            HttpServer.returnError(request, "Couldn't parse body: " + e.getMessage(), BAD_REQUEST);
            return;
        }

        if(!metastore.checkPermission(read.filterQuery.project, Metastore.AccessKeyType.READ_KEY, read.filterQuery.apiKey)) {
            HttpServer.returnError(request, UNAUTHORIZED.reasonPhrase(), UNAUTHORIZED);
        }

        Expression expression;
        if (read.filterQuery.filter != null) {
            try {
                synchronized (sqlParser) {
                    expression = sqlParser.createExpression(read.filterQuery.filter);
                }
            } catch (Exception e) {
                throw new RakamException(format("filter expression '%s' couldn't parsed", read.filterQuery.filter),
                        HttpResponseStatus.BAD_REQUEST);
            }
        } else {
            expression = null;
        }

        final CompletableFuture<QueryResult> search = service.filter(read.filterQuery.project, null, expression,
                read.filterQuery.event_filter, read.filterQuery.sorting, 100000, null);
        final CompletableFuture<byte[]> stream;
        switch (read.exportFormat) {
            case XLS:
                stream = exportAsExcel(search);
                break;
            case CSV:
                stream = exportAsCSV(search);
                break;
            default:
                throw new IllegalStateException();
        }

        stream.thenAccept(result -> {
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            HttpHeaders.setContentLength(response, result.length);
            response.headers().set(CONTENT_TYPE, "application/octet-stream");
            response.headers().set(EXPIRES, "0");
            response.headers().set(CONTENT_TRANSFER_ENCODING, "binary");
            response.headers().set("Content-Disposition", "attachment;filename=\"exported_people." + read.exportFormat.name().toLowerCase(Locale.ENGLISH) + "\"");

            request.context().write(response);
            ChannelFuture lastContentFuture = request.context().writeAndFlush(Unpooled.wrappedBuffer(result));

            if (!HttpHeaders.isKeepAlive(request)) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        });

    }

    private static class ExportQuery {
        public final FilterQuery filterQuery;
        public final ExportFormat exportFormat;

        @JsonCreator
        public ExportQuery(@ApiParam(name="filter") FilterQuery filterQuery,
                           @ApiParam(name="export_format") ExportFormat exportFormat) {
            this.filterQuery = filterQuery;
            this.exportFormat = exportFormat;
        }
    }

    public enum ExportFormat {
        XLS, CSV;

        @JsonCreator
        public static ExportFormat fromString(String str) {
            return valueOf(str.toUpperCase(Locale.ENGLISH));
        }
    }


    private CompletableFuture<byte[]> exportAsCSV(CompletableFuture<QueryResult> queryResult) {
        return queryResult.thenApply(result -> {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final CSVPrinter csvPrinter;
            try {
                csvPrinter = new CSVPrinter(new PrintWriter(out), CSVFormat.DEFAULT);
                csvPrinter.printRecord(result.getMetadata().stream().map(SchemaField::getName).collect(Collectors.toList()));
                csvPrinter.printRecords(result.getResult());
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }

            return out.toByteArray();
        });
    }

    private CompletableFuture<byte[]> exportAsExcel(CompletableFuture<QueryResult> queryResult) {
        return queryResult.thenApply(result -> {
            HSSFWorkbook workbook = new HSSFWorkbook();
            HSSFSheet sheet = workbook.createSheet("Users generated by Rakam");

            final List<SchemaField> metadata = result.getMetadata();

            HSSFFont boldFont = workbook.createFont();
            boldFont.setBoldweight(HSSFFont.BOLDWEIGHT_BOLD);

            HSSFCellStyle headerSyle = workbook.createCellStyle();
            headerSyle.setBorderBottom(CellStyle.BORDER_THIN);
            headerSyle.setBottomBorderColor(IndexedColors.BLACK.getIndex());
            headerSyle.setFont(boldFont);

            Row headerRow = sheet.createRow(0);
            headerRow.setRowStyle(headerSyle);

            for (int i = 0; i < metadata.size(); i++) {
                final Cell cell = headerRow.createCell(i);
                cell.setCellType(Cell.CELL_TYPE_STRING);
                cell.setCellValue(metadata.get(i).getDescriptiveName());
            }

            for (int i = 0; i < result.getResult().size(); i++) {
                List<Object> objects = result.getResult().get(i);
                Row row = sheet.createRow(i + 1);
                for (int c = 0; c < metadata.size(); c++) {
                    final FieldType type = metadata.get(c).getType();

                    final int excelType = getExcelType(type);
                    Cell cell = row.createCell(c, excelType);
                    writeExcelValue(cell, objects.get(c), type);
                }
            }

            try {
                final ByteArrayOutputStream output = new ByteArrayOutputStream();
                workbook.write(output);
                output.close();
                return output.toByteArray();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    private static void writeExcelValue(Cell cell, Object field, FieldType type) {
        switch (type) {
            case STRING:
            case TIMESTAMP:
            case TIME:
                cell.setCellValue(field.toString());
                break;
            case LONG:
            case DOUBLE:
                cell.setCellValue(((Number) field).doubleValue());
                break;
            default:
                throw new IllegalArgumentException("field type is not supported");
        }
    }

    private static int getExcelType(FieldType type) {
        switch (type) {
            case STRING:
                return Cell.CELL_TYPE_STRING;
            case LONG:
            case DOUBLE:
                return Cell.CELL_TYPE_NUMERIC;
            case BOOLEAN:
                return Cell.CELL_TYPE_BOOLEAN;
            default:
                return Cell.CELL_TYPE_BLANK;
        }
    }
}
