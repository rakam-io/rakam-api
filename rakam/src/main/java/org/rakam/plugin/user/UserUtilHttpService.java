package org.rakam.plugin.user;

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
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFFont;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.rakam.analysis.ApiKeyService;
import org.rakam.analysis.RequestContext;
import org.rakam.collection.FieldType;
import org.rakam.collection.SchemaField;
import org.rakam.report.QueryResult;
import org.rakam.server.http.HttpServer;
import org.rakam.server.http.HttpService;
import org.rakam.server.http.RakamHttpRequest;
import org.rakam.server.http.annotations.ApiParam;
import org.rakam.server.http.annotations.IgnoreApi;
import org.rakam.util.ExportUtil;
import org.rakam.util.JsonHelper;
import org.rakam.util.RakamException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.lang.String.format;
import static org.rakam.analysis.ApiKeyService.AccessKeyType.READ_KEY;

@Path("/user/export")
@IgnoreApi
public class UserUtilHttpService
        extends HttpService {
    private final SqlParser sqlParser = new SqlParser();
    private final AbstractUserService service;
    private final ApiKeyService apiKeyService;

    @Inject
    public UserUtilHttpService(ApiKeyService apiKeyService, AbstractUserService service) {
        this.service = service;
        this.apiKeyService = apiKeyService;
    }

    private static void writeExcelValue(Cell cell, Object field, FieldType type) {
        if (field == null) {
            return;
        }

        switch (type) {
            case STRING:
            case TIMESTAMP:
            case TIME:
                cell.setCellValue(field.toString());
                break;
            case LONG:
            case INTEGER:
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
            case INTEGER:
            case DOUBLE:
                return Cell.CELL_TYPE_NUMERIC;
            case BOOLEAN:
                return Cell.CELL_TYPE_BOOLEAN;
            default:
                return Cell.CELL_TYPE_BLANK;
        }
    }

    @GET
    @Path("/")
    public void export(RakamHttpRequest request) {
        final Map<String, List<String>> params = request.params();
        final List<String> query = params.get("query");
        if (query.isEmpty()) {
            HttpServer.returnError(request, BAD_REQUEST.reasonPhrase(), BAD_REQUEST);
            return;
        }

        final List<String> readKey = params.get("read_key");
        if (readKey == null || readKey.isEmpty()) {
            HttpServer.returnError(request, FORBIDDEN.reasonPhrase(), FORBIDDEN);
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

        String project = apiKeyService.getProjectOfApiKey(readKey.get(0), READ_KEY);

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

        final CompletableFuture<QueryResult> search = service.searchUsers(new RequestContext(project, readKey.get(0)), null, expression,
                read.filterQuery.event_filter, read.filterQuery.sorting, 100000, null);
        final CompletableFuture<byte[]> stream;
        switch (read.exportFormat) {
            case XLS:
                stream = exportAsExcel(search);
                break;
            case CSV:
                stream = search.thenApply(ExportUtil::exportAsCSV);
                break;
            default:
                throw new IllegalStateException();
        }

        stream.whenComplete((result, ex) -> {
            if (ex != null) {
                HttpServer.returnError(request, "Couldn't generate file: " + ex.getMessage(), BAD_REQUEST);
                return;
            }
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
                cell.setCellValue(metadata.get(i).getName());
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

    public enum ExportFormat {
        XLS, CSV;

        @JsonCreator
        public static ExportFormat fromString(String str) {
            return valueOf(str.toUpperCase(Locale.ENGLISH));
        }
    }

    public static class FilterQuery {
        public final String filter;
        public final List<UserStorage.EventFilter> event_filter;
        public final UserStorage.Sorting sorting;

        @JsonCreator
        public FilterQuery(@ApiParam(value = "filter", required = false) String filter,
                           @ApiParam(value = "event_filters", required = false) List<UserStorage.EventFilter> event_filter,
                           @ApiParam(value = "sorting", required = false) UserStorage.Sorting sorting) {
            this.filter = filter;
            this.event_filter = event_filter;
            this.sorting = sorting;
        }
    }

    private static class ExportQuery {
        public final FilterQuery filterQuery;
        public final ExportFormat exportFormat;

        @JsonCreator
        public ExportQuery(@ApiParam("filter") FilterQuery filterQuery,
                           @ApiParam("export_format") ExportFormat exportFormat) {
            this.filterQuery = filterQuery;
            this.exportFormat = exportFormat;
        }
    }
}
