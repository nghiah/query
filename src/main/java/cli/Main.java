package cli;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;
import org.apache.commons.cli.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Options options = new Options();

        Option format = new Option("f", "format", true,
                "Optional: output format ‘format’: 'csv’ or ‘tabular'. Defaults to ‘tabular’.");
        format.setRequired(false);
        options.addOption(format);

        Option column = new Option("c", "column", true,
                "Optional: comma separated list of columns 'col_list' as string (e.g." +
                        "'column1,column2,column3’). If not specified, all columns are selected.");
        column.setRequired(false);
        options.addOption(column);

        Option limit = new Option("l", "limit", true,
                "Optional: a query limit ‘limit’: ‘100’. If not specified, all records are selected.\n");
        limit.setRequired(false);
        options.addOption(limit);

        Option min = new Option("m", "min", true,
                "Optional: minimum timestamp 'min_time' in unix timestamp or 'NULL'.");
        min.setRequired(false);
        options.addOption(min);

        Option engine = new Option("e", "engine", true,
                "Optional: query engine ‘engine’: 'hive' or 'presto'. Defaults to ‘presto'.");
        engine.setRequired(false);
        options.addOption(engine);

        Option max = new Option("M", "MAX", true,
                "Optional: maximum timestamp 'max_time' in unix timestamp or 'NULL'.");
        max.setRequired(false);
        options.addOption(max);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            String minimumTimestamp = cmd.getOptionValue("min");
            String maximumTimestamp = cmd.getOptionValue("MAX");
            String outputFormat = cmd.hasOption("format")?cmd.getOptionValue("format"):"tabular";
            String columnName = cmd.hasOption("column")?cmd.getOptionValue("column").replace("'",""):"*";
            String queryEngine = cmd.hasOption("engine")?cmd.getOptionValue("engine"):"presto";
            String limitRecord = cmd.getOptionValue("limit");

            String[] remainingArguments = cmd.getArgs();
            String dbName = remainingArguments[0];
            String tbName = remainingArguments[1];

            String sqlQuery;
            if (limitRecord != null){
                sqlQuery = String.format("SELECT %s FROM %s WHERE TD_TIME_RANGE(time, %s, %s) LIMIT %s",
                        columnName,tbName, minimumTimestamp, maximumTimestamp, limitRecord);
            }else
                sqlQuery = String.format("SELECT %s FROM %s WHERE TD_TIME_RANGE(time, %s, %s)",
                        columnName,tbName, minimumTimestamp, maximumTimestamp);

            runQuery(sqlQuery, dbName, queryEngine, outputFormat);

        } catch (Exception ex) {

            if (ex instanceof ArrayIndexOutOfBoundsException) {
                 System.err.println("FAILED: Missing one or both required database name and table name.");
            } else if (ex instanceof NumberFormatException){
                System.err.println("FAILED: NumberFormatException For input strings.");
            } else {
                System.err.println("FAILED:" +ex.getMessage());
            }

            System.out.println("\nIssue a query on Treasure Data and query a database and " +
                            "table to retrieve the values of a specified set of columns in a specified date/time range.");
            formatter.printHelp("query my_db my_table [-option]", options);

            System.exit(1);
        }
        System.exit(0);
    }

    private static String runQuery(String query, String dbName, String queryEngine, String outputFormat) {

        // Create a new TD client by using configurations in $HOME/.td/td.conf
        TDClient client = TDClient.newClient();
        try {
            // Submit a new Presto query (for Hive, use TDJobRequest.newHiveQuery)
            String jobId = queryEngine.equals("presto")?client.submit(TDJobRequest.newPrestoQuery(dbName, query)):
                    client.submit(TDJobRequest.newHiveQuery(dbName, query));

            // Wait until the query finishes
            ExponentialBackOff backOff = new ExponentialBackOff();
            TDJobSummary job = client.jobStatus(jobId);
            while (!job.getStatus().isFinished()) {
                Thread.sleep(backOff.nextWaitTimeMillis());
                job = client.jobStatus(jobId);
            }

            // Read the detailed job information
            TDJob jobInfo = client.jobInfo(jobId);
            if (!jobInfo.getStdErr().isEmpty()){
                Exception exception = new Exception(jobInfo.getStdErr());
                throw Throwables.propagate(exception);
            }

            TDResultFormat resultFormat = (outputFormat.equals("tabular"))? TDResultFormat.TSV:TDResultFormat.CSV;
            return client.jobResult(jobId, resultFormat, new Function<InputStream, String>()
            {
                @Override
                public String apply(InputStream input)
                {
                    try {
                        String result = CharStreams.toString(new InputStreamReader(input));
                        System.out.println("SQL result:");
                        System.out.println(result);
                        System.out.println("END__");
                        return result;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            });

        }catch (Exception e) {
             throw Throwables.propagate(e);
        }
        finally {
            // Never forget to close the TDClient.
            client.close();
        }
    }
}
