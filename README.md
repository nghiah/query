# A command line interface wrapper using td-client for java.
A command line that allows the user to issue a query on Treasure Data and query a database and
table to retrieve the values of a specified set of columns in a specified date/time range:

 - required: database name 'db_name'
 - required: table name 'table_name'
 - optional: comma separated list of columns 'col_list' as string (e.g. 'column1,column2,column3’). If not specified, all columns are selected.
 - optional: minimum timestamp 'min_time' in unix timestamp or 'NULL'
 - optional: maximum timestamp 'max_time' in unix timestamp or 'NULL'.
 - Obviously 'max_time' must be greater than 'min_time' or NULL.
 - optional: query engine ‘engine’: 'hive' or 'presto'. Defaults to ‘presto’.
 - optional: output format ‘format’: ‘csv’ or ‘tabular'. Defaults to ‘tabular’.
 - optional: a query limit ‘limit’: ‘100’. If not specified, all records are selected.
 
 and it will run a query like:

```
SELECT <col_list>
FROM <table_name>
WHERE TD_TIME_RANGE(time, <min_time>, <max_time>)
LIMIT <limit>

```

## Usage

To use query tool, you need to set your API key in the following file:

**$HOME/.td/td.conf**

```
[account]
  user = (your TD account e-mail address)
  apikey = (your API key)
```

You can retrieve your API key from [My profile](https://console.treasuredata.com/users/current) page.

It is also possible to use `TD_API_KEY` environment variable. Add the following configuration to your shell configuration `.bash_profile`, `.zprofile`, etc.

```
export TD_API_KEY = (your API key)
```

For Windows, add `TD_API_KEY` environment variable in the user preference panel.

### Proxy Server

If you need to access Web through proxy, add the following configuration to `$HOME/.td/td.conf` file:

```
[account]
  user = (your TD account e-mail address)
  apikey = (your API key)
  td.client.proxy.host = (optional: proxy host name)
  td.client.proxy.port = (optional: proxy port number)
  td.client.proxy.user = (optional: proxy user name)
  td.client.proxy.password = (optional: proxy password)
```

### Example

You could find a jar file and execution file in out folder.

For running the jar file, please run as below:

```
"C:\Program Files\Java\jdk-11.0.1\bin\java.exe" -jar query.jar samsungdb sstable -f tabular -e presto 
-c 'firstname,lastname' -m 1427347140 -M 1427350726 -l 100
```

OR with execution file:
```
query samsungdb sstable -f tabular -e presto -c 'firstname,lastname' -m 1427347140 -M 1427350726 -l 100
```

You are able to build the .exe file with your updates.
```
$ git clone https://github.com/nghiah/query.git
$ cd query
$ mvn package
```

Happy Testing !!

