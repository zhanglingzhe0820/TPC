# All the command below are specific for HBase please change as needed for other clients/databases

CHECK_IF_TABLE_EXISTS="machbase_scripts/select_table.sql"

TRUNCATE_TABLE="machbase_scripts/truncate_table.sql"

CREATE_TABLE="machbase_scripts/create_table.sql"

CHECK_STATS_DB="machadmin -e | grep server"

COUNT_ROWS_IN_TABLE="select count(*) from tag"

SUT_TABLE_PATH="/data/machbase/broker/dbs"

ROW_COUNT="ROWS="

DB_HOST="10.100.230.11"

DB_PORT="5656"

SUT_SHELL="xargs machsql -s $DB_HOST -u SYS -p MANAGER -P $DB_PORT -i -f"

IOT_DATA_TABLE="TAG"

DB_BATCHSIZE=20000

SUT_PARAMETERS="machbase=machbase.host=$DB_HOST,machbase.port=$DB_PORT,machbase.batchsize=$DB_BATCHSIZE,machbase.debug=1"

COORDINATOR_HOME="/data/machbase/coordinator"

SUDO="sudo"
