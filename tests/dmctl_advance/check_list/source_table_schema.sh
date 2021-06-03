#!/bin/bash

function source_table_schema_empty_arg() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"source-table-schema" \
		"Available Commands" 1
}

function source_table_schema_lack_arguments() {
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"source-table-schema test" \
		"Available Commands" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"source-table-schema update test" \
		"dmctl source-table-schema update <task-name> <table-filter1> <table-filter2> ... <schema-file> \[flags\]" 1
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"source-table-schema delete test" \
		"delete <task-name> <table-filter1> <table-filter2> ... \[flags\]" 1
}
