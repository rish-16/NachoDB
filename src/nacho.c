#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define COLUMN_NAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
	uint32_t id;
	char name[COLUMN_NAME_SIZE];
	char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
	char * buffer;
	size_t buffer_length;
	ssize_t input_length;
} QueryBuffer;

typedef enum { 
	EXECUTE_SUCCESS, 
	EXECUTE_TABLE_FULL 
} ExecuteResult;

typedef enum {
	META_COMMAND_SUCCESS,
	META_UNRECOGNISED_COMMAND,
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNISED_STATEMENT
} PrepareResult;

typedef enum {
	STATEMENT_INSERT, 
	STATEMENT_SELECT
} StatementType;

typedef struct {
	StatementType type;
	Row row_to_insert;
} Statement;

typedef struct {
	uint32_t num_rows;
	void * pages[TABLE_MAX_PAGES];
} Table;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t NAME_SIZE = size_of_attribute(Row, name);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t NAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = NAME_OFFSET + NAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + NAME_SIZE + EMAIL_SIZE;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

void serialise_row(Row * source, void * destination) {
	memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
	memcpy(destination + NAME_OFFSET, &(source->name), NAME_OFFSET);
	memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_OFFSET);
}

void print_row(Row * row) {
	printf("(%d, %s, %s)\n", row->id, row->name, row->email);
}

void deserialise_row(void * source, Row * destination) {
	memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->name), source + NAME_OFFSET, NAME_SIZE);
	memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);	
}

void dump_table(Table * table) {
	for (int i = 0; table->pages[i]; i++) {
		free(table->pages[i]);
	}
	free(table);
}

void close_query_buffer(QueryBuffer * query_buffer) {
	free(query_buffer->buffer);
	free(query_buffer);
}

void * slot_row(Table * table, uint32_t row_num) {
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void * page = table->pages[page_num];
	
	if (page == NULL) {
		page = table->pages[page_num] = malloc(PAGE_SIZE);
	}
	
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	
	return page + byte_offset;
}

QueryBuffer * new_query_buffer() {
	QueryBuffer * query_buffer = malloc(sizeof(QueryBuffer)); // allocate memory to query
	query_buffer->buffer = NULL;
	query_buffer->buffer_length = 0;
	query_buffer->input_length = 0;
	
	return query_buffer;
}

MetaCommandResult do_meta_command(QueryBuffer * query_buffer, Table * table) {
	if (strcmp(query_buffer->buffer, ".exit") == 0) { // user wants to exit
		close_query_buffer(query_buffer);
		dump_table(table);
		exit(EXIT_SUCCESS);
	} else {
		return META_UNRECOGNISED_COMMAND;
	}
}

PrepareResult prepare_statement(QueryBuffer * query_buffer, Statement * statement) {
	if (strncmp(query_buffer->buffer, "insert", 6) == 0) {
		statement->type = STATEMENT_INSERT;
		int args_assigned = sscanf(
			query_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),
			statement->row_to_insert.name,
			statement->row_to_insert.email
		);
		
		if (args_assigned < 3) {
			return PREPARE_SYNTAX_ERROR;
		}
		
		return PREPARE_SUCCESS;
	}
	
	if (strcmp(query_buffer->buffer, "select") == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	
	return PREPARE_UNRECOGNISED_STATEMENT;
}

ExecuteResult execute_insert(Statement * statement, Table * table) {
	if (table->num_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}
	
	Row * row = &(statement->row_to_insert);
	
	serialise_row(row, slot_row(table, table->num_rows));
	table->num_rows += 1;
	
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement * statement, Table * table) {
	Row row;
	
	for (uint32_t i = 0; i < table->num_rows; i++) {
		deserialise_row(slot_row(table, i), &row);
		print_row(&row);
	}
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement * statement, Table * table) {
	switch (statement->type) {
		case (STATEMENT_INSERT):
			return execute_insert(statement, table);
		case (STATEMENT_SELECT):
			return execute_select(statement, table);
	}
}

Table * new_table() {
	Table * table = malloc(sizeof(Table));
	table->num_rows = 0;
	
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		table->pages[i] = NULL;
	}
	return table;
}

void prompt() {
	printf ("nacho > ");
}

void read_query(QueryBuffer * query_buffer) {
	ssize_t bytes_read = getline(&(query_buffer->buffer), &(query_buffer->buffer_length), stdin);
	
	if (bytes_read <= 0) { // user does not enter valid query
		printf ("Error reading query\n");
		exit(EXIT_FAILURE); // exits program
	}
	
	query_buffer->input_length = bytes_read - 1;
	query_buffer->buffer[bytes_read - 1] = 0;
}

int main(int argc, char * argv[]) {
	Table * table = new_table();
	QueryBuffer * query_buffer = new_query_buffer();

	while (true) {
		prompt();
		read_query(query_buffer);
		
		if (query_buffer->buffer[0] == '.') {
			switch (do_meta_command(query_buffer, table)) {
				case (META_COMMAND_SUCCESS):
					continue;
				case (META_UNRECOGNISED_COMMAND):
					printf ("Unrecognised command: '%s'\n", query_buffer->buffer);
					continue;
			}
		}
		
		Statement statement;
		switch (prepare_statement(query_buffer, &statement)) {
			case (PREPARE_SUCCESS):
				break;
			case (PREPARE_SYNTAX_ERROR):
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case (PREPARE_UNRECOGNISED_STATEMENT):
				printf ("Unrecognised keyword at the start of '%s'\n", query_buffer->buffer);
				continue;
		}
		
		switch (execute_statement(&statement, table)) {
			case (EXECUTE_SUCCESS):
				printf ("Executed.\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf ("Error: Table full.\n");
				break;
		}
	}
}