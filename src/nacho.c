#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 255
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute) // number of bytes per attribute

typedef struct {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef enum {
	NODE_INTERNAL;
	NODE_LEAF;
} NodeType;

// takes in user queries
typedef struct {
	char * buffer;
	size_t buffer_length;
	ssize_t input_length;
} QueryBuffer;

typedef enum { 
	EXECUTE_SUCCESS, 
	EXECUTE_TABLE_FULL 
} Executable;

typedef enum {
	META_COMMAND_SUCCESS,
	META_UNRECOGNISED_COMMAND,
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_NEGATIVE_ID,
	PREPARE_SYNTAX_ERROR,
	PREPARE_STRING_TOO_LONG,
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
	int file_descriptor;
	uint32_t file_length;
	void * pages[TABLE_MAX_PAGES]; // cache size is the same as maximum page limit
} PageCache;

typedef struct {
	PageCache * page_cache;
	uint32_t n_rows;
} Table;

typedef struct {
	Table * table;
	uint32_t row_num;
	bool end_of_table;
} Cursor;

const uint32_t PAGE_SIZE = 4096;
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

void serialise_row(Row * source, void * destination) {
	memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
	strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
	strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void print_row(Row * row) {
	printf("%d, %s, %s\n", row->id, row->username, row->email);
}

void deserialise_row(void * source, Row * destination) {
	memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);	
}

void close_query_buffer(QueryBuffer * query_buffer) {
	free(query_buffer->buffer);
	free(query_buffer);
}

void * get_page(PageCache * page_cache, uint32_t page_num) {
	if (page_num > TABLE_MAX_PAGES) {
		printf ("Page index out of bounds.\n");
		exit(EXIT_FAILURE); 
	}
	
	if (page_cache->pages[page_num] == NULL) {
		void * page = malloc(PAGE_SIZE); // allocate memory to page
		uint32_t n_pages = page_cache->file_length / PAGE_SIZE;
		
		if (page_cache->file_length % PAGE_SIZE) {
			n_pages++;
		}
		
		if (page_num <= n_pages) {
			lseek(page_cache->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(page_cache->file_descriptor, page, PAGE_SIZE);
			
			if (bytes_read == -1) {
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}			
		}
		
		page_cache->pages[page_num] = page;
	}
	
	return page_cache->pages[page_num];
}

void * cursor_value(Cursor * cursor) {
	uint32_t row_num = cursor->row_num;
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	
	void * page = get_page(cursor->table->page_cache, page_num);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	
	return page + byte_offset;
}

void * advance_cursor(Cursor * cursor) {
	cursor->row_num += 1;
	
	if (cursor->row_num >= cursor->table->n_rows) {
		cursor->end_of_table = true;
	}
}

void flush_cache(PageCache * page_cache, uint32_t page_num, uint32_t size) {
	if (page_cache->pages[page_num] == NULL) {
		printf ("Failed to flush page.\n");
		exit(EXIT_FAILURE);
	}
	
	off_t offset = lseek(page_cache->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	
	if (offset == -1) {
		printf ("Error seeking %d.\n", errno);
		exit(EXIT_FAILURE);
	}
	
	ssize_t bytes_written = write(page_cache->file_descriptor, page_cache->pages[page_num], size);
	
	if (bytes_written == -1) {
		printf ("Error writing %d to memory.\n", errno);
		exit(EXIT_FAILURE);
	}
}

void close_conn(Table * table) {
	PageCache * page_cache = table->page_cache;
	
	uint32_t n_full_pages = table->n_rows / ROWS_PER_PAGE;
	
	for (uint32_t i = 0; i < n_full_pages; i++) {
		if (page_cache->pages[i] != NULL) {
			continue;
		}
		
		flush_cache(page_cache, i, PAGE_SIZE);
		page_cache->pages[i] = NULL;
	}
	
	uint32_t n_extra_rows = table->n_rows % ROWS_PER_PAGE;
	
	if (n_extra_rows > 0) {
		uint32_t page_num = n_full_pages;
		if (page_cache->pages[page_num] != NULL) {
			flush_cache(page_cache, page_num, n_extra_rows * ROW_SIZE);
			free(page_cache->pages[page_num]);
			page_cache->pages[page_num] = NULL;
		}
	}
	
	int result = close(page_cache->file_descriptor);
	if (result == -1) {
		printf ("Cannot close database connection.\n");
		exit(EXIT_FAILURE);
	}
	
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		void * page = page_cache->pages[i];
		if (page) {
			free(page);
			page_cache->pages[i] = NULL;
		}
	}
	
	// dump memory allocated for cache and table
	free(page_cache);
	free(table);
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
		close_conn(table);
		exit(EXIT_SUCCESS);
	} else {
		return META_UNRECOGNISED_COMMAND;
	}
}

PrepareResult prepare_insert(QueryBuffer * query_buffer, Statement * statement) {
	statement->type = STATEMENT_INSERT;
	
	// breaking query into tokens
	char * keyword = strtok(query_buffer->buffer, " ");
	char * id_string = strtok(NULL, " ");
	char * username = strtok(NULL, " ");
	char * email = strtok(NULL, " ");
	
	if (id_string == NULL || username == NULL || email == NULL) {
		return PREPARE_SYNTAX_ERROR;
	}
	
	// casting string to integer
	int id = atoi(id_string);
	
	if (id < 0) {
		return PREPARE_NEGATIVE_ID;
	}
	
	if (strlen(username) > COLUMN_USERNAME_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	
	if (strlen(email) > COLUMN_EMAIL_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	
	statement->row_to_insert.id = id;
	strcpy(statement->row_to_insert.username, username);
	strcpy(statement->row_to_insert.email, email);
	
	return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(QueryBuffer * query_buffer, Statement * statement) {
	if (strncmp(query_buffer->buffer, "insert", 6) == 0) {
		return prepare_insert(query_buffer, statement);
	}
	
	if (strcmp(query_buffer->buffer, "select") == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}
	
	return PREPARE_UNRECOGNISED_STATEMENT;
}

Executable execute_insert(Statement * statement, Table * table) {
	if (table->n_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}
	
	Row * row = &(statement->row_to_insert);
	Cursor * cursor = end_table(table);
	
	serialise_row(row, cursor_value(cursor));
	table->n_rows += 1;
	
	free(cursor);
	
	return EXECUTE_SUCCESS;
}

Executable execute_select(Statement * statement, Table * table) {
	Cursor * cursor = table_start(table);
	Row row;
	int count = 0;
	
	while (!(cursor->end_of_table)) {
		deserialise_row(cursor_value(cursor), &row);
		print_row(&row);
		advance_cursor(cursor);
		count++;
	}
	
	free(cursor);
	
	printf ("\n\nFound %d records\n", count);
	return EXECUTE_SUCCESS;
}

Executable execute_statement(Statement * statement, Table * table) {
	switch (statement->type) {
		case (STATEMENT_INSERT):
			return execute_insert(statement, table);
		case (STATEMENT_SELECT):
			return execute_select(statement, table);
	}
}

// stores data on disk
PageCache * open_page_cache(const char * filename) {
	int fd = open(filename, O_RDWR | O_CREAT | S_IWUSR | S_IRUSR);
	
	if (fd == -1) {
		printf ("Unable to open file.\n");
		exit(EXIT_FAILURE);
	}
	
	off_t file_length = lseek(fd, 0, SEEK_END); // getting byte offset for current file
	
	PageCache * page_cache = malloc(sizeof(PageCache));
	
	page_cache->file_descriptor = fd;
	page_cache->file_length = file_length;
	
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		page_cache->pages[i] = NULL;
	}
	
	return page_cache;
}

Table * open_db(const char * filename) {
	PageCache * page_cache = open_page_cache(filename);
	uint32_t n_rows = page_cache->file_length / ROW_SIZE;
	
	Table * table = malloc(sizeof(Table));
	table->page_cache = page_cache;
	table->n_rows = n_rows;
	
	return table;
}

Cursor * start_table(Table * table) {
	Cursor * cursor = malloc(sizeof(Cursor));
	cursor->table = table;
	cursor->row_num = 0;
	cursor->end_of_table = (table->n_rows == 0);
	
	return cursor;
}

Cursor * end_table(Table * table) {
	Cursor * cursor = malloc(sizeof(Cursor));
	cursor->table = table;
	cursor->row_num = table->n_rows;
	cursor->end_of_table = true;
	
	return cursor;
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
	if (argc < 2) {
		printf ("Must supply a database filename to access.\n");
		exit(EXIT_FAILURE);
	}
	
	char * filename = argv[1];
	Table * table = open_db(filename);
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
			case (PREPARE_NEGATIVE_ID):
				printf ("ID must be positive.\n");
				continue;
			case (PREPARE_SYNTAX_ERROR):
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case (PREPARE_STRING_TOO_LONG):
				printf ("String is too long.\n");
				continue;
			case (PREPARE_UNRECOGNISED_STATEMENT):
				printf ("Unrecognised keyword at the start of '%s'.\n", query_buffer->buffer);
				continue;
		}
		
		switch (execute_statement(&statement, table)) {
			case (EXECUTE_SUCCESS):
				// printf ("Executed.\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf ("Error: Table full.\n");
				break;
		}
	}
}