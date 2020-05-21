#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	char * buffer;
	size_t buffer_length;
	ssize_t input_length;
} QueryBuffer;

QueryBuffer * new_query_buffer() {
	QueryBuffer * query_buffer = malloc(sizeof(QueryBuffer)); // allocate memory to query
	query_buffer->buffer = NULL;
	query_buffer->buffer_length = 0;
	query_buffer->input_length = 0;
	
	return query_buffer;
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
	
	// for debug purposes
	// printf ("%s", query_buffer->buffer);
	
	query_buffer->input_length = bytes_read - 1;
	query_buffer->buffer[bytes_read - 1] = 0;
}

void close_query_buffer(QueryBuffer * query_buffer) {
	free(query_buffer->buffer);
	free(query_buffer);
}

int main() {
	QueryBuffer * query_buffer = new_query_buffer();

	while (true) {
		prompt();
		read_query(query_buffer);
		
		// user wants to quit
		if (strcmp(query_buffer->buffer, ".exit") == 0) {
			close_query_buffer(query_buffer);
			exit(EXIT_SUCCESS);
		} else {
			printf ("Unrecognised command: '%s'. Please try again.\n", query_buffer->buffer);
		}
	}
}