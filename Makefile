
# Created by:         Sam Pibworth
# Created date:       2022-04-19
# Last modified by:   Sam Pibworth
# Last modified date: 2022-05-10

# Description:
#     Simple makefile to run various command in sequnce rather than having to call them individually.
# Parameters:

# Usage:
#     At the command line run make followed by the desired command.
#	  e.g.
#	  make first_run

first_run:
	dbt clean
	dbt deps
	dbt docs generate
	dbt run
	echo "Your project is ready!"

clean:
	dbt clean

compile: clean
	dbt compile --exclude +elementary

deps:
	dbt deps

run: compile
	dbt run --exclude +elementary

run_table:
	dbt compile --select $(table)
	dbt run --select $(table)

run_full: clean
	dbt compile
	dbt deps
	dbt run
	dbt test

test: compile deps
	dbt test

source_test: compile deps
	dbt test --select tag:source

output_tests: compile deps
	dbt test --exclude tag:source
	
docs:
	dbt docs generate
	dbt docs serve
