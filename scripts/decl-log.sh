#!/bin/bash

# Assumes PWD is `leanprover-community.github.io` repo

input_file=mathlib_stats.html
output_file=decl_log.csv

function extract()
{
	# We assume the file format has not changed if the Definitions: and the Theorems: headers are still in the expected place.
	definitions_line='<th>Definitions</th>'
	theorems_line='<th>Theorems</th>'
	if [[ "$(sed -n '58 s/^[ \t]*// p' "$input_file" )" != "$definitions_line" ]]; then
		echo "Could not match input line: expected '$definitions_line', got '$(sed -n '58p' "$input_file" )'"
		return 1;
	fi
	if [[ "$(sed -n '59 s/^[ \t]*// p' "$input_file" )" != "$theorems_line" ]]; then
		echo "Could not match input line: expected '$theorems_line', got '$(sed -n '59p' "$input_file" )'"
		return 1;
	fi

	# We assume the commit date is close enough to the date of the Mathlib commit.
	date="$(git show -s --format="%ci" HEAD)"

	# Get the line and remove all non-numeric characters.
	definitions_value="$(sed -n '63 s/[^0-9]*//g p' "$input_file")"
	theorems_value="$(sed -n '64 s/[^0-9]*//g p' "$input_file")"

	# Print to output file.
	echo "$date,$definitions_value,$theorems_value" >> $output_file

	return 0
}

git checkout master
git pull

echo "Date,Definitions,Theorems" > $output_file

while true; do
	extract || break
	git checkout HEAD~1
done
