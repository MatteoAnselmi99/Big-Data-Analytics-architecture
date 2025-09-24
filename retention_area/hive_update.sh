# Parameters
JDBC_URL="jdbc:hive2://localhost:10000/telematic_dw"
USERNAME=""
PASSWORD=""

# List of tables to update
tables=("claims" "behaviour" "province" "trip_summary")

echo "Starting the update of the Hive tables in database 'telematic_dw'"

for x in "${tables[@]}"; do
    echo "Updating table: $x"

    beeline -u "$JDBC_URL" --silent=true --showHeader=false --outputformat=tsv2 -e "USE telematic_dw; MSCK REPAIR TABLE $x;" 2>/dev/null

    if [ $? -eq 0 ]; then
        echo "Table $x successfully updated."
    else
        echo "Table $x not updated."
    fi
done

echo "Update completed."

