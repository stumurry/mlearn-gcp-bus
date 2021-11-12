# This is a starting point for a helper script when looking for vibe schema changes.
# Maybe we could actually search the migrations from a specific point?
# jc 9/4/2020

# Don't forget to setup an SSH tunnel
# ssh -L 3306:worldventures-p1db-data-science-replica.ckt27h4brzc8.us-west-2.rds.amazonaws.com:3306 jcain@gateway.prd.vibeoffice.com

# E.g., ./dump_vibe_columns.sh worldventures pyr_resource_assets

CLIENT=${1:-"worldventures"}
TABLE=${2:-"pyr_resource_assets"}
mysql -A -h 127.0.0.1 -u icentrismaster -p -D pyr-${CLIENT}-prod  -e "show columns from ${TABLE};"
