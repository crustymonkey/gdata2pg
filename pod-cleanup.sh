#!/bin/bash


SQL_FILE="$(dirname $0)/pod-cleanup.sql"
DB="grafana"


function get_in_query() {
    local tmp="$1"
    echo "$(tr '\n' ',' < "$tmp" | sed -e 's#,$##')"
}

function del_tsd() {
    local tmp="$1"
    local in_q="$(get_in_query "$tmp")"
    local query="DELETE FROM tsd WHERE key_id in ($in_q);"

    psql "$DB" <<< "$query"
}


function del_keys() {
    local tmp="$1"
    local in_q="$(get_in_query "$tmp")"
    local query="DELETE FROM keys WHERE id in ($in_q);"

    psql "$DB" <<< "$query"
}


function dump_keys() {
    local tmp="$1"
    psql -At "$DB" < "$SQL_FILE" > "$tmp"
}

function main() {
    if [[ $1 == '-h' || $1 == '--help' ]] ; then
        echo "$0"
        echo
        echo "  This will clean up old pod metrics"
    fi
    local wd=$(dirname $0)

    local tmp=$(mktemp)
    echo "tmpfile: $tmp"
    # Get the list of keys to del
    dump_keys "$tmp"

    local num_ids="$(wc -l $tmp | awk '{print $1}')"
    # If there isn't anything to do, exit
    if [[ $num_ids -eq 0 ]] ; then
        echo "No ids found, exiting"
        rm "$tmp"
        exit 0
    fi

    echo "Found $num_ids IDs to cleanup"

    # Now we delete the tsd items
    del_tsd "$tmp"

    # And finally, we delete the keys themselves
    del_keys "$tmp"

    rm "$tmp"
    exit 0
}

main "$@"
