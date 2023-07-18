#!/bin/sh
# set -x #echo on

READINESS_PROBE="/is_ready"
CURRENT_TS_PROBE="/current_timestamp"

READINESS_URL="${CHARGEBACK_READINESS_PROBE}${READINESS_PROBE}"
TS_URL="${CHARGEBACK_READINESS_PROBE}${CURRENT_TS_PROBE}"

SCRAPE_URL="${CHARGEBACK_METRICS_URL}"

check_readiness () {
    # This function checks if the readiness probe is True
    # If it is not, it will wait 5 seconds and try again
    test=`wget -O - -q ${READINESS_URL} 2>&1 | cut -d ' ' -f 1 `
    echo "Readiness probe is ${test}"
    while [ ${test} != "True" ]
    do
        echo "Waiting for readiness probe to be True"
        sleep 5
    done
}

check_ts_vicinity () {
    # This function checks if the scrape timestamp is getting close to the current time
    # If it is, it will increase the scrape interval to 10 minutes
    # If it is not, it will set the scrape interval to 0.1 seconds
    TS_VALUE=`wget -O - -q ${TS_URL} 2>&1 | cut -d ' ' -f 1 `
    VICINITY_CUTOFF=$(( `date '+%s'` - $(( 24 * 60 * 60 * 5 )) ))
    if [ ${TS_VALUE} -gt ${VICINITY_CUTOFF} ]
    then
        echo "Scrape timestamp is getting close, Scrape interval increased to 10 minutes."
        return 600
    else
        return 1
    fi
}

# Main loop
# This loop will check if the readiness probe is True
# If it is not, it will wait 5 seconds and try again
# If it is, it will check if the scrape timestamp is getting close to the current time
# If it is, it will increase the scrape interval to 10 minutes
# If it is not, it will set the scrape interval to 0.1 seconds
# It will then scrape the Chargeback API and create a new block
# It will then wait for the scrape interval and repeat
# Don't we just love the Auto generated Comments :) 
while true
do
    check_readiness
    `check_ts_vicinity`
    SCRAPE_INTERVAL=$?
    echo "Scraping Interval set to ${SCRAPE_INTERVAL}"
    rm -f index.html index2.html
    wget -T 60 ${SCRAPE_URL}
    tail +19 index.html > index2.html
    echo "# EOF" >> index2.html
    promtool tsdb create-blocks-from openmetrics index2.html .
    rm -f index.html index2.html
    echo "Sleeping for ${SCRAPE_INTERVAL} seconds"
    sleep ${SCRAPE_INTERVAL}
done

