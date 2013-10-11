#!/bin/sh

UPLOAD_URL='http://sigmod.kaust.edu.sa/upload.php'

check_upload() {
    TOKEN=$1
    curl -s ${UPLOAD_URL} -b "PHPSESSID=${TOKEN}" | grep "can not upload"
}

upload() {
    TOKEN=$1
    NOTES=$2
    RES=`check_upload ${TOKEN}`
    if [ -n "${RES}" ]; then
        echo ${RES}
        return
    fi

    echo -n "uploading..."
    curl -s -b "PHPSESSID=${TOKEN}" --request POST -F "file=@libcore.so" -F "notes=${NOTES}" -F "Submit=Upload File" ${UPLOAD_URL} > upload.log
    echo "done"
    return
}

polling() {
    TOKEN=$1
    echo -n "waiting for result..."
    while true; do
        RES=`check_upload ${TOKEN}`
        if [ -z "${RES}" ]; then
            notify-send "Your SIGMOD Contest Submission has Finished"
            echo "done"
            break
        fi
        sleep 10
    done
}

CMD=$1
TOKEN=$2

if [ "${CMD}" = "upload" ]; then
    upload "${TOKEN}" "$3"
    polling "${TOKEN}"
elif [ "${CMD}" = "poll" ]; then
    polling "${TOKEN}"
fi
