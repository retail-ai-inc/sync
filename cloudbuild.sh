#!/usr/bin/env bash

# Usage: cloudbuild.sh -w <webhook_url> -c <channel> -u <username> -m <message> [-a <alert_type>] [-b <branch_name>] [-s <storage_name>] [-k <k8s_cluster>] [-p <pod>] [-C <container>] [-i <is_build>]

# Exit immediately if a command exits with a non-zero status
set -e

# Error if variable referenced before being set
set -u

# Produce failure return code if any command fails in pipe
set -o pipefail

# Accepted values: good, warning, danger
alert_type=""
channel=""
message=""
username=""
webhook_url=""
branch_name=""
storage_name=""
k8s_cluster=""
pod=""
container=""
is_build=""
trigger=""
commit_url=""
trivy_report=""

# Colon after var means it has a value rather than it being a bool flag
while getopts 'a:b:c:f:m:s:T:u:w:k:p:C:i:t:U:' OPTION; do
    case "$OPTION" in
        a)
            alert_type="$OPTARG"
            ;;
        b)
            branch_name="$OPTARG"
            ;;
        c)
            channel="$OPTARG"
            ;;
        m)
            message="$OPTARG"
            ;;
        s)
            storage_name="$OPTARG"
            ;;
        T)
            trivy_report="$OPTARG"
            ;;
        u)
            username="$OPTARG"
            ;;
        w)
            webhook_url="$OPTARG"
            ;;
        k)
            k8s_cluster="$OPTARG"
            ;;
        p)
            pod="$OPTARG"
            ;;
        C)
            container="$OPTARG"
            ;;
        i)
            is_build="$OPTARG"
            ;;
        t)
            trigger="$OPTARG"
            ;;
        U)
            commit_url="$OPTARG"
            ;;
        ?)
            echo "script usage: $(basename $0) {-c channel} {-m message} {-u username} {-w webhook} [-a alert_type] [-b branch_name] [-s storage_name] [-T trivy_report] [-k <k8s_cluster>] [-p <pod>] [-C <container>] [-i <is_build>] [-t <trigger>] [-U <commit_url>]" >&2
            exit 1
            ;;
    esac
done

shift "$(($OPTIND -1))"

# Exit if channel not provided
if [[ -z "$channel" ]]
then
    echo "No channel specified"
    exit 1
fi

# Read piped data as message if message argument is not provided
if [[ -z "$message" ]]
then
    message=$*

    while IFS= read -r line; do
        message="$message$line\n"
    done
fi

# Exit if username not provided
if [[ -z "$username" ]]
then
    echo "No username specified"
    exit 1
fi

# Exit if webhook not provided
if [[ -z "$webhook_url" ]]
then
    echo "No webhook_url specified"
    exit 1
fi

# Escape message text and preserve newlines
escapedText=$(printf "%s" "$message" | sed 's/"/\"/g' | sed "s/'/\'/g")

# Create fields
fields="{}"

if [[ -n "$trigger" ]]
then
    fields+=", {\"title\": \"*Trigger*\", \"value\": \"$trigger\", \"short\": true}"
fi

if [[ -n "$branch_name" ]]
then
    fields+=", {\"title\": \"*Branch*\", \"value\": \"$branch_name\", \"short\": true}"
fi

if [[ -n "$commit_url" ]]
then
    fields+=", {\"title\": \"*Commit*\", \"value\": \"$commit_url\", \"short\": true}"
fi

# Fill Bucket to fields
if [[ -n "$storage_name" ]]
then
    fields+=", {\"title\": \"*Bucket*\", \"value\": \"$storage_name\", \"short\": true}"
fi

# Fill Cluster to fields
if [[ -n "$k8s_cluster" ]]
then
    fields+=", {\"title\": \"*Cluster*\", \"value\": \"$k8s_cluster\", \"short\": true}"
fi

# Fill Pod to fields
if [[ -n "$pod" ]]
then
    fields+=", {\"title\": \"*gRPC*\", \"value\": \"$pod\", \"short\": true}"
fi

# Fill Container to fields
if [[ -n "$container" ]]
then
    fields+=", {\"title\": \"*Container Tag*\", \"value\": \"$container\", \"short\": true}"
fi

# Fill Trivy reprot to fields
if [[ -n "$trivy_report" ]]
then
    fields+=", {\"title\": \"*SonarQube Report*\", \"value\": \"$trivy_report\", \"short\": true}"
fi

# Create footer
footer=""
if [[ -n "$trivy_report" ]]
then
    footer="Any questions, please contact the *Devops team* :blush:"
fi

# Create JSON payload
json="{\"channel\": \"$channel\", \"username\":\"$username\", \"icon_emoji\":\"hammer_and_wrench\", \"attachments\":[{\"color\":\"$alert_type\", \"text\": \"$escapedText\", \"fields\": [$fields], \"footer\": \"$footer\"}]}"

# Fill Pod to fields
if [[ -n "$is_build" ]]
then
    go mod vendor
    make slim
fi


# Fire off slack message post
curl -s -d "payload=$json" "$webhook_url"
