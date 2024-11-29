for group in $(confluent kafka consumer group list --cluster lkc-np1rvz | cut -d '|' -f 2 | tr -d ' '); do
    echo "Deleting consumer group: $group"
    confluent kafka consumer group delete "$group" --cluster lkc-np1rvz
done