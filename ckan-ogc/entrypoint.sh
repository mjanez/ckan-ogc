# Function to check if the site is available
site_available() {
  # If $SSL_UNVERIFIED_MODE is set to "True", we add the -k option to disable SSL verification.
  if [ "$SSL_UNVERIFIED_MODE" = "True" ]; then
    curl -k --output /dev/null --silent --head --fail "$CKAN_URL"
  else
    curl --output /dev/null --silent --head --fail "$CKAN_URL"
  fi
}

# URL avalaible or timeout
start_time=$(date +%s)
end_time=$((start_time + TIMEOUT))

until site_available || [ "$(date +%s)" -ge "$end_time" ]; do
  sleep 1
done

if [ "$(date +%s)" -ge "$end_time" ]; then
  echo "Error: The $CKAN_URL site is not available after waiting $TIMEOUT seconds. Put SSL_UNVERIFIED_MODE=True in .env file if you want to disable SSL verification."
  exit 1
fi

pdm run python3 ogc2ckan/ogc2ckan.py

exec "$@"
