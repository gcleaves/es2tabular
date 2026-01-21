#!/bin/sh
set -e

# Fix ownership of the data directory if running as root
if [ "$(id -u)" = '0' ]; then
    # Ensure the data directory exists and is owned by nodejs
    chown -R nodejs:nodejs /app/data
    
    # Run the command as the nodejs user
    exec gosu nodejs "$@"
fi

# If not root, just run the command directly
exec "$@"
