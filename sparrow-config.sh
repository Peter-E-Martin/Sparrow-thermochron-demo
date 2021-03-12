# Configures environment for TRaIL lab

export SPARROW_BACKUP_DIR="$SPARROW_CONFIG_DIR/backups"
export SPARROW_LAB_NAME="TRaIL"
export SPARROW_VERSION=">=2.0.0.beta1"

#export SPARROW_SITE_CONTENT="$PROJECT_DIR/site-content"

# Keep volumes for this project separate from those for different labs
export COMPOSE_PROJECT_NAME="${SPARROW_LAB_NAME}"
export SPARROW_DATA_DIR="$SPARROW_CONFIG_DIR/TRaIL-Data"

# Plugins
export SPARROW_PLUGIN_DIR="$SPARROW_CONFIG_DIR/plugins"

overrides="${SPARROW_CONFIG_DIR}/sparrow-secrets.sh"
[ -f "$overrides" ] && source "$overrides"
