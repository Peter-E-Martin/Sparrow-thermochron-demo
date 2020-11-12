# Configures environment for TRaIL lab

export SPARROW_PATH="$SPARROW_CONFIG_DIR/Sparrow"
export SPARROW_BACKUP_DIR="$SPARROW_CONFIG_DIR/backups"
export SPARROW_LAB_NAME="TRaIL"

#export SPARROW_SITE_CONTENT="$PROJECT_DIR/site-content"

pipeline="$SPARROW_CONFIG_DIR/import-pipeline"
export SPARROW_COMMANDS="$pipeline/bin"

# Keep volumes for this project separate from those for different labs
export COMPOSE_PROJECT_NAME="${SPARROW_LAB_NAME}"
export SPARROW_DATA_DIR="$SPARROW_CONFIG_DIR/TRaIL-Data"

overrides="${SPARROW_CONFIG_DIR}/sparrow-secrets.sh"
[ -f "$overrides" ] && source "$overrides"
