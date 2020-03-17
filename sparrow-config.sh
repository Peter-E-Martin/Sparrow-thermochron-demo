# Configures environment for TRaIL lab

export PROJECT_DIR="${0:h}"
export SPARROW_PATH="$PROJECT_DIR/Sparrow"
export SPARROW_BACKUP_DIR="$PROJECT_DIR/backups"
export SPARROW_LAB_NAME="TRaIL"

#export SPARROW_SITE_CONTENT="$PROJECT_DIR/site-content"

pipeline="$PROJECT_DIR/import-pipeline"
export SPARROW_COMMANDS="$pipeline/bin"

# Keep volumes for this project separate from those for different labs
export COMPOSE_PROJECT_NAME="${SPARROW_LAB_NAME}"
export SPARROW_DATA_DIR="$PROJECT_DIR/TRaIL-Data"

overrides="${0:h}/sparrow-secrets.sh"
[ -f "$overrides" ] && source "$overrides"
