BINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export PROC_DECAM_DIR=$(dirname "$BINDIR")
export PATH=$PROC_DECAM_DIR/bin:$PATH
