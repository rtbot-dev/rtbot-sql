#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="${HOME}/.rtbot-sql/bin"
BASE_URL="https://rtbot.dev/sql"

# Detect OS
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
  linux)  OS="linux" ;;
  darwin) OS="darwin" ;;
  *)      echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64)         ARCH="x86_64" ;;
  arm64|aarch64)  ARCH="aarch64" ;;
  *)              echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

# Determine version
if [ -z "${RTBOT_SQL_VERSION:-}" ]; then
  VERSION="$(curl -fsSL "${BASE_URL}/versions.json" | grep -o '"latest":"[^"]*"' | cut -d'"' -f4)"
  if [ -z "$VERSION" ]; then
    echo "Failed to determine latest version." >&2
    exit 1
  fi
else
  VERSION="$RTBOT_SQL_VERSION"
fi

TARBALL="rtbot-sql-${OS}-${ARCH}.tar.gz"
URL="${BASE_URL}/v${VERSION}/${TARBALL}"
CHECKSUM_URL="${URL}.sha256"

echo "Installing rtbot-sql v${VERSION} (${OS}/${ARCH})..."

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# Download tarball and checksum
curl -fsSL -o "${TMPDIR}/${TARBALL}" "$URL"
curl -fsSL -o "${TMPDIR}/${TARBALL}.sha256" "$CHECKSUM_URL"

# Verify checksum
cd "$TMPDIR"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum -c "${TARBALL}.sha256"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 -c "${TARBALL}.sha256"
else
  echo "Warning: no sha256 tool found, skipping checksum verification." >&2
fi

# Install
mkdir -p "$INSTALL_DIR"
tar -xzf "$TARBALL" -C "$INSTALL_DIR"

# Update PATH in shell rc
add_to_path() {
  local rc_file="$1"
  local line="export PATH=\"${INSTALL_DIR}:\$PATH\""
  if [ -f "$rc_file" ] && ! grep -qF "$INSTALL_DIR" "$rc_file"; then
    echo "$line" >> "$rc_file"
    echo "  Added to $rc_file"
  fi
}

if [ -f "${HOME}/.zshrc" ]; then
  add_to_path "${HOME}/.zshrc"
elif [ -f "${HOME}/.bashrc" ]; then
  add_to_path "${HOME}/.bashrc"
fi

if [ -d "${HOME}/.config/fish" ]; then
  FISH_LINE="set -gx PATH ${INSTALL_DIR} \$PATH"
  FISH_RC="${HOME}/.config/fish/config.fish"
  if [ -f "$FISH_RC" ] && ! grep -qF "$INSTALL_DIR" "$FISH_RC"; then
    echo "$FISH_LINE" >> "$FISH_RC"
    echo "  Added to $FISH_RC"
  fi
fi

echo "rtbot-sql v${VERSION} installed to ${INSTALL_DIR}"
echo "Restart your shell or run: export PATH=\"${INSTALL_DIR}:\$PATH\""
