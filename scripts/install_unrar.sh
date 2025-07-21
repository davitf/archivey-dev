#!/bin/sh
set -e

OS="$(uname -s)"

case "$OS" in
    Linux*)
        if ! command -v unrar >/dev/null 2>&1; then
            echo "⏳ Installing 'unrar' on Linux..."
            sudo apt-get update
            sudo apt-get install -y unrar
        else
            echo "✅ 'unrar' is already installed on Linux."
        fi
        ;;
    Darwin*)
        if ! command -v unrar >/dev/null 2>&1; then
            echo "⏳ Installing 'unrar' on macOS (via Homebrew)..."
            brew install rar
        else
            echo "✅ 'unrar' is already installed on macOS."
        fi
        ;;
    MINGW* | MSYS* | CYGWIN*)
        if command -v unrar >/dev/null 2>&1; then
            echo "✅ 'unrar' is already installed on Windows."
        else
            echo "⏳ Installing 'unrar' on Windows (via Chocolatey)..."
            choco install unrar -y
        fi
        ;;
    *)
        echo "❌ Unsupported OS: $OS"
        exit 1
        ;;
esac
