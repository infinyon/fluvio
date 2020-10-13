#!/bin/bash
FLUVIO_DIR=$HOME/.fluvio
FLUVIO_BIN=$HOME/.fluvio/bin

success_msg () {
    printf "\n\n\n\e[1mFluvio Installation complete!\n\n\nRun the following command to update your PATH for the fluvio binary:\n\n\n\n"
}

error_msg_unsupported_os () {
        echo "unsupported operating system $OSTYPE; ignoring fluvio install"
}

install_unix () {
    mkdir $FLUVIO_DIR
    mkdir $FLUVIO_BIN
    cd $FLUVIO_BIN
        wget https://github.com/infinyon/fluvio/releases/download/$1/fluvio-$1-$2
        mv fluvio-$1-$2 fluvio
        chmod +x fluvio
    cd -

    case $SHELL in 
        *"bash"* )
            echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.bashrc
            . $HOME/.bashrc
        ;;
        *"zsh"* )
            echo 'export PATH="$HOME/.fluvio/bin:$PATH"' >> $HOME/.zshrc
            . $HOME/.zshrc
        ;;
        *)
            echo "Unknown Operating System Shell, $SHELL, is not supported; recommend exporting fluvio PATH manually"
            echo "export PATH:$FLUVIO_BIN"
        ;;
    esac
}

if [ -z "$VERSION" ]; then
    echo '$VERSION variable is not set'
    exit 1
fi

case $OSTYPE in
    "linux-gnu"*)
        install_unix $VERSION x86_64-unknown-linux-musl
    ;;
    "darwin"* )
        install_unix $VERSION x86_64-apple-darwin
    ;;
    "cygwin" )
        error_msg_unsupported_os
        exit 1
    ;;
    "msys" )
        error_msg_unsupported_os
        exit 1
    ;;
    "win32" )
        error_msg_unsupported_os
        exit 1
    ;;
    "freebsd"* )
        error_msg_unsupported_os
        exit 1
    ;;
    *)
        error_msg_unsupported_os
        exit 1
    ;;
esac