#!/bin/sh
# shellcheck shell=dash

readonly FLUVIO_BIN="${HOME}/.fluvio/bin"
readonly FLUVIO_LATEST_URL="https://packages.fluvio.io/v1/latest"

main() {
    downloader --check
    need_cmd uname
    need_cmd mktemp
    need_cmd chmod
    need_cmd mkdir
    need_cmd mv
    need_cmd shasum

    # Detect architecture and ensure it's supported
    get_architecture || return 1
    local _arch="$RETVAL"
    assert_nz "$_arch"
    assert_supported_architecture ${_arch}
    local _latest=$(fetch_latest_version_for_architecture "${_arch}")
    local _url="https://packages.fluvio.io/v1/packages/fluvio/fluvio/${_latest}/${_arch}/fluvio"

    # Download Fluvio to a temporary file
    say "⏳ Downloading fluvio ${_latest} for ${_arch}..."
    local _temp_file=$(download_fluvio_to_temp "${_url}")
    ensure downloader "${_url}" "${_temp_file}"

    # Verify the checksum of the temporary file
    say "🔑 Downloaded fluvio, verifying checksum..."
    verify_checksum "${_url}" "${_temp_file}" || return 1

    # After verification, install the file and make it executable
    say "✅ Verified checksum, installing fluvio..."
    ensure mkdir -p "${FLUVIO_BIN}"
    local _install_file="${FLUVIO_BIN}/fluvio"
    ensure mv "${_temp_file}" "${_install_file}"
    ensure chmod +x "${_install_file}"
    say "🎉 Install complete!"
    remind_path

    return 0
}

# Ensure that this architecture is supported and matches the
# naming convention of known platform releases in the registry
#
# @param $1: The target triple of this architecture
# @return: Status 0 if the architecture is supported, exit if not
assert_supported_architecture() {
    local _arch="$1"; shift

    # Match against all supported architectures
    case $_arch in
        x86_64-apple-darwin)
            return 0
            ;;
        x86_64-unknown-linux-musl)
            return 0
            ;;
    esac

    # If this architecture is not supported, return error
    err "Architecture ${_arch} is not supported."
    abort_prompt_issue
}

# Fetch the latest released version for this architecture
#
# @param $1: The target triple of this architecture
# @return <stdout>: The version of the latest release
fetch_latest_version_for_architecture() {
    local _arch="$1"; shift

    # Download the list of latest releases
    local _downloaded=$(ensure downloader "${FLUVIO_LATEST_URL}" - 2>&1)

    # Find the latest release for this architecture
    local _latest=$(echo "${_downloaded}" | grep "${_arch}" | sed -E "s/(.*)?=(.*)?/\2/")
    echo "${_latest}"
}

# Download the Fluvio CLI binary to a temporary file
# This returns the name of the temporary file on stdout,
# and is intended to be captured via a subshell $()
#
# @param $1: The URL of the file to download to a temporary dir
# @return <stdout>: The path of the temporary file downloaded
download_fluvio_to_temp() {
    local _url="$1"; shift
    local _dir="$(mktemp -d 2>/dev/null || ensure mktemp -d -t fluvio-download)"
    local _temp_file="${_dir}/fluvio"
    ensure downloader "${_url}" "${_temp_file}"
    echo "${_temp_file}"
}

# Verify the checksum of the downloaded file with the published
# checksum that accompanies the file.
#
# @param $1: The URL of the file that was downloaded. The checksums
#            of files in the Fluvio registry are located at the same
#            path with a .sha256 extension.
# @param $2: The temporary file path where the Fluvio CLI was downloaded
# @return: Status 0 if checksum matches, exit if it does not
verify_checksum() {
    local _url="$1"; shift
    local _temp_file="$1"; shift

    local _dir="$(mktemp -d 2>/dev/null || ensure mktemp -d -t fluvio-checksum)"
    local _checksum_url="${_url}.sha256"
    local _downloaded_checksum_file="${_dir}/fluvio.sha256-downloaded"
    local _calculated_checksum_file="${_dir}/fluvio.sha256-calculated"

    # Download the posted checksum
    local _downloaded=$(ensure downloader "${_checksum_url}" - 2>&1)
    # Add '  -\n' to the download to match the formatting given by shasum
    echo "${_downloaded}  -" > "${_downloaded_checksum_file}"

    # Calculate the checksum from the downloaded executable
    shasum -a256 < "${_temp_file}" > "${_dir}/fluvio.sha256-calculated"

    local _err=$(diff "${_downloaded_checksum_file}" "${_calculated_checksum_file}")
    if [ -n "$_err" ]; then
        err "❌ Unable to verify the checksum of the downloaded file, aborting"
        abort_prompt_issue
    fi
}

# Prompts the user to add ~/.fluvio/bin to their PATH variable
remind_path() {
    say "💡 You'll need to add '~/.fluvio/bin/' to your PATH variable"
    say "    You can run the following to set your PATH on shell startup:"
    # shellcheck disable=SC2016
    say '      For bash: echo '\''export PATH="${HOME}/.fluvio/bin:${PATH}"'\'' >> ~/.bashrc'
    # shellcheck disable=SC2016
    say '      For zsh : echo '\''export PATH="${HOME}/.fluvio/bin:${PATH}"'\'' >> ~/.zshrc'
    say ""
    say "    To use fluvio you'll need to restart your shell or run the following:"
    # shellcheck disable=SC2016
    say '      export PATH="${HOME}/.fluvio/bin:${PATH}"'
}

############################################################
##### Below are utilities borrowed from rustup-init.sh #####
############################################################

say() {
    printf '\e[1;34mfluvio:\e[0m %s\n' "$1"
}

err() {
    printf '\e[1;31mfluvio:\e[0m %s\n' "$1" >&2
}

# Exit immediately, prompting the user to file an issue on GH <3
abort_prompt_issue() {
    err ""
    err "If you believe this is incorrect, please feel free to file an issue on Github ❤️"
    err "    https://github.com/infinyon/fluvio/issues/new"
    exit 1
}

need_cmd() {
    if ! check_cmd "$1"; then
        err "need '$1' (command not found)"
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

# Run a command that should never fail. If the command fails execution
# will immediately terminate with an error showing the failing
# command.
ensure() {
    if ! "$@"; then err "command failed: $*"; fi
}

assert_nz() {
    if [ -z "$1" ]; then err "assert_nz $2"; fi
}

# Adapted from rustup-init.sh:
# This wraps curl or wget. Try curl first, if not installed, use wget instead.
downloader() {
    local _dld _status

    if check_cmd curl; then
        _dld=curl
    elif check_cmd wget; then
        _dld=wget
    else
        _dld='curl or wget' # to be used in error message of need_cmd
    fi

    if [ "$1" = --check ]; then
        need_cmd "$_dld"
    elif [ "$_dld" = curl ]; then
        # Use curl
        _err=$(curl --proto '=https' --tlsv1.2 --silent --show-error --fail --location "$1" --output "$2" 2>&1)
        _status=$?

        # If there is anything on stderr, print it
        if [ -n "$_err" ]; then
            echo "$_err" >&2
        fi
        return $_status
    elif [ "$_dld" = wget ]; then
        # Use wget
        wget --https-only --secure-protocol=TLSv1_2 "$1" -O "$2" 2>&1
        _status=$?

        # If there is anything on stderr, print it
        if [ -n "$_err" ]; then
            echo "$_err" >&2
        fi
        return $_status
    else
        err "Unknown downloader"   # should not reach here
    fi
}

# Cross-platform architecture detection, borrowed from rustup-init.sh
get_architecture() {
    local _ostype _cputype _bitness _arch _clibtype
    _ostype="$(uname -s)"
    _cputype="$(uname -m)"
    _clibtype="gnu"

    if [ "$_ostype" = Linux ]; then
        if [ "$(uname -o)" = Android ]; then
            _ostype=Android
        fi
        if ldd --version 2>&1 | grep -q 'musl'; then
            _clibtype="musl"
        fi
    fi

    if [ "$_ostype" = Darwin ] && [ "$_cputype" = i386 ]; then
        # Darwin `uname -m` lies
        if sysctl hw.optional.x86_64 | grep -q ': 1'; then
            _cputype=x86_64
        fi
    fi

    if [ "$_ostype" = SunOS ]; then
        # Both Solaris and illumos presently announce as "SunOS" in "uname -s"
        # so use "uname -o" to disambiguate.  We use the full path to the
        # system uname in case the user has coreutils uname first in PATH,
        # which has historically sometimes printed the wrong value here.
        if [ "$(/usr/bin/uname -o)" = illumos ]; then
            _ostype=illumos
        fi

        # illumos systems have multi-arch userlands, and "uname -m" reports the
        # machine hardware name; e.g., "i86pc" on both 32- and 64-bit x86
        # systems.  Check for the native (widest) instruction set on the
        # running kernel:
        if [ "$_cputype" = i86pc ]; then
            _cputype="$(isainfo -n)"
        fi
    fi

    case "$_ostype" in

        Android)
            _ostype=linux-android
            ;;

        Linux)
            _ostype=unknown-linux-$_clibtype
            _bitness=$(get_bitness)
            ;;

        FreeBSD)
            _ostype=unknown-freebsd
            ;;

        NetBSD)
            _ostype=unknown-netbsd
            ;;

        DragonFly)
            _ostype=unknown-dragonfly
            ;;

        Darwin)
            _ostype=apple-darwin
            ;;

        illumos)
            _ostype=unknown-illumos
            ;;

        MINGW* | MSYS* | CYGWIN*)
            _ostype=pc-windows-gnu
            ;;

        *)
            err "unrecognized OS type: $_ostype"
            ;;

    esac

    case "$_cputype" in

        i386 | i486 | i686 | i786 | x86)
            _cputype=i686
            ;;

        xscale | arm)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            fi
            ;;

        armv6l)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        armv7l | armv8l)
            _cputype=armv7
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        aarch64 | arm64)
            _cputype=aarch64
            ;;

        x86_64 | x86-64 | x64 | amd64)
            _cputype=x86_64
            ;;

        mips)
            _cputype=$(get_endianness mips '' el)
            ;;

        mips64)
            if [ "$_bitness" -eq 64 ]; then
                # only n64 ABI is supported for now
                _ostype="${_ostype}abi64"
                _cputype=$(get_endianness mips64 '' el)
            fi
            ;;

        ppc)
            _cputype=powerpc
            ;;

        ppc64)
            _cputype=powerpc64
            ;;

        ppc64le)
            _cputype=powerpc64le
            ;;

        s390x)
            _cputype=s390x
            ;;
        riscv64)
            _cputype=riscv64gc
            ;;
        *)
            err "unknown CPU type: $_cputype"

    esac

    # Detect 64-bit linux with 32-bit userland
    if [ "${_ostype}" = unknown-linux-gnu ] && [ "${_bitness}" -eq 32 ]; then
        case $_cputype in
            x86_64)
                _cputype=i686
                ;;
            mips64)
                _cputype=$(get_endianness mips '' el)
                ;;
            powerpc64)
                _cputype=powerpc
                ;;
            aarch64)
                _cputype=armv7
                if [ "$_ostype" = "linux-android" ]; then
                    _ostype=linux-androideabi
                else
                    _ostype="${_ostype}eabihf"
                fi
                ;;
            riscv64gc)
                err "riscv64 with 32-bit userland unsupported"
                ;;
        esac
    fi

    # Detect armv7 but without the CPU features Rust needs in that build,
    # and fall back to arm.
    # See https://github.com/rust-lang/rustup.rs/issues/587.
    if [ "$_ostype" = "unknown-linux-gnueabihf" ] && [ "$_cputype" = armv7 ]; then
        if ensure grep '^Features' /proc/cpuinfo | grep -q -v neon; then
            # At least one processor does not have NEON.
            _cputype=arm
        fi
    fi

    _arch="${_cputype}-${_ostype}"

    RETVAL="$_arch"
}

main "$@"
