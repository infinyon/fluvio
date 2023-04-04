#!/bin/bash
# shellcheck shell=bash

set -e
set -u
set -o pipefail


readonly TEST="${TEST:-false}"
if [ "$TEST" = "true" ]; then
    export FLUVIO_PREFIX="https://packages.fluvio.io/test"
    echo "Sourcing packages from test repo: ${FLUVIO_PREFIX}"
fi

readonly FLUVIO_BIN="${HOME}/.fluvio/bin"
readonly FLUVIO_PREFIX="${FLUVIO_PREFIX:-"https://packages.fluvio.io/v1"}"
readonly FLUVIO_TAG_STABLE="stable"
readonly FLUVIO_PACKAGE="fluvio/fluvio"
readonly FLUVIO_FRONTEND_PACKAGE="fluvio/fluvio-channel"
readonly FLUVIO_EXTENSIONS="${HOME}/.fluvio/extensions"

# Ensure that this target is supported and matches the
# naming convention of known platform releases in the registry
#
# @param $1: The target triple of this architecture
# @return: Status 0 if the architecture is supported, exit if not
normalize_target() {
    local _target="$1"; shift

    # Match against all supported targets
    case $_target in
        x86_64-unknown-linux-gnu)
            echo "x86_64-unknown-linux-musl"
            return 0
            ;;
        aarch64-unknown-linux-gnu)
            echo "aarch64-unknown-linux-musl"
            return 0
            ;;
    esac

    echo "${_target}"
    return 0
}

# Fetch the tagged version of the given package
#
# @param $1: The name of the tag to lookup the version for
#   - Defaults to $FLUVIO_TAG_STABLE, i.e. "stable"
# @param $2 (optional): The name of the package to lookup
#   - Defaults to $FLUVIO_PACKAGE, i.e. "fluvio/fluvio"
# @return <stdout>: The version of the release that is tagged
# shellcheck disable=SC2120
fetch_tag_version() {
    local _status _downloaded
    local _tag="${1:-"$FLUVIO_TAG_STABLE"}"; shift
    local _package="${1:-"$FLUVIO_PACKAGE"}";
    local -r _tag_url="${FLUVIO_PREFIX}/packages/${_package}/tags/${_tag}"

    # Download the list of latest releases
    _downloaded="$(downloader "${_tag_url}" - 2>&1)"
    _status=$?

    if [ $_status -ne 0 ]; then
        return $_status
    fi

    if [ -z "$_downloaded" ]; then
        return 1
    fi

    echo "${_downloaded}"
}

# Download the Fluvio CLI binary to a temporary file
# This returns the name of the temporary file on stdout,
# and is intended to be captured via a subshell $()
#
# @param $1: The URL of the file to download to a temporary dir
# @return <stdout>: The path of the temporary file downloaded
download_fluvio_to_temp() {
    local _status _dir _temp_file
    local _url="$1"; shift

    _dir="$(mktemp -d 2>/dev/null || ensure mktemp -d -t fluvio-download)"
    _temp_file="${_dir}/fluvio-${_version}"

    downloader "${_url}" "${_temp_file}"
    _status=$?
    if [ $_status -ne 0 ]; then
        if [ -n "${FLV_VERSION:-""}" ]; then
            # If a FLV_VERSION was set, warn that it may be invalid
            err "❌ Failed to download Fluvio, could not find custom FLV_VERSION=${FLV_VERSION}"
            err "    Make sure this is a valid version, or unset FLV_VERSION to download stable"
        else
            # If downloading the latest version, warn a general download error
            err "❌ Failed to download Fluvio!"
            err "    Error downloading from ${_url}"
        fi
        abort_prompt_issue
    fi

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
    local _dir _downloaded _err

    _dir="$(mktemp -d 2>/dev/null || ensure mktemp -d -t fluvio-checksum)"
    local _checksum_url="${_url}.sha256"
    local _downloaded_checksum_file="${_dir}/fluvio.sha256-downloaded"
    local _calculated_checksum_file="${_dir}/fluvio.sha256-calculated"

    # Download the posted checksum
    _downloaded="$(ensure downloader "${_checksum_url}" - 2>&1)"
    # Add '  -\n' to the download to match the formatting given by shasum
    echo "${_downloaded}  -" > "${_downloaded_checksum_file}"

    # Calculate the checksum from the downloaded executable
    shasum -a256 < "${_temp_file}" > "${_dir}/fluvio.sha256-calculated"

    _err=$(diff "${_downloaded_checksum_file}" "${_calculated_checksum_file}")
    if [ -n "$_err" ]; then
        err "❌ Unable to verify the checksum of the downloaded file, aborting"
        abort_prompt_issue
    fi
}

# Prompts the user to add ~/.fluvio/bin to their PATH variable
remind_path() {
    say "💡 You'll need to add '~/.fluvio/bin/' to your PATH variable"
    say "    You can run the following to set your PATH on shell startup:"
    
    # shellcheck disable=SC2016,SC2155
    local bash_hint="$(tput bold)"'echo '\''export PATH="${HOME}/.fluvio/bin:${PATH}"'\'' >> ~/.bashrc'"$(tput sgr0)"
    # shellcheck disable=SC2016,SC2155
    local zsh_hint="$(tput bold)"'echo '\''export PATH="${HOME}/.fluvio/bin:${PATH}"'\'' >> ~/.zshrc'"$(tput sgr0)"
    # shellcheck disable=SC2016,SC2155
    local fish_hint="$(tput bold)"'fish_add_path "$HOME/.fluvio/bin"'"$(tput sgr0)"

    case "$(basename "${SHELL}")" in
        bash)
            say "      ${bash_hint}"
            ;;
        zsh)
            say "      ${zsh_hint}"
            ;;
        fish)
            say "      ${fish_hint}"
            ;;
        *)
            say "      For bash: ${bash_hint}"
            say "      For zsh : ${zsh_hint}"
            say "      For fish : ${fish_hint}"
            ;;
    esac
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
    err "If you believe this is a bug (or just need help),"
    err "please feel free to file an issue on Github ❤️"
    err "    https://github.com/infinyon/fluvio/issues/new"
    exit 1
}

need_cmd() {
    if ! check_cmd "$1"; then
        err "need '$1' (command not found)"
        exit 1
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

# Run a command that should never fail. If the command fails execution
# will immediately terminate with an error showing the failing
# command.
ensure() {
    if ! "$@"; then
        err "command failed: $*"
        exit 1
    fi
}

assert_nz() {
    if [ -z "$1" ]; then
        err "assert_nz $2"
        exit 1
    fi
}

# Uses curl to download the contents of a URL to a file.
#
# @param $1: The URL of the file to download
# @param $2: The filename of where to download
downloader() {
    local _status
    local _url="$1"; shift
    local _file="$1"; shift

    # Use curl for downloads
    _err=$(curl --proto '=https' --tlsv1.2 --silent --show-error --fail --location "${_url}" --output "${_file}" 2>&1)
    _status=$?

    # If there is anything on stderr, print it
    if [ -n "$_err" ]; then
        echo "$_err" >&2
    fi
    return $_status
}

get_bitness() {
    need_cmd head
    # Architecture detection without dependencies beyond coreutils.
    # ELF files start out "\x7fELF", and the following byte is
    #   0x01 for 32-bit and
    #   0x02 for 64-bit.
    # The printf builtin on some shells like dash only supports octal
    # escape sequences, so we use those.
    local _current_exe_head
    _current_exe_head=$(head -c 5 /proc/self/exe )
    if [ "$_current_exe_head" = "$(printf '\177ELF\001')" ]; then
        echo 32
    elif [ "$_current_exe_head" = "$(printf '\177ELF\002')" ]; then
        echo 64
    else
        err "unknown platform bitness"
    fi
}

get_endianness() {
    local cputype=$1
    local suffix_eb=$2
    local suffix_el=$3

    # detect endianness without od/hexdump, like get_bitness() does.
    need_cmd head
    need_cmd tail

    local _current_exe_endianness
    _current_exe_endianness="$(head -c 6 /proc/self/exe | tail -c 1)"
    if [ "$_current_exe_endianness" = "$(printf '\001')" ]; then
        echo "${cputype}${suffix_el}"
    elif [ "$_current_exe_endianness" = "$(printf '\002')" ]; then
        echo "${cputype}${suffix_eb}"
    else
        err "unknown platform endianness"
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

main() {
    need_cmd curl
    need_cmd uname
    need_cmd mktemp
    need_cmd chmod
    need_cmd mkdir
    need_cmd mv
    # need_cmd shasum
    local _status _target _version _temp_file

    # Detect architecture and ensure it's supported
    get_architecture || return 1
    local _arch="$RETVAL"
    assert_nz "$_arch"

    # Some architectures may be folded into a single 'target' distribution
    # e.g. x86_64-unknown-linux-musl and x86_64-unknown-linux-gnu both download
    # the musl target release. The _target here is used in the URL to download
    _target=$(normalize_target ${_arch})


    FLV_VERSION="${FLV_VERSION:-stable}"


    set +e
    _version=$(fetch_tag_version "${FLV_VERSION}")
    _status=$?
    set -e

    # If we did not find a version via a tag, use FLV_VERSION as the version itself
    if [ $_status -ne 0 ]; then
        _version="${FLV_VERSION}"
    fi

    # If we are using "stable" or "latest"
    # Be sure to use those tags in the installed artifacts
    # Otherwise, use the value from ${_version}

    if [[ "${FLV_VERSION}" == "stable" ]] || [[ "${FLV_VERSION}" == "latest" ]] ; then
        #say "DEBUG: Using stable or latest"
        _use_tag_file_name="true"
    else
        _use_tag_file_name="false"
    fi

    # Download Fluvio to a temporary file
    local _url="${FLUVIO_PREFIX}/packages/${FLUVIO_PACKAGE}/${_version}/${_target}/fluvio"
    say "⏳ Downloading Fluvio ${_version} for ${_target}..."
    _temp_file=$(download_fluvio_to_temp "${_url}")
    _status=$?
    if [ $_status -ne 0 ]; then
        err "❌ Failed to download Fluvio!"
        err "    Error downloading from ${_url}"
        abort_prompt_issue
    fi

    # Verify the checksum of the temporary file
    # say "🔑 Downloaded fluvio, verifying checksum..."
    # verify_checksum "${_url}" "${_temp_file}" || return 1

    # After verification, install the file and make it executable
    say "⬇️  Downloaded Fluvio, installing..."
    ensure mkdir -p "${FLUVIO_BIN}"

    # Install as fluvio-${_version}
    if [[ "${_use_tag_file_name}" == "true" ]] ; then

        # Stable extensions go in main dir
        if [[ "${FLV_VERSION}" == "stable" ]] ; then
            ensure mkdir -p "${FLUVIO_EXTENSIONS}"
        fi

        # Latest extensions go in latest dir
        if [[ "${FLV_VERSION}" == "latest" ]] ; then
            ensure mkdir -p "${FLUVIO_EXTENSIONS}-${FLV_VERSION}"
        fi

        local _install_file="${FLUVIO_BIN}/fluvio-${FLV_VERSION}"
    else 
        # If we installed a specific version, place extension in that dir
        # Ex. fluvio-0.x.y
        local _install_file="${FLUVIO_BIN}/fluvio-${_version}"
        ensure mkdir -p "${FLUVIO_EXTENSIONS}-${_version}"
    fi

    ensure mv "${_temp_file}" "${_install_file}"
    ensure chmod +x "${_install_file}"
    say "✅ Successfully installed ${_install_file}"

    # Install Fluvio channel
    # Download Fluvio-channel to a temporary file

    set +e
    _version=$(fetch_tag_version "${FLV_VERSION}" "${FLUVIO_FRONTEND_PACKAGE}")
    _status=$?
    set -e

    # If we did not find a version via a tag, use FLV_VERSION as the version itself
    if [ $_status -ne 0 ]; then
        _version="${FLV_VERSION}"
    fi

    local _url="${FLUVIO_PREFIX}/packages/${FLUVIO_FRONTEND_PACKAGE}/${_version}/${_target}/fluvio-channel"
    local _install_file="${FLUVIO_BIN}/fluvio"
    say "⏳ Downloading Fluvio channel frontend ${_version} for ${_target}..."
    _temp_file=$(download_fluvio_to_temp "${_url}")
    _status=$?
    if [ $_status -ne 0 ]; then
        err "❌ Failed to download Fluvio!"
        err "    Error downloading from ${_url}"
        abort_prompt_issue
    fi

    ensure mv "${_temp_file}" "${_install_file}"
    ensure chmod +x "${_install_file}"
    say "✅ Successfully installed ${_install_file}"


    # If a FLV_VERSION env variable is set, use it for fluvio-run:
    if [ -n "${FLV_VERSION:-""}" ]; then
        local _fluvio_run="fluvio-run:${FLV_VERSION}"
    else
        local _fluvio_run="fluvio-run"
    fi

    # Let fluvio know it is invoked from installer
    # Also let fluvio know what channel is being installed
    say "☁️ Installing Fluvio Cloud..."
    FLUVIO_BOOTSTRAP=true CHANNEL_BOOTSTRAP=${FLV_VERSION} "${FLUVIO_BIN}/fluvio" install fluvio-cloud

    say "☁️ Installing Fluvio Runner..."
    FLUVIO_BOOTSTRAP=true CHANNEL_BOOTSTRAP=${FLV_VERSION} "${FLUVIO_BIN}/fluvio" install "${_fluvio_run}"

    say "🎉 Install complete!"
    remind_path

    return 0
}

main "$@"
