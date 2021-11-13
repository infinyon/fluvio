MAKE_DIR = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
VERSION := $(shell cat $(MAKE_DIR)/../../VERSION)
APP_VERSION = $(VERSION)

# remove packaged charts
clean:
	rm -rf pkg_sys;
	rm -rf pkg_app;

# generate packaged chart for sys
pkg_sys:	fluvio-sys/*.* ../../VERSION
	mkdir -p pkg_sys;
	helm package ./fluvio-sys --app-version $(APP_VERSION) -d pkg_sys

# generate packaged chart for app
pkg_app:	fluvio-app/*.* ../../VERSION
	mkdir -p pkg_app
	helm package ./fluvio-app --app-version $(APP_VERSION) -d pkg_app

package:	pkg_sys pkg_app