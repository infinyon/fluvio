MAKE_DIR = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
VERSION := $(shell cat $(MAKE_DIR)/../../VERSION)
APP_VERSION = $(VERSION)

# remove packaged charts
# must clean if any of the contents changed due to limitation of "build.rs"
clean:
	rm -rf pkg_sys;
	rm -rf pkg_app;

# generate packaged chart for sys
pkg_sys:	fluvio-sys/*.* ../../VERSION
	mkdir -p pkg_sys;
	helm package ./fluvio-sys --app-version $(APP_VERSION) -d pkg_sys
	cd pkg_sys;mv fluvio-sys-* fluvio-chart-sys.tgz

# generate packaged chart for app
pkg_app:	fluvio-app/*.* ../../VERSION
	mkdir -p pkg_app
	helm package ./fluvio-app --app-version $(APP_VERSION) -d pkg_app
	cd pkg_app;mv fluvio-app-* fluvio-chart-app.tgz

package:	pkg_sys pkg_app
	echo "Packaged charts are in pkg_sys and pkg_app"