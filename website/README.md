# Fluvio Website

All documentation intended for [fluvio-website](https://github.com/infinyon/fluvio-website) (feeder for [fluvio.io](https://fluvio.io)) must be placed inside `website` folder.


## Website Folder Organization

**Fluvio Website** assumes 'website' directory is located at the top of the **project** directory has 2 sub-directories:

* docs
* img (optional)

These folders should be to copied in **fluvio-website** by using [test-repo.sh](https://github.com/infinyon/fluvio-website/tree/master/scripts) script. If test-repo.sh is executed with target directory `cli`, the content is placed in **fluvio-website** as follows:

* `docs` content is placed inside `content/docs/cli`
* `img` content placed in `static/img/cli`
    * to reference an image, use the following shortcode:
    ```
    {{< image src="cli/fluvio.svg" alt="Fluvio" justify="center" width="560" type="scaled-90">}}
    ```


## Test Fluvio Rendering

To ensure the `website` content is parsed successfully, image are render correctly, and links are pointed to the appropriate content, the website should be be tested on local machine. Website validation tests should be performed before this content is checked-in.

### Setup Website environment on local machine

1. [Clone fluvio-website](https://github.com/infinyon/fluvio-website) to your local machine
2. [Install Hugo](https://github.com/infinyon/fluvio-website#run-website-on-local-machine)
3. Navigate to `fluvio-website/scripts` run `test-repo.sh`:
    ```
    ./test-repo.sh -b master -r fluvio -d cli
    ```
4. View changes in:
    ```
    http://localhost:1313/docs/cli/
    ```

### Incremental changes

Hugo continuously watches for file changes and automatically renders a new content.
To change the content of the website:

1. Make your changes in `fluvio/website` folder
2. Switch to`fluvio-website` repo and run `./scripts/test-repo.sh` to pull the changes:
    ```
    ./scripts/test-repo.sh -b master -r fluvio -d cli
    ```
3. Checkout the website:
    ```
    http://localhost:1313/docs/cli/
    ```

 