# Useful snippets

**Tips**: For pretty display on a Mac, install **jsonpp**  ```brew install jsonpp```

## Display raw jason output

Display all infinyon objects
> kubectl get --raw /apis/fluvio.infinyon.com/v1 | jsonpp

Display topic ```test```
> kubectl get --raw /apis/fluvio.infinyon.com/v1/namespaces/default/topics/test | jsonpp
