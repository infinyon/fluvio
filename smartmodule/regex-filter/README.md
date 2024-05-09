Example of SmartModule with regex.

To run this, have fluvio stable installed.

compile this package:
```
$ smdk build
```

run regex at top level:

positive:
```
$ smdk test --text AA -e regex="[A-Z]"
1 records
AA
```

negative:
```
$ smdk test --text aa -e regex="[A-Z]"
0 records
```
