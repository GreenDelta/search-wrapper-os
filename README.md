# Search wrapper - Implementation for opensearch
This project provides an implementation for the search-wrapper API using opensearch as a search engine.

## Build from source

#### Dependent modules
In order to build the search-wrapper-os module, you will need to install the search-wrapper API first.
This is a plain Maven projects and can be installed via `mvn install`. See the
[search-wrapper](https://github.com/GreenDelta/search-wrapper) repository for more
information.

#### Get the source code of the application
We recommend that to use Git to manage the source code but you can also download
the source code as a [zip file](https://github.com/GreenDelta/search-wrapper-os/archive/main.zip).
Create a development directory (the path should not contain whitespaces):

```bash
mkdir dev
cd dev
```

and get the source code:

```bash
git clone https://github.com/GreenDelta/search-wrapper-os.git
```

#### Build
Now you can build the module with `mvn install`, which will install the module in your local maven repository.