## Usage

Install coyote 

```sh
go get github.com/landoop/coyote
```

Build Kafka Backup (from the root directory):

```sh
./gradlew shadowJar
```
  
Then, just run coyote inside this directory.

```
coyote
```

When finished, open `coyote.html`.

## Software

You need these programs to run the test:
- [Coyote](https://github.com/Landoop/coyote/releases)
- [Docker](https://docs.docker.com/engine/installation/)
- [Docker Compose](https://docs.docker.com/engine/installation/)

Everything else is set up automatically inside containers.