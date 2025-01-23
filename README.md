# 💾 datago
**datago** is a simple database operation collection for Java developers. It highly concentrates the operations of various 
mainstream databases. And it provides a variety of extended functions.

datago supports **Java 1.8** or above.

### Modules
##### [`datago-mongodb`](https://github.com/Hexsook/datago/tree/master/mongodb): For MongoDB operations.
![Maven Central Version](https://img.shields.io/maven-central/v/io.github.hexsook/datago-mongodb.svg) ![Javadoc Version](https://javadoc.io/badge/io.github.hexsook/datago-mongodb.svg)
##### [`datago-redis`](https://github.com/Hexsook/datago/tree/master/redis): For Redis operations
![Maven Central Version](https://img.shields.io/maven-central/v/io.github.hexsook/datago-redis.svg) ![Javadoc Version](https://javadoc.io/badge/io.github.hexsook/datago-redis.svg)

### Adding datago to your project
**How to**: Add the dependency to pom.xml in your project:
```xml
<dependencies>
    <dependency>
        <groupId>io.github.hexsook</groupId>
        <artifactId>[module]</artifactId>
        <version>[version]</version>
        <scope>compile</scope>
    </dependency>
</dependencies>
```
##### Placeholders:
`[module]` - the module you need to import. **See [Modules](#modules).** <br>
`[version]` - the module version.

### About Project
##### Version suffixes:
##### `no suffixes` - **Release version.**

`-pr` - Pre-release version. <br>
`-beta` - Beta test version. <br>
`-unstable` - Internal testing. (Very unstable!) <br>

##### Version naming rule (e.g. 1.3.2):
**X** (Major code changes) **. X** (Minor functional updates) **. X** (Bug fixes/Minor changes)