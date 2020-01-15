## How to use snapshot version

Snapshot version is also able to work normally. Please add the following repository to your repositories if you are using the snapshot version. See more on [sonatype](https://oss.sonatype.org/content/repositories/snapshots/com/alibaba/innodb-java-reader/).

```
<!-- Note: add this if you are using SNAPSHOT version -->
<repositories>
    <repository>
        <id>Sonatype</id>
        <name>Sonatype's repository</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```