## Reference
-   [Is there a free GUI management tool for Oracle Database Express?](https://stackoverflow.com/questions/3496948/is-there-a-free-gui-management-tool-for-oracle-database-express)
    -   [Oracle SQL Developer](https://www.oracle.com/database/technologies/appdev/sqldeveloper-landing.html)  
    -   [SQL Developer 20.4.1 Downloads](https://www.oracle.com/tools/downloads/sqldev-downloads.html)
    -   [Oracle Instant Client](https://www.oracle.com/tw/database/technologies/instant-client.html)
        ```bash
        mkdir -p ~/libs
        unzip instantclient-basic-linuxAMD64-10.1.0.5.0-20060519.zip
        mv instantclient10_1 ~/libs
        cd ~/libs
        ln -s $(pwd)/instantclient10_1 $(pwd)/instantclient
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/instantclient
        ```
-   [如何在ubuntu 底下安裝rpm檔](https://flipper.pixnet.net/blog/post/39285479)
    ```bash
    sudo apt install -y alien
    sudo alien xxx.rpm
    sudo dpkg -i xxx.deb
    ```
-   [How do I get Java FX running with OpenJDK 8 on Ubuntu 18.04.2 LTS?](https://stackoverflow.com/questions/56166267/how-do-i-get-java-fx-running-with-openjdk-8-on-ubuntu-18-04-2-lts)
    ```bash
    sudo apt purge openjfx
    sudo apt install openjfx=8u161-b12-1ubuntu2 libopenjfx-jni=8u161-b12-1ubuntu2 libopenjfx-java=8u161-b12-1ubuntu2
    sudo apt-mark hold openjfx libopenjfx-jni libopenjfx-java
    ```
-   JDK 8 root path  
    Basically it's `/usr/lib/jvm/java-8-openjdk-amd64` if you use [OpenJDK](https://openjdk.java.net/).
-   [Oracle Database Docker image](https://hub.docker.com/r/oracleinanutshell/oracle-xe-11g)
    ```bash
    docker pull oracleinanutshell/oracle-xe-11g
    docker run --name oracle -d \
        --network nexmasa \
        -p 49161:1521 \
        -e ORACLE_DISABLE_ASYNCH_IO=true \
        -e ORACLE_ALLOW_REMOTE=true \
        -v /etc/localtime:/etc/localtime \
        -v oracle:/u01/app/oracle/oradata \
        --restart unless-stopped \
        oracleinanutshell/oracle-xe-11g
    ```
    By default, the password verification is disable (password never expired).  
    Connect database with following setting:
    ```
    port: 49161
    sid: xe
    username: system
    password: oracle
    ```

## Installation
```bash
sudo apt install -y alien
sudo alien sqldeveloper-20.4.1.407.0006-20.4.1-407.0006.noarch.rpm
sudo dpkg -i sqldeveloper-20.4.1.407.0006-20.4.1-407.0006.noarch.deb
sudo apt purge openjfx
sudo apt install openjfx=8u161-b12-1ubuntu2 libopenjfx-jni=8u161-b12-1ubuntu2 libopenjfx-java=8u161-b12-1ubuntu2
sudo apt-mark hold openjfx libopenjfx-jni libopenjfx-java
sqldeveloper
```
