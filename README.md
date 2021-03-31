# Convergence Functional Test

## Prerequisties
1.  MongoDB server, 必須有一個 database 叫做 `nexmasa`, 且 `nexmasa` 內有兩個 collections 分別為 `dcts` 與 `sensormap`
1.  Oracle Database, 以建好一個 table 名為 `conv_it_test`, 且設好 CDC/logminer.
1.  Kafka broker (單機或叢集)
1.  Python 3.6 或更新的版本

## Syntax
### datagen.py
```
usage: datagen.py [-h] [--mongo MONGO] [--input INPUT] [--output OUTPUT]

optional arguments:
  -h, --help            show this help message and exit
  --mongo MONGO, -m MONGO
                        MongoDB server IP:PORT (default: mongo:27017)
  --input INPUT, -i INPUT
                        Path to IT example data (Excel) file. (default:
                        ./it_data.xlsx)
  --output OUTPUT, -o OUTPUT
                        Path to output dataset (Excel) file. (default:
                        ./data.xlsx)
```

### itgen.py
```
usage: itgen.py [-h] [--host HOST] [--port PORT] [--service SERVICE]
                [--username USERNAME] [--password PASSWORD] [--table TABLE]
                [--excel EXCEL]

optional arguments:
  -h, --help            show this help message and exit
  --host HOST, -H HOST  Oracle database host. (default: localhost)
  --port PORT, -p PORT  Oracle database port (usually 49161 or 1521) (default:
                        49161)
  --service SERVICE, -s SERVICE
                        Oracle dtabase service name (default: xe)
  --username USERNAME, -U USERNAME
                        Oracle username (default: system)
  --password PASSWORD, -P PASSWORD
                        Oracle password (default: oracle)
  --table TABLE, -t TABLE
                        Oracle database table name for IT. (default:
                        conv_it_test)
  --excel EXCEL, -x EXCEL
                        Path to Excel data file. (default: ./data.xlsx)
```

### otgen.py
```
usage: otgen.py [-h] [--broker BROKER] [--topic TOPIC] [--excel EXCEL]
                [--group GROUP]

optional arguments:
  -h, --help            show this help message and exit
  --broker BROKER, -b BROKER
                        Kafka broker IP:PORT. (default: 10.1.5.45:9094)
  --topic TOPIC, -t TOPIC
                        OT topic. (default: conv_function_test)
  --excel EXCEL, -x EXCEL
                        Path to Excel data file. (default: ./data.xlsx)
  --group GROUP, -G GROUP
                        Path to OT sensor group file. (default:
                        ./sensor_grouping.json)
```

### apicheck.py
```
usage: apicheck.py [-h] [--host HOST] [--port PORT] [--excel EXCEL]

optional arguments:
  -h, --help            show this help message and exit
  --host HOST, -H HOST  API server host address. (default: 10.1.5.41)
  --port PORT, -p PORT  API server port number. (default: 80)
  --excel EXCEL, -x EXCEL
                        Path to Excel data file. (default: ./data.xlsx)
```

## Test Steps
1.  如果沒有安裝 [`virtualenv`](https://pypi.org/project/virtualenv/)
    ```bash
    sudo apt install -y virtualenv
    ```
1.  下載測試程式並建立 Python 虛擬環境
    ```bash
    git clone https://github.com/nexgus/convfunctest.git
    cd convfunctest
    virtualenv -p python3 .
    source bin/activate
    ```
1.  安裝 Python 相依模組
    ```bash
    pip install -r requirements.txt
    ```
1.  切換至工作目錄
    ```bash
    cd works
    ```
1.  建立資料集
    ```bash
    python datagen.py
    ```
1.  建立 IT/OT 資料
    ```bash
    python itgen.py
    python otgen.py
    ```
1.  檢查 API 結果
    ```bash
    python apicheck.py
    ```