# Convergence Functional Test

## Prerequisties
1.  MongoDB server, 必須有一個 database 叫做 `nexmasa`, 且 `nexmasa` 內有兩個 collections 分別為 `dcts` 與 `sensormap`
1.  Python 3.6 或更新的版本

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