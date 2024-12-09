# wfimport

# Pre-install
```
kubectl get pods -n newwelfare-batch-report
```

เอาตัว monthly-report และ monthly-txn-report มา

login to pod:
```
kubectl exec -it <pod> -n newwelfare-batch-report bash
```

prepare:
```
mkdir import-datainno // สร้างโฟลเด้อเตรียมไว้

apt update

apt-get update

apt install python3

apt-get install unzip

apt install vim

apt-get -y install python3-pip
```


install gcp sdk:
```
apt-get install apt-transport-https ca-certificates gnupg curl

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

apt-get update

apt-get install google-cloud-sdk

create key // เอา key จาก prd มาใส่

touch key.json

vim key.json

gcloud auth activate-service-account --key-file key.json

exit
```


copy file from local to pod // set ENV ให้เสร็จก่อน ใน local:
```
kubectl cp <source> -n newwelfare-batch-report <pod>:import-datainno
```


login to pod:
```
kubectl exec -it <pod> -n newwelfare-batch-report bash

cd import-datainno

unzip wfimport.zip

cd wfimport

mkdir -p data && cd data && mkdir -p campaign company credit customer payment && cd ..

source venv/bin/activate // ได้ (venv)

pip3 install -r requirements.txt
```


cd data/credit เข้าไปใน folder ที่จะยัดไฟล์เข้า (ตามชื่อ DB)

copy  cmd จาก bucket ได้เลยย
```
gsutil -m cp \   "gs://newwelfare_batch_uat/batch/welfare/datainno/tbl_companies_20241112_00.txt.gz" \   "gs://newwelfare_batch_uat/batch/welfare/datainno/tbl_companies_audit_20241112_00.txt.gz" \   .
```


Execute python script
```
python3 main.py data/<folder_name> // สำหรับการ import data เข้า temp table
python3 main.py update // สำหรับการ update data เข้า main table (list table ไป set ใน .env file)
```





# Setting project

## 1. Setting venv

For Mac:
```
 python3 -m venv venv
 source venv/bin/activate 
 pip3 install -r requirements.txt
```


## 2. Update .env file values
```
DB_HOST=
DB_USER=
DB_PASSWORD=
DATABASE=

IS_ALERT=1
IS_VALIDATE_TIME=1

UPDATE_TABLES="tbl_companies_audit,tbl_company_accounts,tbl_companies,tbl_company_accounts_audit"
...
```

## 3. Move import files 
#### 3.1 create folder 
```
mkdir -p data && cd data && mkdir -p campaign company credit customer payment && cd .. 
```
#### 3.2 prepare import files to folder 
format file must be `tbl_privilege_txn_202XXX_XX.txt.gz` to folder `data/<DB>`


## 4. Run project

update main.py on data folder to import,
then run
`python3 main.py data/<folder_name>` OR `python3 main.py update`
```
python3 main.py data/<folder_name> // สำหรับการ import data เข้า temp table
python3 main.py update // สำหรับการ update data เข้า main table (list table ไป set ใน .env file)
```

---

## [For Dev] To Add some lib to project pls. run this cmd
### To generate requirements.txt
```
pip3 freeze > requirements.txt
```

### To remove cache
```
find . -name '__pycache__' -type d -exec rm -r {} + &&
find . -name ".DS_Store" -delete
```


