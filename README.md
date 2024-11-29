# wfimport

# Setting Local env

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
...
```

## 3. Move import files 
#### 3.1 create folder 
```
mkdir data2 && cd data2 && mkdir campaign company credit customer payment
```
#### 3.2 prepare import files to folder 
format file must be `tbl_privilege_txn_202XXX_XX.txt.gz` to folder `data/<DB>`


## 4. Run project

update main.py on data folder to import,
then run
```
python3 main.py
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