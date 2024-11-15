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
HOST=
USER=
PASSWORD=
DATABASE=
...
```

## 3. Move import files 
format `tbl_privilege_txn_202XXX_XX.txt.gz` to folder `data/credit`


## 4. Run project
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
find . -name '__pycache__' -type d -exec rm -r {} +
```