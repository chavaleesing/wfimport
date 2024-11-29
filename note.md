## STEP TO RUN AFTER INSTALLED

1. activate venv (in folder wfimport-main)
```
source venv/bin/activate 
```

2. Download file to `data/credit`

3. Run python
```
python3 main.py
```

---

### STEP OF CODE

1. เริ่มจาก file main.py call ไปที่ function bulk_import ที่อยู่ใน file import_data.py
2. bulk_import จะ loop file ใน folder data/credit แล้วนำไป process ทีละไฟล
3. การ process แต่ละไฟลคือ
    
    3.1 แตก .gz file to .txt file (แตกเสร็จลบไฟล .gz ทิ้ง)
    
    3.2 นำ file ไป preprocess จัดบรรทัด -> result จะอยุ่ใน folder preprocessed (ทำเสร็จลบไฟล .txt ก่อน preprocessed ทิ้ง)

    3.3 นำ file ใน folder preprocessed ไปอ่าน แล้วแบ่ง insert เป็น chuck ตอนนี้ กำหนดไว้  20,000 (commit ทีละ 20,000)

    3.4 หลังจากทำจนครบทุก record ใน file จะทำการลบ file preprocessed ทิ้งไป



### การปรับเวลา validate end_time ก่อนการ process file ถัดไป

1. go to file `services/import_data.py`
2. หา function is_exceed_time `end_time = dtime(3, 30)` ตอนนี้ กำหนดเป็นตีสามครึ่ง ถ้าไฟล์มีแนวโน้ว process เร็วกว่า 1 ชั่วโมง สามารถปรับเวลาเป็นตีสี่แทนได้ โดยเปลี่ยนเป็น `end_time = dtime(4, 0)`

### การเพิ่ม reconcile
1. go to file `services/import_data.py`
2. หา keyword `os.getenv("IS_RECONCILE", 0)` แก้จากเลข 0 เป็นเลข 1 เพื่อเพิ่ม process ในการ reconcile 

NOTE :: การ reconcile ทำโดย เทียบจำนวนข้อมูลใน file กับ ข้อมูลที่ insert ลง table ไปแล้ว จะเป็นการ count data ทั้งหมดสองรอบ คือ ก่อน  process file และ หละง process file

### การปรับ chuck size
1. go to file `services/import_data.py`
2. หา keyword `batch_size = int(os.getenv("BATCH_SIZE", 20000))` แก้จากเลข 20000 เป็นเลข อื่นๆ


### การดู Noti on MS Team
กด run script แล้ว

1. noti แรก เมื่อกด run script
```
🆗[INFO][20ca7788] ⁍ ⁍ ⁍ ⁍ ⁍ Start import file(s) ⁌ ⁌ ⁌ ⁌ ⁌
```

`20ca7788` เป็น unique key ของการ run แต่ละครั้ง

เช่น ถ้าบน kcorp-monthly กำลัง import data 3 files 
noti ของการ process data นี้ จะมี unique key เดียวกันคือ `20ca7788`

และระหว่างที่ process file ข้างบนอยู่ ไป run ที่ kcorp-txn-monthly อีก 5 files Noti ของ kcorp-txn-monthly ตรง  unique key จะถูกสร้างขึ้นมาใหม่ ซึ่งจะแตกต่างกันออกไปจาก kcorp-monthly ทำให้รู้ว่า noti นี้เป็นของ kcorp-monthly หรือ kcorp-txn-monthly

2. noti ที่บอกว่า กำลังจะ import data เข้า DB จากไฟล
```
🆗[INFO][79b495b7] Importing data from file tbl_privilege_txn_202408_17.txt | Total records = 672478
```

3. noti ที่บอกว่า เลยเวลา end_time ที่กำหนดไว้แล้ว 
```
🆗[INFO][79b495b7] Exceed time process
```

4. noti ก่อนจบการรัน ที่จะบอกว่า ที่ผ่านมา รันไฟล์อะไรไปบ้าง

จะเกิดก็ต่อเมื่อ เลยเวลา end_time หรือ import data from folder data/credit ไปหมดแล้ว
```
🆗[INFO][79b495b7] Completed import file(s) ✅ processed_files = ['tbl_privilege_txn_202408_07.txt', 'tbl_privilege_txn_202408_01.txt', 'tbl_privilege_txn_202408_14.txt', 'tbl_privilege_txn_202408_09.txt', 'tbl_privilege_txn_202408_11.txt', 'tbl_privilege_txn_202408_04.txt', 'tbl_privilege_txn_202408_16.txt', 'tbl_privilege_txn_202408_05.txt', 'tbl_privilege_txn_202408_17.txt', 'tbl_privilege_txn_202408_15.txt']
```