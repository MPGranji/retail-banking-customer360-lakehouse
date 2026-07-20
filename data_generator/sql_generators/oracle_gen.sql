-- =============================================================
-- Oracle Core Banking — SQL Data Generator (manual-run version)
-- Tương thích: DBeaver, SQL Developer, SQL*Plus, sqlcl
--
-- ── Tham số (chỉnh trực tiếp tại đây trước khi chạy) ─────────
--   n_customers    = 10000
--   n_vip          = 500          (total * 0.05)
--   n_vip_plus_pri = 3000         (total * 0.30)
--   cob_dt         = 2025-12-31
--   start_dt       = 2025-01-01   (cob_dt - months_history * 30 days)
--   months_history = 12
--   txn_vip_total  = 360          (VIP_per_month  30 * months 12)
--   txn_pri_total  = 180          (PRI_per_month  15 * months 12)
--   txn_ret_total  = 60           (RET_per_month   5 * months 12)
--   max_txns       = 360          (max of the three above)
-- =============================================================

-- ── Truncate (child-first FK order) ──────────────────────────
TRUNCATE TABLE core_banking.txn_account;
TRUNCATE TABLE core_banking.deposit;
TRUNCATE TABLE core_banking.loan;
TRUNCATE TABLE core_banking.account;
TRUNCATE TABLE core_banking.customer;
TRUNCATE TABLE core_banking.product;
TRUNCATE TABLE core_banking.branch;
COMMIT;

-- ── Branch (50 rows) ─────────────────────────────────────────
INSERT ALL
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN001','Chi nhanh Ha Noi 1','NORTH','Ha Noi','Ba Dinh','So 18 Duong 8, Ba Dinh','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN002','Chi nhanh Ha Noi 2','NORTH','Ha Noi','Hoan Kiem','So 35 Duong 15, Hoan Kiem','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN003','Chi nhanh Ha Noi 3','NORTH','Ha Noi','Dong Da','So 52 Duong 22, Dong Da','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN004','Chi nhanh Ha Noi 4','NORTH','Ha Noi','Hai Ba Trung','So 69 Duong 29, Hai Ba Trung','Nguyen Van D',DATE '2015-05-25','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN005','Chi nhanh Ha Noi 5','NORTH','Ha Noi','Cau Giay','So 86 Duong 36, Cau Giay','Nguyen Van E',DATE '2015-06-10','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN006','Chi nhanh Ha Noi 6','NORTH','Ha Noi','Thanh Xuan','So 103 Duong 43, Thanh Xuan','Nguyen Van F',DATE '2015-07-23','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN007','Chi nhanh Ha Noi 7','NORTH','Ha Noi','Long Bien','So 120 Duong 50, Long Bien','Nguyen Van G',DATE '2015-08-08','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN008','Chi nhanh Ha Noi 8','NORTH','Ha Noi','Tay Ho','So 137 Duong 7, Tay Ho','Nguyen Van H',DATE '2015-09-21','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN009','Chi nhanh Ha Noi 9','NORTH','Ha Noi','Ba Dinh','So 154 Duong 14, Ba Dinh','Nguyen Van I',DATE '2015-10-06','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HAN010','Chi nhanh Ha Noi 10','NORTH','Ha Noi','Hoan Kiem','So 171 Duong 21, Hoan Kiem','Nguyen Van J',DATE '2015-11-19','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HPG001','Chi nhanh Hai Phong 1','NORTH','Hai Phong','Hong Bang','So 18 Duong 8, Hong Bang','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HPG002','Chi nhanh Hai Phong 2','NORTH','Hai Phong','Ngo Quyen','So 35 Duong 15, Ngo Quyen','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HPG003','Chi nhanh Hai Phong 3','NORTH','Hai Phong','Le Chan','So 52 Duong 22, Le Chan','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HPG004','Chi nhanh Hai Phong 4','NORTH','Hai Phong','Hai An','So 69 Duong 29, Hai An','Nguyen Van D',DATE '2015-05-25','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('QNI001','Chi nhanh Quang Ninh 1','NORTH','Quang Ninh','Ha Long','So 18 Duong 8, Ha Long','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('QNI002','Chi nhanh Quang Ninh 2','NORTH','Quang Ninh','Cam Pha','So 35 Duong 15, Cam Pha','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('NDH001','Chi nhanh Nam Dinh 1','NORTH','Nam Dinh','Nam Dinh','So 18 Duong 8, Nam Dinh','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DAN001','Chi nhanh Da Nang 1','CENTRAL','Da Nang','Hai Chau','So 18 Duong 8, Hai Chau','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DAN002','Chi nhanh Da Nang 2','CENTRAL','Da Nang','Thanh Khe','So 35 Duong 15, Thanh Khe','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DAN003','Chi nhanh Da Nang 3','CENTRAL','Da Nang','Son Tra','So 52 Duong 22, Son Tra','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DAN004','Chi nhanh Da Nang 4','CENTRAL','Da Nang','Ngu Hanh Son','So 69 Duong 29, Ngu Hanh Son','Nguyen Van D',DATE '2015-05-25','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DAN005','Chi nhanh Da Nang 5','CENTRAL','Da Nang','Lien Chieu','So 86 Duong 36, Lien Chieu','Nguyen Van E',DATE '2015-06-10','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HUE001','Chi nhanh Thua Thien Hue 1','CENTRAL','Thua Thien Hue','TP Hue','So 18 Duong 8, TP Hue','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HUE002','Chi nhanh Thua Thien Hue 2','CENTRAL','Thua Thien Hue','Phong Dien','So 35 Duong 15, Phong Dien','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HUE003','Chi nhanh Thua Thien Hue 3','CENTRAL','Thua Thien Hue','Phu Vang','So 52 Duong 22, Phu Vang','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('NAN001','Chi nhanh Nghe An 1','CENTRAL','Nghe An','Vinh','So 18 Duong 8, Vinh','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('NAN002','Chi nhanh Nghe An 2','CENTRAL','Nghe An','Cua Lo','So 35 Duong 15, Cua Lo','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('KHH001','Chi nhanh Khanh Hoa 1','CENTRAL','Khanh Hoa','Nha Trang','So 18 Duong 8, Nha Trang','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('KHH002','Chi nhanh Khanh Hoa 2','CENTRAL','Khanh Hoa','Cam Ranh','So 35 Duong 15, Cam Ranh','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('KHH003','Chi nhanh Khanh Hoa 3','CENTRAL','Khanh Hoa','Nha Trang','So 52 Duong 22, Nha Trang','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM001','Chi nhanh Ho Chi Minh 1','SOUTH','Ho Chi Minh','Quan 1','So 18 Duong 8, Quan 1','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM002','Chi nhanh Ho Chi Minh 2','SOUTH','Ho Chi Minh','Quan 3','So 35 Duong 15, Quan 3','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM003','Chi nhanh Ho Chi Minh 3','SOUTH','Ho Chi Minh','Quan 5','So 52 Duong 22, Quan 5','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM004','Chi nhanh Ho Chi Minh 4','SOUTH','Ho Chi Minh','Quan 7','So 69 Duong 29, Quan 7','Nguyen Van D',DATE '2015-05-25','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM005','Chi nhanh Ho Chi Minh 5','SOUTH','Ho Chi Minh','Binh Thanh','So 86 Duong 36, Binh Thanh','Nguyen Van E',DATE '2015-06-10','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM006','Chi nhanh Ho Chi Minh 6','SOUTH','Ho Chi Minh','Phu Nhuan','So 103 Duong 43, Phu Nhuan','Nguyen Van F',DATE '2015-07-23','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM007','Chi nhanh Ho Chi Minh 7','SOUTH','Ho Chi Minh','Go Vap','So 120 Duong 50, Go Vap','Nguyen Van G',DATE '2015-08-08','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM008','Chi nhanh Ho Chi Minh 8','SOUTH','Ho Chi Minh','Thu Duc','So 137 Duong 7, Thu Duc','Nguyen Van H',DATE '2015-09-21','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM009','Chi nhanh Ho Chi Minh 9','SOUTH','Ho Chi Minh','Binh Chanh','So 154 Duong 14, Binh Chanh','Nguyen Van I',DATE '2015-10-06','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM010','Chi nhanh Ho Chi Minh 10','SOUTH','Ho Chi Minh','Tan Binh','So 171 Duong 21, Tan Binh','Nguyen Van J',DATE '2015-11-19','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM011','Chi nhanh Ho Chi Minh 11','SOUTH','Ho Chi Minh','Quan 1','So 188 Duong 28, Quan 1','Nguyen Van K',DATE '2015-12-04','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('HCM012','Chi nhanh Ho Chi Minh 12','SOUTH','Ho Chi Minh','Quan 3','So 205 Duong 35, Quan 3','Nguyen Van L',DATE '2015-01-17','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('BDG001','Chi nhanh Binh Duong 1','SOUTH','Binh Duong','Thu Dau Mot','So 18 Duong 8, Thu Dau Mot','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('BDG002','Chi nhanh Binh Duong 2','SOUTH','Binh Duong','Di An','So 35 Duong 15, Di An','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('BDG003','Chi nhanh Binh Duong 3','SOUTH','Binh Duong','Thuan An','So 52 Duong 22, Thuan An','Nguyen Van C',DATE '2015-04-12','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('BDG004','Chi nhanh Binh Duong 4','SOUTH','Binh Duong','Ben Cat','So 69 Duong 29, Ben Cat','Nguyen Van D',DATE '2015-05-25','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DNI001','Chi nhanh Dong Nai 1','SOUTH','Dong Nai','Bien Hoa','So 18 Duong 8, Bien Hoa','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('DNI002','Chi nhanh Dong Nai 2','SOUTH','Dong Nai','Long Khanh','So 35 Duong 15, Long Khanh','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('CTH001','Chi nhanh Can Tho 1','SOUTH','Can Tho','Ninh Kieu','So 18 Duong 8, Ninh Kieu','Nguyen Van A',DATE '2015-02-14','ACTIVE',SYSTIMESTAMP)
  INTO core_banking.branch (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated) VALUES ('CTH002','Chi nhanh Can Tho 2','SOUTH','Can Tho','Binh Thuy','So 35 Duong 15, Binh Thuy','Nguyen Van B',DATE '2015-03-27','ACTIVE',SYSTIMESTAMP)
SELECT 1 FROM DUAL;
COMMIT;

-- ── Product (16 rows) ─────────────────────────────────────────
INSERT ALL
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('CASA001','Tai khoan thanh toan VND','DEPOSIT','CASA','VND',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('CASA002','Tai khoan thanh toan USD','DEPOSIT','CASA','USD',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('SAVE001','Tiet kiem 1 thang','DEPOSIT','SAVINGS','VND',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('SAVE003','Tiet kiem 3 thang','DEPOSIT','SAVINGS','VND',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('SAVE006','Tiet kiem 6 thang','DEPOSIT','SAVINGS','VND',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('SAVE012','Tiet kiem 12 thang','DEPOSIT','SAVINGS','VND',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('SAVE024','Tiet kiem 24 thang','DEPOSIT','SAVINGS','VND',1,DATE '2016-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('SAVE036','Tiet kiem 36 thang','DEPOSIT','SAVINGS','VND',1,DATE '2016-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('LOAN001','Vay tieu dung ca nhan','LOAN','PERSONAL_LOAN','VND',1,DATE '2015-06-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('LOAN002','Vay mua nha o','LOAN','MORTGAGE','VND',1,DATE '2015-06-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('LOAN003','Vay kinh doanh nho','LOAN','PERSONAL_LOAN','VND',1,DATE '2016-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('CC_VISA','The tin dung Visa Classic','CARD','CREDIT_CARD','VND',1,DATE '2016-03-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('CC_MAST','The tin dung Mastercard','CARD','CREDIT_CARD','VND',1,DATE '2016-03-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('CC_GOLD','The tin dung Visa Gold','CARD','CREDIT_CARD','VND',1,DATE '2018-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('DC_NAPS','The ghi no Napas','CARD','DEBIT_CARD','VND',1,DATE '2015-01-01',TIMESTAMP '2024-01-01 00:00:00')
  INTO core_banking.product (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated) VALUES ('DC_VISA','The ghi no Visa Debit','CARD','DEBIT_CARD','VND',1,DATE '2017-06-01',TIMESTAMP '2024-01-01 00:00:00')
SELECT 1 FROM DUAL;
COMMIT;

-- ── Customer (10000 rows) ─────────────────────────────────────
INSERT INTO core_banking.customer (
  customer_id, cccd, full_name, gender, date_of_birth,
  phone, email, address, city, district, branch_code,
  customer_segment, kyc_status, register_date, is_active, last_updated
)
WITH
  branches AS (
    SELECT branch_code, ROW_NUMBER() OVER (ORDER BY branch_code) rn
    FROM core_banking.branch
  ),
  gen AS (SELECT LEVEL AS lvl FROM DUAL CONNECT BY LEVEL <= 10000)
SELECT
  10000 + g.lvl                                                              AS customer_id,
  LPAD(TO_CHAR(g.lvl), 12, '0')                                             AS cccd,
  -- Ho (16 options, cycles every 16)
  CASE MOD(g.lvl, 16)
    WHEN 0  THEN 'Nguyen' WHEN 1  THEN 'Tran'  WHEN 2  THEN 'Le'
    WHEN 3  THEN 'Pham'   WHEN 4  THEN 'Hoang' WHEN 5  THEN 'Huynh'
    WHEN 6  THEN 'Phan'   WHEN 7  THEN 'Vu'    WHEN 8  THEN 'Dang'
    WHEN 9  THEN 'Bui'    WHEN 10 THEN 'Do'    WHEN 11 THEN 'Ho'
    WHEN 12 THEN 'Ngo'    WHEN 13 THEN 'Duong' WHEN 14 THEN 'Ly'
    ELSE 'Dinh'
  END
  || ' ' ||
  -- Ten dem (10 options per gender, cycles every 16*10=160)
  CASE WHEN MOD(g.lvl, 3) = 0 THEN
    CASE MOD(TRUNC(g.lvl / 16), 10)
      WHEN 0 THEN 'Thi'   WHEN 1 THEN 'Thuy'  WHEN 2 THEN 'Ngoc'
      WHEN 3 THEN 'Lan'   WHEN 4 THEN 'Thanh' WHEN 5 THEN 'Kim'
      WHEN 6 THEN 'Thu'   WHEN 7 THEN 'Huong' WHEN 8 THEN 'Bich'
      ELSE 'My'
    END
  ELSE
    CASE MOD(TRUNC(g.lvl / 16), 10)
      WHEN 0 THEN 'Van'   WHEN 1 THEN 'Huu'   WHEN 2 THEN 'Duc'
      WHEN 3 THEN 'Minh'  WHEN 4 THEN 'Quoc'  WHEN 5 THEN 'Thanh'
      WHEN 6 THEN 'Cong'  WHEN 7 THEN 'Trong' WHEN 8 THEN 'Anh'
      ELSE 'Trung'
    END
  END
  || ' ' ||
  -- Ten chinh (20 options per gender, cycles every 160*20=3200)
  CASE WHEN MOD(g.lvl, 3) = 0 THEN
    CASE MOD(TRUNC(g.lvl / 160), 20)
      WHEN 0  THEN 'Anh'    WHEN 1  THEN 'Chi'    WHEN 2  THEN 'Dung'
      WHEN 3  THEN 'Ha'     WHEN 4  THEN 'Hoa'    WHEN 5  THEN 'Lan'
      WHEN 6  THEN 'Linh'   WHEN 7  THEN 'Mai'    WHEN 8  THEN 'Ngan'
      WHEN 9  THEN 'Nhung'  WHEN 10 THEN 'Phuong' WHEN 11 THEN 'Quynh'
      WHEN 12 THEN 'Trang'  WHEN 13 THEN 'Trinh'  WHEN 14 THEN 'Van'
      WHEN 15 THEN 'Yen'    WHEN 16 THEN 'Ngoc'   WHEN 17 THEN 'Thu'
      WHEN 18 THEN 'Hang'   ELSE 'Dieu'
    END
  ELSE
    CASE MOD(TRUNC(g.lvl / 160), 20)
      WHEN 0  THEN 'An'    WHEN 1  THEN 'Binh'  WHEN 2  THEN 'Cuong'
      WHEN 3  THEN 'Dung'  WHEN 4  THEN 'Hung'  WHEN 5  THEN 'Khoa'
      WHEN 6  THEN 'Long'  WHEN 7  THEN 'Minh'  WHEN 8  THEN 'Nam'
      WHEN 9  THEN 'Phong' WHEN 10 THEN 'Quan'  WHEN 11 THEN 'Son'
      WHEN 12 THEN 'Thanh' WHEN 13 THEN 'Tung'  WHEN 14 THEN 'Viet'
      WHEN 15 THEN 'Huy'   WHEN 16 THEN 'Tuan'  WHEN 17 THEN 'Dat'
      WHEN 18 THEN 'Khoi'  ELSE 'Tai'
    END
  END                                                                        AS full_name,
  CASE WHEN MOD(g.lvl, 3) = 0 THEN 'F' ELSE 'M' END                        AS gender,
  DATE '1960-01-01' + MOD(g.lvl * 137, 365 * 38)                           AS date_of_birth,
  '09' || LPAD(TO_CHAR(MOD(g.lvl * 9973, 100000000)), 8, '0')              AS phone,
  -- Email: ho_lowercase + 4-digit number (pseudo-random) + domain
  LOWER(
    CASE MOD(g.lvl, 16)
      WHEN 0  THEN 'nguyen' WHEN 1  THEN 'tran'  WHEN 2  THEN 'le'
      WHEN 3  THEN 'pham'   WHEN 4  THEN 'hoang' WHEN 5  THEN 'huynh'
      WHEN 6  THEN 'phan'   WHEN 7  THEN 'vu'    WHEN 8  THEN 'dang'
      WHEN 9  THEN 'bui'    WHEN 10 THEN 'do'    WHEN 11 THEN 'ho'
      WHEN 12 THEN 'ngo'    WHEN 13 THEN 'duong' WHEN 14 THEN 'ly'
      ELSE 'dinh'
    END
    || TO_CHAR(MOD(g.lvl * 97, 9000) + 1000)
    || '@'
    || CASE MOD(g.lvl, 5)
         WHEN 0 THEN 'gmail.com'
         WHEN 1 THEN 'yahoo.com'
         WHEN 2 THEN 'outlook.com'
         WHEN 3 THEN 'hotmail.com'
         ELSE        'icloud.com'
       END
  )                                                                          AS email,
  -- Address: So X + ten duong pho (15 ten pho pho bien)
  'So ' || (MOD(g.lvl * 17, 299) + 1) || ' ' ||
  CASE MOD(g.lvl, 15)
    WHEN 0  THEN 'Nguyen Hue'        WHEN 1  THEN 'Le Loi'
    WHEN 2  THEN 'Tran Hung Dao'     WHEN 3  THEN 'Hai Ba Trung'
    WHEN 4  THEN 'Le Duan'           WHEN 5  THEN 'Nguyen Du'
    WHEN 6  THEN 'Phan Chu Trinh'    WHEN 7  THEN 'Hoang Dieu'
    WHEN 8  THEN 'Quang Trung'       WHEN 9  THEN 'Ly Thuong Kiet'
    WHEN 10 THEN 'Dien Bien Phu'     WHEN 11 THEN 'Nam Ky Khoi Nghia'
    WHEN 12 THEN 'Dong Khoi'         WHEN 13 THEN 'Pasteur'
    ELSE         'Vo Van Tan'
  END                                                                        AS address,
  CASE MOD(g.lvl, 3)
    WHEN 0 THEN 'Ha Noi'
    WHEN 1 THEN 'Ho Chi Minh'
    ELSE        'Da Nang'
  END                                                                        AS city,
  CASE MOD(g.lvl, 5)
    WHEN 0 THEN 'Quan 1'   WHEN 1 THEN 'Ba Dinh'
    WHEN 2 THEN 'Hai Chau' WHEN 3 THEN 'Ninh Kieu'
    ELSE        'Quan 7'
  END                                                                        AS district,
  b.branch_code,
  CASE
    WHEN g.lvl <= 500  THEN 'VIP'
    WHEN g.lvl <= 3000 THEN 'PRIORITY'
    ELSE                    'RETAIL'
  END                                                                        AS customer_segment,
  CASE
    WHEN MOD(g.lvl, 100) < 90 THEN 'VERIFIED'
    WHEN MOD(g.lvl, 100) < 98 THEN 'PENDING'
    ELSE                            'REJECTED'
  END                                                                        AS kyc_status,
  DATE '2025-12-31' - (MOD(g.lvl * 37, 365 * 6) + 365)                    AS register_date,
  CASE WHEN MOD(g.lvl, 33) = 0 THEN 0 ELSE 1 END                          AS is_active,
  SYSTIMESTAMP - MOD(g.lvl, 365) / 86400                                   AS last_updated
FROM gen g
JOIN branches b ON b.rn = MOD(g.lvl - 1, 50) + 1;
COMMIT;

-- ── Account: CASA (1 per customer) ───────────────────────────
INSERT INTO core_banking.account (
  account_id, account_no, customer_id, product_code, branch_code,
  account_type, currency, balance, open_date, close_date, status, last_updated
)
WITH custs AS (
  SELECT customer_id, customer_segment, register_date, branch_code,
         ROW_NUMBER() OVER (ORDER BY customer_id) rn
  FROM core_banking.customer
)
SELECT
  100000 + c.rn                                                              AS account_id,
  '090' || LPAD(TO_CHAR(100000 + c.rn), 10, '0')                           AS account_no,
  c.customer_id,
  CASE MOD(c.rn, 2) WHEN 0 THEN 'CASA001' ELSE 'CASA002' END               AS product_code,
  c.branch_code,
  'CASA'                                                                     AS account_type,
  'VND'                                                                      AS currency,
  ROUND(
    CASE c.customer_segment
      WHEN 'VIP'      THEN DBMS_RANDOM.VALUE(500000000,  5000000000)
      WHEN 'PRIORITY' THEN DBMS_RANDOM.VALUE(50000000,    500000000)
      ELSE                 DBMS_RANDOM.VALUE(5000000,      80000000)
    END, -3)                                                                 AS balance,
  c.register_date + MOD(c.rn * 7, 30)                                       AS open_date,
  NULL                                                                       AS close_date,
  CASE
    WHEN MOD(c.rn, 100) < 95 THEN 'ACTIVE'
    WHEN MOD(c.rn, 100) < 98 THEN 'FROZEN'
    ELSE                           'CLOSED'
  END                                                                        AS status,
  SYSDATE - MOD(c.rn, 90)                                                   AS last_updated
FROM custs c;
COMMIT;

-- ── Account: TIME_DEPOSIT (~35% of customers) ────────────────
INSERT INTO core_banking.account (
  account_id, account_no, customer_id, product_code, branch_code,
  account_type, currency, balance, open_date, close_date, status, last_updated
)
WITH custs AS (
  SELECT customer_id, customer_segment, register_date, branch_code,
         ROW_NUMBER() OVER (ORDER BY customer_id) rn
  FROM core_banking.customer
  WHERE MOD(customer_id, 100) < 35
)
SELECT
  200000 + c.rn                                                              AS account_id,
  '091' || LPAD(TO_CHAR(200000 + c.rn), 10, '0')                           AS account_no,
  c.customer_id,
  CASE MOD(c.rn, 6)
    WHEN 0 THEN 'SAVE001' WHEN 1 THEN 'SAVE003' WHEN 2 THEN 'SAVE006'
    WHEN 3 THEN 'SAVE012' WHEN 4 THEN 'SAVE024' ELSE    'SAVE036'
  END                                                                        AS product_code,
  c.branch_code,
  'TIME_DEPOSIT'                                                             AS account_type,
  'VND'                                                                      AS currency,
  ROUND(
    CASE c.customer_segment
      WHEN 'VIP'      THEN DBMS_RANDOM.VALUE(250000000,  4000000000)
      WHEN 'PRIORITY' THEN DBMS_RANDOM.VALUE(25000000,    400000000)
      ELSE                 DBMS_RANDOM.VALUE(2500000,      64000000)
    END, -3)                                                                 AS balance,
  c.register_date + 60 + MOD(c.rn * 13, 305)                               AS open_date,
  NULL                                                                       AS close_date,
  'ACTIVE'                                                                   AS status,
  SYSDATE - MOD(c.rn, 90)                                                   AS last_updated
FROM custs c;
COMMIT;

-- ── Deposit (one per TIME_DEPOSIT account) ────────────────────
INSERT INTO core_banking.deposit (
  deposit_id, account_id, customer_id, product_code,
  principal_amount, interest_rate, term_months,
  open_date, maturity_date, status, last_updated
)
WITH td AS (
  SELECT account_id, customer_id, balance, open_date,
         ROW_NUMBER() OVER (ORDER BY account_id) rn
  FROM core_banking.account
  WHERE account_type = 'TIME_DEPOSIT'
)
SELECT
  ROW_NUMBER() OVER (ORDER BY t.rn)                                         AS deposit_id,
  t.account_id,
  t.customer_id,
  CASE MOD(t.rn, 6)
    WHEN 0 THEN 'SAVE001' WHEN 1 THEN 'SAVE003' WHEN 2 THEN 'SAVE006'
    WHEN 3 THEN 'SAVE012' WHEN 4 THEN 'SAVE024' ELSE    'SAVE036'
  END                                                                        AS product_code,
  t.balance                                                                  AS principal_amount,
  CASE MOD(t.rn, 6)
    WHEN 0 THEN 3.5 WHEN 1 THEN 4.0 WHEN 2 THEN 5.0
    WHEN 3 THEN 6.5 WHEN 4 THEN 7.0 ELSE    7.5
  END                                                                        AS interest_rate,
  CASE MOD(t.rn, 6)
    WHEN 0 THEN 1 WHEN 1 THEN 3  WHEN 2 THEN 6
    WHEN 3 THEN 12 WHEN 4 THEN 24 ELSE   36
  END                                                                        AS term_months,
  t.open_date,
  ADD_MONTHS(t.open_date,
    CASE MOD(t.rn, 6)
      WHEN 0 THEN 1 WHEN 1 THEN 3  WHEN 2 THEN 6
      WHEN 3 THEN 12 WHEN 4 THEN 24 ELSE   36
    END)                                                                     AS maturity_date,
  CASE
    WHEN MOD(t.rn, 10) < 7 THEN 'ACTIVE'
    WHEN MOD(t.rn, 10) < 9 THEN 'MATURED'
    ELSE                         'EARLY_WITHDRAWN'
  END                                                                        AS status,
  SYSTIMESTAMP - MOD(t.rn, 90) / 86400                                      AS last_updated
FROM td t;
COMMIT;

-- ── Loan (per customer, rate varies by segment) ───────────────
INSERT INTO core_banking.loan (
  loan_id, customer_id, product_code, branch_code,
  loan_amount, outstanding_balance, interest_rate, term_months,
  disbursement_date, maturity_date, loan_status, last_updated
)
WITH loan_base AS (
  SELECT
    c.customer_id,
    c.branch_code,
    c.customer_segment,
    c.register_date,
    ROW_NUMBER() OVER (ORDER BY c.customer_id) rn,
    ROUND(
      CASE c.customer_segment
        WHEN 'VIP'      THEN DBMS_RANDOM.VALUE(100000000,  5000000000)
        WHEN 'PRIORITY' THEN DBMS_RANDOM.VALUE(20000000,    500000000)
        ELSE                 DBMS_RANDOM.VALUE(5000000,     100000000)
      END, -6)                                                               AS loan_amount
  FROM core_banking.customer c
  WHERE MOD(c.customer_id, 100) <
    CASE c.customer_segment
      WHEN 'VIP'      THEN 60
      WHEN 'PRIORITY' THEN 40
      ELSE                 20
    END
)
SELECT
  ROW_NUMBER() OVER (ORDER BY lb.rn)                                        AS loan_id,
  lb.customer_id,
  CASE MOD(lb.rn, 3)
    WHEN 0 THEN 'LOAN001' WHEN 1 THEN 'LOAN002' ELSE 'LOAN003'
  END                                                                        AS product_code,
  lb.branch_code,
  lb.loan_amount,
  ROUND(lb.loan_amount * DBMS_RANDOM.VALUE(0.05, 0.95), -6)                AS outstanding_balance,
  CASE MOD(lb.rn, 3)
    WHEN 0 THEN 8.5 WHEN 1 THEN 9.0 ELSE 10.5
  END                                                                        AS interest_rate,
  CASE MOD(lb.rn, 4)
    WHEN 0 THEN 12 WHEN 1 THEN 24 WHEN 2 THEN 36 ELSE 60
  END                                                                        AS term_months,
  lb.register_date + MOD(lb.rn * 29, 365)                                  AS disbursement_date,
  ADD_MONTHS(lb.register_date + MOD(lb.rn * 29, 365),
    CASE MOD(lb.rn, 4) WHEN 0 THEN 12 WHEN 1 THEN 24 WHEN 2 THEN 36 ELSE 60 END)
                                                                             AS maturity_date,
  CASE
    WHEN MOD(lb.rn, 100) < 70 THEN 'ACTIVE'
    WHEN MOD(lb.rn, 100) < 85 THEN 'CLOSED'
    WHEN MOD(lb.rn, 100) < 95 THEN 'OVERDUE'
    ELSE                            'WRITTEN_OFF'
  END                                                                        AS loan_status,
  SYSTIMESTAMP - MOD(lb.rn, 90) / 86400                                     AS last_updated
FROM loan_base lb;
COMMIT;

-- ── TXN_ACCOUNT (variable rows per CASA account by segment) ───
-- txn_vip=360  txn_pri=180  txn_ret=60  max_txns=360
-- start_dt=2025-01-01  months_history=12
INSERT INTO core_banking.txn_account (
  txn_id, account_id, customer_id, txn_date, txn_amount,
  txn_type, debit_credit, balance_after, channel,
  description, counter_account, created_ts, last_updated
)
WITH
  casa AS (
    SELECT
      a.account_id,
      a.customer_id,
      CASE c.customer_segment
        WHEN 'VIP'      THEN 360
        WHEN 'PRIORITY' THEN 180
        ELSE                  60
      END AS n_txns
    FROM core_banking.account a
    JOIN core_banking.customer c ON a.customer_id = c.customer_id
    WHERE a.account_type = 'CASA'
  ),
  gen AS (SELECT LEVEL AS g FROM DUAL CONNECT BY LEVEL <= 360),
  raw_txns AS (
    SELECT
      ca.account_id,
      ca.customer_id,
      gen.g,
      TO_DATE('2025-01-01', 'YYYY-MM-DD') + DBMS_RANDOM.VALUE(0, 365) AS txn_date,
      ROUND(DBMS_RANDOM.VALUE(10000, 50000000), -3)                        AS txn_amount,
      ROUND(DBMS_RANDOM.VALUE(0, 100000000), -3)                           AS balance_after
    FROM casa ca, gen
    WHERE gen.g <= ca.n_txns
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY r.account_id, r.g)                          AS txn_id,
  r.account_id,
  r.customer_id,
  r.txn_date,
  r.txn_amount,
  CASE MOD(r.account_id + r.g, 6)
    WHEN 0 THEN 'DEPOSIT'      WHEN 1 THEN 'WITHDRAWAL'
    WHEN 2 THEN 'TRANSFER_IN'  WHEN 3 THEN 'TRANSFER_OUT'
    WHEN 4 THEN 'FEE'          ELSE        'INTEREST'
  END                                                                       AS txn_type,
  CASE MOD(r.account_id + r.g, 2) WHEN 0 THEN 'D' ELSE 'C' END          AS debit_credit,
  r.balance_after,
  CASE MOD(r.account_id * 3 + r.g, 5)
    WHEN 0 THEN 'BRANCH'           WHEN 1 THEN 'ATM'
    WHEN 2 THEN 'INTERNET_BANKING' WHEN 3 THEN 'MOBILE_BANKING'
    ELSE        'POS'
  END                                                                       AS channel,
  'GD-' || r.account_id || '-' || r.g                                     AS description,
  NULL                                                                      AS counter_account,
  CAST(r.txn_date AS TIMESTAMP)                                            AS created_ts,
  CAST(r.txn_date AS TIMESTAMP)                                            AS last_updated
FROM raw_txns r;
COMMIT;
