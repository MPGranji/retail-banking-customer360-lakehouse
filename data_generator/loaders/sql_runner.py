"""
SQL-mode data generator.

Generates fake data by executing SQL directly in Oracle (core_banking) and
PostgreSQL (card_crm), bypassing Python-side data generation entirely.
Much faster for large volumes since no Python memory is used.

Data quality trade-offs vs Python mode:
  - No Faker library: names are 'Khach Hang N', emails are 'khN@bank.vn'
  - Randomness is deterministic per DB engine seed but not reproducible
    across Oracle/PostgreSQL reseeds (unlike Python's seeded random.Random)
  - FK relationships are maintained; uniqueness ensured via modular arithmetic
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from string import Template

_SQL_DIR = Path(__file__).parent.parent / "sql_generators"

# Mirrors generators/branch.py _CITY_CONFIG
_CITY_CONFIG = [
    ("NORTH",   "Ha Noi",         "HAN", ["Ba Dinh", "Hoan Kiem", "Dong Da", "Hai Ba Trung", "Cau Giay", "Thanh Xuan", "Long Bien", "Tay Ho"], 10),
    ("NORTH",   "Hai Phong",      "HPG", ["Hong Bang", "Ngo Quyen", "Le Chan", "Hai An"], 4),
    ("NORTH",   "Quang Ninh",     "QNI", ["Ha Long", "Cam Pha", "Uong Bi"], 2),
    ("NORTH",   "Nam Dinh",       "NDH", ["Nam Dinh", "My Loc"], 1),
    ("CENTRAL", "Da Nang",        "DAN", ["Hai Chau", "Thanh Khe", "Son Tra", "Ngu Hanh Son", "Lien Chieu"], 5),
    ("CENTRAL", "Thua Thien Hue", "HUE", ["TP Hue", "Phong Dien", "Phu Vang"], 3),
    ("CENTRAL", "Nghe An",        "NAN", ["Vinh", "Cua Lo"], 2),
    ("CENTRAL", "Khanh Hoa",      "KHH", ["Nha Trang", "Cam Ranh"], 3),
    ("SOUTH",   "Ho Chi Minh",    "HCM", ["Quan 1", "Quan 3", "Quan 5", "Quan 7", "Binh Thanh", "Phu Nhuan", "Go Vap", "Thu Duc", "Binh Chanh", "Tan Binh"], 12),
    ("SOUTH",   "Binh Duong",     "BDG", ["Thu Dau Mot", "Di An", "Thuan An", "Ben Cat"], 4),
    ("SOUTH",   "Dong Nai",       "DNI", ["Bien Hoa", "Long Khanh"], 2),
    ("SOUTH",   "Can Tho",        "CTH", ["Ninh Kieu", "Binh Thuy", "Cai Rang"], 2),
]

_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Mirrors generators/product.py
_PRODUCTS = [
    ("CASA001", "Tai khoan thanh toan VND", "DEPOSIT", "CASA",          "VND", 1, "2015-01-01"),
    ("CASA002", "Tai khoan thanh toan USD", "DEPOSIT", "CASA",          "USD", 1, "2015-01-01"),
    ("SAVE001", "Tiet kiem 1 thang",        "DEPOSIT", "SAVINGS",       "VND", 1, "2015-01-01"),
    ("SAVE003", "Tiet kiem 3 thang",        "DEPOSIT", "SAVINGS",       "VND", 1, "2015-01-01"),
    ("SAVE006", "Tiet kiem 6 thang",        "DEPOSIT", "SAVINGS",       "VND", 1, "2015-01-01"),
    ("SAVE012", "Tiet kiem 12 thang",       "DEPOSIT", "SAVINGS",       "VND", 1, "2015-01-01"),
    ("SAVE024", "Tiet kiem 24 thang",       "DEPOSIT", "SAVINGS",       "VND", 1, "2016-01-01"),
    ("SAVE036", "Tiet kiem 36 thang",       "DEPOSIT", "SAVINGS",       "VND", 1, "2016-01-01"),
    ("LOAN001", "Vay tieu dung ca nhan",    "LOAN",    "PERSONAL_LOAN", "VND", 1, "2015-06-01"),
    ("LOAN002", "Vay mua nha o",            "LOAN",    "MORTGAGE",      "VND", 1, "2015-06-01"),
    ("LOAN003", "Vay kinh doanh nho",       "LOAN",    "PERSONAL_LOAN", "VND", 1, "2016-01-01"),
    ("CC_VISA", "The tin dung Visa Classic", "CARD",   "CREDIT_CARD",   "VND", 1, "2016-03-01"),
    ("CC_MAST", "The tin dung Mastercard",   "CARD",   "CREDIT_CARD",   "VND", 1, "2016-03-01"),
    ("CC_GOLD", "The tin dung Visa Gold",    "CARD",   "CREDIT_CARD",   "VND", 1, "2018-01-01"),
    ("DC_NAPS", "The ghi no Napas",          "CARD",   "DEBIT_CARD",    "VND", 1, "2015-01-01"),
    ("DC_VISA", "The ghi no Visa Debit",     "CARD",   "DEBIT_CARD",    "VND", 1, "2017-06-01"),
]


def _ora_branch_insert_all() -> str:
    lines = []
    for region, city, prefix, districts, count in _CITY_CONFIG:
        for i in range(1, count + 1):
            district = districts[(i - 1) % len(districts)]
            code     = f"{prefix}{i:03d}"
            name     = f"Chi nhanh {city} {i}"
            address  = f"So {(i * 17) % 999 + 1} Duong {(i * 7) % 50 + 1}, {district}"
            mgr      = f"Nguyen Van {_ALPHABET[(i - 1) % 26]}"
            month    = (i % 12) + 1
            day      = (i * 13 % 28) + 1
            lines.append(
                f"  INTO core_banking.branch"
                f" (branch_code,branch_name,region,city,district,address,manager_name,open_date,status,last_updated)"
                f" VALUES ('{code}','{name}','{region}','{city}','{district}',"
                f"'{address}','{mgr}',DATE '2015-{month:02d}-{day:02d}','ACTIVE',SYSTIMESTAMP)"
            )
    return "INSERT ALL\n" + "\n".join(lines) + "\nSELECT 1 FROM DUAL"


def _ora_product_insert_all() -> str:
    lines = []
    for code, name, grp, typ, cur, active, launch in _PRODUCTS:
        lines.append(
            f"  INTO core_banking.product"
            f" (product_code,product_name,product_group,product_type,currency,is_active,launch_date,last_updated)"
            f" VALUES ('{code}','{name}','{grp}','{typ}','{cur}',{active},"
            f"DATE '{launch}',TIMESTAMP '2024-01-01 00:00:00')"
        )
    return "INSERT ALL\n" + "\n".join(lines) + "\nSELECT 1 FROM DUAL"


def build_params(cfg: dict, cob: date) -> dict:
    n    = cfg["customers"]["total"]
    dist = cfg["customers"]["segment_distribution"]
    n_vip      = int(n * dist["VIP"])
    n_priority = int(n * dist["PRIORITY"])
    n_retail   = n - n_vip - n_priority
    months     = cfg["transactions"]["months_history"]
    start_dt   = cob - timedelta(days=months * 30)

    txn = cfg["transactions"]["txn_account_per_month"]
    cc  = cfg["transactions"]["card_txn_per_month"]
    cc_ratio = cfg["customers"]["credit_card_ratio"]
    crm_total = cfg["transactions"].get(
        "crm_total",
        cfg["transactions"].get("crm_per_month", 300) * months,
    )

    txn_v = txn["VIP"]      * months
    txn_p = txn["PRIORITY"] * months
    txn_r = txn["RETAIL"]   * months
    cc_v  = cc["VIP"]       * months
    cc_p  = cc["PRIORITY"]  * months
    cc_r  = cc["RETAIL"]    * months

    seed = cfg.get("seed", 42)

    return dict(
        seed             = seed,
        seed_float       = round(seed / 1000.0, 6),   # setseed() takes -1..1
        n_customers      = n,
        n_vip            = n_vip,
        n_vip_plus_pri   = n_vip + n_priority,
        cob_dt           = str(cob),
        start_dt         = str(start_dt),
        months_history   = months,
        txn_vip_total    = txn_v,
        txn_pri_total    = txn_p,
        txn_ret_total    = txn_r,
        max_txns         = max(txn_v, txn_p, txn_r),
        cc_vip_total     = cc_v,
        cc_pri_total     = cc_p,
        cc_ret_total     = cc_r,
        max_card_txns    = max(cc_v, cc_p, cc_r),
        crm_total        = crm_total,
        branch_inserts   = _ora_branch_insert_all(),
        product_inserts  = _ora_product_insert_all(),
    )


def _split_oracle(sql: str) -> list[str]:
    """Split Oracle SQL on ';' lines, skipping comment-only lines."""
    stmts: list[str] = []
    buf: list[str]   = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        if stripped.endswith(";"):
            buf.append(stripped[:-1])          # strip trailing semicolon
            stmt = "\n".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
        else:
            buf.append(line)
    return stmts


def _preprocess_postgres(sql: str, params: dict) -> str:
    """
    Prepare postgres_gen.sql for execution via psycopg2.

    postgres_gen.sql uses psql client syntax (\\set / :variable) so it can
    also be run directly with `psql -f`.  Before handing it to psycopg2 we:
      1. Drop \\set lines (psql meta-commands — values come from params instead).
      2. Replace :'variable' → 'value'  (quoted psql var, e.g. :'start_dt').
      3. Replace :variable  → value     (bare psql var, e.g. :n_customers).
         Uses negative lookbehind so :: (PostgreSQL cast) is left untouched.
    """
    import re

    lines = [l for l in sql.splitlines() if not l.strip().startswith("\\")]
    text  = "\n".join(lines)

    def _quoted(m: re.Match) -> str:
        return f"'{params[m.group(1)]}'" if m.group(1) in params else m.group(0)

    def _bare(m: re.Match) -> str:
        return str(params[m.group(1)]) if m.group(1) in params else m.group(0)

    text = re.sub(r":'([a-zA-Z_]\w*)'", _quoted, text)
    text = re.sub(r"(?<!:):([a-zA-Z_]\w*)",  _bare,   text)
    return text


def _split_postgres(sql: str) -> list[str]:
    """Split PostgreSQL SQL on ';', ignoring semicolons inside -- comments."""
    stmts: list[str] = []
    buf:   list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        # Strip inline trailing comment before scanning for ';'
        dash = line.find("--")
        code = line[:dash].rstrip() if dash >= 0 else line
        if not code.strip():
            continue
        if ";" in code:
            parts = code.split(";")
            for part in parts[:-1]:
                buf.append(part)
                stmt = "\n".join(buf).strip()
                if stmt:
                    stmts.append(stmt)
                buf = []
            remainder = parts[-1].strip()
            if remainder:
                buf.append(remainder)
        else:
            buf.append(code)
    if buf:
        stmt = "\n".join(buf).strip()
        if stmt:
            stmts.append(stmt)
    return stmts


def run_oracle(cfg: dict, params: dict) -> None:
    from . import oracle_loader
    conn = oracle_loader.connect(cfg)
    cur  = conn.cursor()
    # Seed Oracle's random number generator
    cur.execute("BEGIN DBMS_RANDOM.SEED(:1); END;", [params["seed"]])
    text  = Template((_SQL_DIR / "oracle_gen.sql").read_text(encoding="utf-8")).substitute(params)
    stmts = _split_oracle(text)
    print(f"  Executing {len(stmts)} Oracle statements...")
    for stmt in stmts:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()


def run_postgres(cfg: dict, params: dict) -> None:
    from . import postgres_loader
    conn = postgres_loader.connect(cfg)
    cur  = conn.cursor()
    raw   = (_SQL_DIR / "postgres_gen.sql").read_text(encoding="utf-8")
    text  = _preprocess_postgres(raw, params)
    stmts = _split_postgres(text)
    print(f"  Executing {len(stmts)} PostgreSQL statements...")
    for stmt in stmts:
        cur.execute(stmt)
    conn.commit()
    cur.close()
    conn.close()
