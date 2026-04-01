"""
PRIME Shop API — полная версия
Возможности:
  - Товары с брендами, вариациями, ссылками на источники
  - Заказы с историей статусов
  - Оплата через Payme и Click (webhook-ready)
  - Векторные рекомендации (косинусное сходство по категориям/брендам/ценам)
  - Роли администраторов (superadmin / manager / moderator)
  - Лог действий администраторов
  - Telegram-уведомления при новом заказе
"""

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import sqlite3, json, os, hmac, hashlib, time, math, urllib.request, urllib.parse, base64

# ═══════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════
BOT_TOKEN         = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DB_PATH           = os.environ.get("DB_PATH", "prime.db")
SUPER_ADMIN       = os.environ.get("SUPER_ADMIN", "kimtarwei").lower()
PAYME_KEY         = os.environ.get("PAYME_KEY", "")         # Payme merchant key
PAYME_ID          = os.environ.get("PAYME_ID", "")          # Payme merchant ID
CLICK_SECRET      = os.environ.get("CLICK_SECRET", "")      # Click secret key
CLICK_SERVICE_ID  = os.environ.get("CLICK_SERVICE_ID", "")  # Click service ID
NOTIFY_CHAT_ID    = os.environ.get("NOTIFY_CHAT_ID", "")    # Telegram chat ID for notifications

# Admin roles
ROLES = ["superadmin", "manager", "moderator"]
ROLE_PERMS = {
    "superadmin": ["products", "orders", "admins", "brands", "payments"],
    "manager":    ["products", "orders", "brands"],
    "moderator":  ["orders"],
}

ORDER_STATUSES = ["active", "in_transit", "customs", "warehouse", "delivered"]
# Styles are now stored in DB (managed by admin)
ORDER_STATUS_LABELS = {
    "active":     "В обработке",
    "in_transit": "В пути",
    "customs":    "На таможне",
    "warehouse":  "На складе PRIME",
    "delivered":  "Доставлен",
}

app = FastAPI(title="PRIME Shop API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ═══════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()

    # Products
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT    NOT NULL,
            cat          TEXT    NOT NULL DEFAULT 'Одежда',
            brand        TEXT    NOT NULL DEFAULT '',
            emoji        TEXT    NOT NULL DEFAULT '📦',
            photos       TEXT    NOT NULL DEFAULT '[]',
            price        INTEGER NOT NULL,
            old_price    INTEGER,
            sizes        TEXT    NOT NULL DEFAULT '[]',
            colors       TEXT    NOT NULL DEFAULT '[]',
            variations   TEXT    NOT NULL DEFAULT '[]',
            desc         TEXT    NOT NULL DEFAULT '',
            is_new       INTEGER NOT NULL DEFAULT 0,
            is_sale      INTEGER NOT NULL DEFAULT 0,
            is_preorder  INTEGER NOT NULL DEFAULT 0,
            quality      TEXT    NOT NULL DEFAULT '',
            rating       REAL    NOT NULL DEFAULT 5.0,
            reviews      INTEGER NOT NULL DEFAULT 0,
            sold         INTEGER NOT NULL DEFAULT 0,
            style        TEXT    NOT NULL DEFAULT '',
            source_links TEXT    NOT NULL DEFAULT '[]',
            created_at   INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Styles
    conn.execute("""
        CREATE TABLE IF NOT EXISTS styles (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL UNIQUE,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Brands (with associated styles)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS brands (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL UNIQUE,
            style_names TEXT NOT NULL DEFAULT '[]',
            created_at  INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Orders
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       TEXT    NOT NULL,
            username      TEXT    NOT NULL,
            recipient     TEXT    NOT NULL,
            is_self       INTEGER NOT NULL DEFAULT 1,
            self_username TEXT    NOT NULL DEFAULT '',
            items         TEXT    NOT NULL DEFAULT '[]',
            total         INTEGER NOT NULL DEFAULT 0,
            status        TEXT    NOT NULL DEFAULT 'active',
            payment_status TEXT   NOT NULL DEFAULT 'pending',
            payment_method TEXT   NOT NULL DEFAULT '',
            payment_id    TEXT    NOT NULL DEFAULT '',
            date_str      TEXT    NOT NULL DEFAULT '',
            created_at    INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Admin roles table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_roles (
            username TEXT PRIMARY KEY,
            role     TEXT NOT NULL DEFAULT 'moderator',
            added_by TEXT NOT NULL DEFAULT '',
            added_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Admin action log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            admin      TEXT    NOT NULL,
            action     TEXT    NOT NULL,
            target     TEXT    NOT NULL DEFAULT '',
            details    TEXT    NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Description templates
    conn.execute("""
        CREATE TABLE IF NOT EXISTS desc_templates (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            title      TEXT    NOT NULL,
            body       TEXT    NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # User events for recommendations (views, purchases)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    TEXT    NOT NULL,
            product_id INTEGER NOT NULL,
            event_type TEXT    NOT NULL,  -- 'view' | 'purchase' | 'wish'
            created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Payments table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id     INTEGER NOT NULL,
            provider     TEXT    NOT NULL,  -- 'payme' | 'click'
            provider_id  TEXT    NOT NULL DEFAULT '',
            amount       INTEGER NOT NULL,
            status       TEXT    NOT NULL DEFAULT 'pending',
            raw          TEXT    NOT NULL DEFAULT '{}',
            created_at   INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # Migrations for existing DBs
    migrations = [
        "ALTER TABLE orders ADD COLUMN receipt_url TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE products ADD COLUMN brand TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE products ADD COLUMN style TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE brands ADD COLUMN style_names TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE products ADD COLUMN source_links TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE orders ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'pending'",
        "ALTER TABLE orders ADD COLUMN payment_method TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN payment_id TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE products ADD COLUMN is_preorder INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE products ADD COLUMN quality TEXT NOT NULL DEFAULT ''",
    ]
    for m in migrations:
        try: conn.execute(m)
        except: pass

    # Seed superadmin role
    conn.execute(
        "INSERT OR IGNORE INTO admin_roles (username, role, added_by) VALUES (?,?,?)",
        (SUPER_ADMIN, "superadmin", "system")
    )

    if conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0:
        _seed(conn)

    conn.commit()
    conn.close()

def _seed(conn):
    seeds = [
        ("Платье шёлковое миди","Одежда","","👗",189000,320000,'["XS","S","M","L","XL"]','["#1a1a2e","#2d4a22"]',"Элегантное шёлковое платье.",1,1),
        ("Кроссовки Air Max","Обувь","Nike","👟",890000,None,'["39","40","41","42","43"]','["#fff","#1a1a1a"]',"Лёгкие кроссовки.",0,0),
        ("Джинсы Slim Fit","Одежда","","👖",320000,450000,'["28","30","32","34"]','["#1a3a5c","#1a1a1a"]',"Классические джинсы.",0,1),
        ("Серьги Hoop","Аксессуары","","💍",145000,210000,'["S","M"]','["#d4af37"]',"Золотые серьги.",1,1),
        ("Пуховик оверсайз","Одежда","","🧥",560000,780000,'["S","M","L","XL"]','["#1a1a1a","#f5f5f0"]',"Тёплый пуховик.",1,1),
        ("Ботинки Chelsea","Обувь","","👢",430000,None,'["36","37","38","39","40"]','["#1a1a1a"]',"Натуральная кожа.",0,0),
        ("Шелковый платок","Аксессуары","","🧣",85000,130000,'["70×70","90×90"]','["#8B0000"]',"Шёлковый платок.",0,1),
        ("Кеды классические","Обувь","","👟",250000,320000,'["37","38","39","40","41"]','["#fff","#1a1a1a"]',"Текстильные кеды.",0,1),
    ]
    conn.executemany("""
        INSERT INTO products (name,cat,brand,emoji,price,old_price,sizes,colors,desc,is_new,is_sale)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, seeds)
    # Seed styles
    for s in ["Casual", "Sport", "Formal", "Street", "Luxury", "Vintage", "Minimalist"]:
        conn.execute("INSERT OR IGNORE INTO styles (name) VALUES (?)", (s,))
    # Seed brands with associated styles
    seed_brands = [
        ("Nike",          '["Sport","Street"]'),
        ("Adidas",        '["Sport","Street","Casual"]'),
        ("Puma",          '["Sport","Casual"]'),
        ("Zara",          '["Casual","Minimalist"]'),
        ("H&M",           '["Casual"]'),
        ("Gucci",         '["Luxury","Street"]'),
        ("Prada",         '["Luxury","Formal"]'),
        ("Louis Vuitton", '["Luxury","Formal"]'),
    ]
    for name, styles_json in seed_brands:
        conn.execute("INSERT OR IGNORE INTO brands (name, style_names) VALUES (?,?)", (name, styles_json))

init_db()

# ═══════════════════════════════════════════════
# AUTH & ROLES
# ═══════════════════════════════════════════════
def parse_tg(raw: str) -> Optional[dict]:
    try:
        from urllib.parse import unquote, parse_qsl
        pairs = dict(parse_qsl(unquote(raw), keep_blank_values=True))
        check_hash = pairs.pop("hash", None)
        if not check_hash: return None
        data_check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret   = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(computed, check_hash): return None
        if time.time() - int(pairs.get("auth_date", 0)) > 86400: return None
        return json.loads(pairs.get("user", "{}"))
    except: return None

def get_user(x: Optional[str]) -> Optional[dict]:
    if not x: return None
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        return {"id": 0, "username": SUPER_ADMIN, "first_name": "Dev"}
    return parse_tg(x)

def get_admin_role(username: str) -> Optional[str]:
    conn = get_db()
    row = conn.execute("SELECT role FROM admin_roles WHERE username=?", (username.lower(),)).fetchone()
    conn.close()
    return row["role"] if row else None

def check_admin(x: Optional[str]) -> Optional[dict]:
    """Returns user dict with role if admin, else None."""
    user = get_user(x)
    if not user: return None
    uname = (user.get("username") or "").lower()
    role = get_admin_role(uname)
    if not role: return None
    user["role"] = role
    return user

def require_admin(x: Optional[str], perm: str = None) -> dict:
    user = check_admin(x)
    if not user: raise HTTPException(401, "Unauthorized")
    if perm and perm not in ROLE_PERMS.get(user["role"], []):
        raise HTTPException(403, f"Role '{user['role']}' lacks '{perm}' permission")
    return user

def log_action(admin: str, action: str, target: str = "", details: str = ""):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO admin_log (admin,action,target,details) VALUES (?,?,?,?)",
            (admin, action, target, details)
        )
        conn.commit(); conn.close()
    except: pass

# ═══════════════════════════════════════════════
# TELEGRAM NOTIFICATIONS
# ═══════════════════════════════════════════════
def notify_new_order(order: dict):
    if not NOTIFY_CHAT_ID or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        return
    try:
        items_text = "\n".join(
            f"  • {i.get('product_name','Товар')} × {i.get('qty',1)}"
            for i in (order.get("items") or [])[:5]
        )
        text = (
            f"🛍 *Новый заказ #{order['id']}*\n"
            f"👤 {order['recipient']}\n"
            f"💰 {order['total']:,} сум\n"
            f"{items_text}"
        )
        data = json.dumps({
            "chat_id": NOTIFY_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=data,
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"Notify error: {e}")

# ═══════════════════════════════════════════════
# RECOMMENDATIONS (vector-based cosine similarity)
# ═══════════════════════════════════════════════
CATS  = ["Одежда", "Обувь", "Аксессуары"]
PRICE_BUCKETS = [0, 100000, 250000, 500000, 1000000, 999999999]

def get_style_names(conn) -> List[str]:
    rows = conn.execute("SELECT name FROM styles ORDER BY name").fetchall()
    return [r["name"] for r in rows]

def get_brand_style_map(conn) -> dict:
    """Returns {brand_name: [style1, style2, ...]} from DB."""
    rows = conn.execute("SELECT name, style_names FROM brands").fetchall()
    result = {}
    for r in rows:
        try: result[r["name"]] = json.loads(r["style_names"] or "[]")
        except: result[r["name"]] = []
    return result

def product_vector(p: dict, all_brands: List[str] = [], all_styles: List[str] = [],
                   brand_style_map: dict = {}) -> List[float]:
    """
    Feature vector for a product:
    [cat(3) + price_bucket(5) + style_onehot(N) + brand_onehot(M) + is_new + is_sale]

    Key insight: if a product has a brand, its associated styles are also activated
    in the style vector — so Nike activates Sport+Street automatically.
    """
    # Category (3)
    cat_vec = [0.0] * len(CATS)
    cat = p.get("cat","")
    if cat in CATS: cat_vec[CATS.index(cat)] = 1.0

    # Price bucket (5)
    price = p.get("price", 0) or 0
    price_vec = [0.0] * (len(PRICE_BUCKETS)-1)
    for i in range(len(PRICE_BUCKETS)-1):
        if PRICE_BUCKETS[i] <= price < PRICE_BUCKETS[i+1]:
            price_vec[i] = 1.0; break

    # Style vector — direct style + styles implied by brand
    style_vec = [0.0] * len(all_styles) if all_styles else []
    if all_styles:
        brand = p.get("brand","")
        product_style = p.get("style","")

        # Direct style on product (weight 1.5)
        if product_style and product_style in all_styles:
            style_vec[all_styles.index(product_style)] = 1.5

        # Brand-implied styles (weight 1.0) — this is the key feature!
        # Nike → [Sport, Street] so those positions also get activated
        brand_styles = brand_style_map.get(brand, [])
        for bs in brand_styles:
            if bs in all_styles:
                idx = all_styles.index(bs)
                style_vec[idx] = max(style_vec[idx], 1.0)

    # Brand vector (weight 2.0)
    brand_vec = []
    if all_brands:
        brand_vec = [0.0] * len(all_brands)
        brand = p.get("brand","")
        if brand and brand in all_brands:
            brand_vec[all_brands.index(brand)] = 2.0

    # Flags (2)
    flags = [float(bool(p.get("isNew"))), float(bool(p.get("isSale")))]

    return cat_vec + price_vec + style_vec + brand_vec + flags

def cosine_sim(a: List[float], b: List[float]) -> float:
    dot  = sum(x*y for x,y in zip(a,b))
    norm_a = math.sqrt(sum(x*x for x in a))
    norm_b = math.sqrt(sum(x*x for x in b))
    if norm_a == 0 or norm_b == 0: return 0.0
    return dot / (norm_a * norm_b)

def get_all_brand_names(conn) -> List[str]:
    rows = conn.execute("SELECT name FROM brands ORDER BY name").fetchall()
    return [r["name"] for r in rows]

def get_user_profile_vector(user_id: str, all_brands: List[str],
                             all_styles: List[str], brand_style_map: dict) -> Optional[List[float]]:
    """
    Weighted average vector of products user interacted with.
    view=1, wish=2, purchase=4
    """
    conn = get_db()
    rows = conn.execute("""
        SELECT e.event_type, p.cat, p.brand, p.style, p.price, p.is_new, p.is_sale
        FROM user_events e
        JOIN products p ON p.id = e.product_id
        WHERE e.user_id = ?
        ORDER BY e.created_at DESC LIMIT 200
    """, (user_id,)).fetchall()
    conn.close()
    if not rows: return None
    weights = {"view": 1.0, "wish": 2.0, "purchase": 4.0}
    dim = len(CATS) + (len(PRICE_BUCKETS)-1) + len(all_styles) + len(all_brands) + 2
    total_w, acc = 0.0, [0.0] * dim
    for r in rows:
        p = {"cat": r["cat"], "brand": r["brand"], "style": r["style"],
             "price": r["price"], "isNew": r["is_new"], "isSale": r["is_sale"]}
        w = weights.get(r["event_type"], 1.0)
        v = product_vector(p, all_brands, all_styles, brand_style_map)
        if len(v) < dim: v += [0.0] * (dim - len(v))
        acc = [acc[i] + v[i]*w for i in range(dim)]
        total_w += w
    if total_w == 0: return None
    return [x/total_w for x in acc]

# ═══════════════════════════════════════════════
# SERIALIZERS
# ═══════════════════════════════════════════════
def row_to_product(row, include_links=False) -> dict:
    cols = row.keys()
    d = {
        "id":         row["id"],
        "name":       row["name"],
        "cat":        row["cat"],
        "brand":      row["brand"] if "brand" in cols else "",
        "style":      row["style"] if "style" in cols else "",
        "emoji":      row["emoji"],
        "photos":     json.loads(row["photos"]),
        "price":      row["price"],
        "old":        row["old_price"],
        "sizes":      json.loads(row["sizes"]),
        "colors":     json.loads(row["colors"]),
        "variations": json.loads(row["variations"]) if "variations" in cols else [],
        "desc":       row["desc"],
        "isNew":      bool(row["is_new"]),
        "isSale":     bool(row["is_sale"]),
        "isPreorder": bool(row["is_preorder"]) if "is_preorder" in cols else False,
        "quality":    row["quality"] if "quality" in cols else "",
        "rating":     row["rating"],
        "reviews":    row["reviews"],
        "sold":       row["sold"],
    }
    if include_links:
        raw = row["source_links"] if "source_links" in cols else "[]"
        d["source_links"] = json.loads(raw or "[]")
    return d

def row_to_order(row) -> dict:
    cols = row.keys()
    return {
        "id":            row["id"],
        "user_id":       row["user_id"],
        "username":      row["username"],
        "recipient":     row["recipient"],
        "isSelf":        bool(row["is_self"]),
        "selfUsername":  row["self_username"],
        "items":         json.loads(row["items"]),
        "total":         row["total"],
        "status":        row["status"],
        "statusLabel":   ORDER_STATUS_LABELS.get(row["status"], "В обработке"),
        "paymentStatus": row["payment_status"] if "payment_status" in cols else "pending",
        "paymentMethod": row["payment_method"] if "payment_method" in cols else "",
        "receiptUrl":    row["receipt_url"] if "receipt_url" in cols else "",
        "date":          row["date_str"],
        "createdAt":     row["created_at"],
    }

# ═══════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════
class SourceLink(BaseModel):
    label: str
    url:   str

class ProductCreate(BaseModel):
    name:         str
    cat:          str             = "Одежда"
    brand:        str             = ""
    emoji:        str             = "📦"
    photos:       List[str]       = []
    price:        int
    old_price:    Optional[int]   = None
    sizes:        List[str]       = []
    colors:       List[str]       = []
    variations:   List[dict]      = []
    desc:         str             = ""
    style:        str             = ""
    is_new:       bool            = False
    is_preorder:  bool            = False
    quality:      str             = ""
    source_links: List[SourceLink] = []

class ProductUpdate(BaseModel):
    name:         Optional[str]             = None
    cat:          Optional[str]             = None
    brand:        Optional[str]             = None
    emoji:        Optional[str]             = None
    photos:       Optional[List[str]]       = None
    price:        Optional[int]             = None
    old_price:    Optional[int]             = None
    sizes:        Optional[List[str]]       = None
    colors:       Optional[List[str]]       = None
    variations:   Optional[List[dict]]      = None
    desc:         Optional[str]             = None
    style:        Optional[str]             = None
    is_new:       Optional[bool]            = None
    is_preorder:  Optional[bool]            = None
    quality:      Optional[str]             = None
    source_links: Optional[List[SourceLink]] = None

class OrderCreate(BaseModel):
    recipient:     str
    is_self:       bool       = True
    self_username: str        = ""
    items:         List[dict] = []
    total:         int        = 0
    date_str:      str        = ""

class OrderStatusUpdate(BaseModel):
    status: str
    payment_status: Optional[str] = None

class AdminRoleUpdate(BaseModel):
    username: str
    role:     str

class BrandCreate(BaseModel):
    name:   str
    styles: List[str] = []

class StyleCreate(BaseModel):
    name: str

class BrandUpdate(BaseModel):
    styles: List[str] = []

class UserEvent(BaseModel):
    product_id: int
    event_type: str  # view | wish | purchase

class PaymeWebhook(BaseModel):
    method: str
    params: dict

class ClickWebhook(BaseModel):
    click_trans_id:  int
    service_id:      int
    click_paydoc_id: int
    amount:          float
    action:          int
    error:           int
    sign_time:       str
    sign_string:     str
    merchant_trans_id: str = ""
    merchant_prepare_id: Optional[int] = None

# ═══════════════════════════════════════════════
# ROUTES — SYSTEM
# ═══════════════════════════════════════════════
@app.get("/")
def root(): return {"status": "ok", "version": "2.0"}

@app.get("/health")
def health(): return {"status": "ok", "ts": int(time.time())}

# ═══════════════════════════════════════════════
# ROUTES — BRANDS
# ═══════════════════════════════════════════════
@app.get("/brands")
def list_brands():
    conn = get_db()
    brand_rows = conn.execute("SELECT * FROM brands ORDER BY name").fetchall()
    style_rows = conn.execute("SELECT * FROM styles ORDER BY name").fetchall()
    conn.close()
    return {
        "brands": [
            {
                "id": r["id"],
                "name": r["name"],
                "styles": json.loads(r["style_names"] if "style_names" in r.keys() else "[]"),
            }
            for r in brand_rows
        ],
        "styles": [{"id": r["id"], "name": r["name"]} for r in style_rows],
    }

@app.post("/brands", status_code=201)
def create_brand(body: BrandCreate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "brands")
    name = body.name.strip()
    if not name: raise HTTPException(400, "Name required")
    conn = get_db()
    try:
        cur = conn.execute(
            "INSERT INTO brands (name, style_names) VALUES (?,?)",
            (name, json.dumps(body.styles, ensure_ascii=False))
        )
        conn.commit()
        bid = cur.lastrowid
        conn.close()
        log_action(user["username"], "brand_create", name)
        return {"id": bid, "name": name, "styles": body.styles}
    except:
        conn.close()
        raise HTTPException(409, "Brand already exists")

@app.patch("/brands/{bid}/styles")
def update_brand_styles(bid: int, body: BrandUpdate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "brands")
    conn = get_db()
    row = conn.execute("SELECT name FROM brands WHERE id=?", (bid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    conn.execute(
        "UPDATE brands SET style_names=? WHERE id=?",
        (json.dumps(body.styles, ensure_ascii=False), bid)
    )
    conn.commit(); conn.close()
    log_action(user["username"], "brand_update_styles", row["name"])
    return {"id": bid, "name": row["name"], "styles": body.styles}

@app.delete("/brands/{bid}", status_code=204)
def delete_brand(bid: int, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "brands")
    conn = get_db()
    row = conn.execute("SELECT name FROM brands WHERE id=?", (bid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    conn.execute("DELETE FROM brands WHERE id=?", (bid,))
    conn.commit(); conn.close()
    log_action(user["username"], "brand_delete", row["name"])

# ═══════════════════════════════════════════════
# ROUTES — PRODUCTS
# ═══════════════════════════════════════════════
@app.get("/products")
def list_products(x_init_data: Optional[str] = Header(None)):
    is_admin = check_admin(x_init_data) is not None
    conn = get_db()
    rows = conn.execute("SELECT * FROM products ORDER BY created_at DESC").fetchall()
    conn.close()
    return [row_to_product(r, include_links=is_admin) for r in rows]

@app.post("/products", status_code=201)
def create_product(body: ProductCreate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "products")
    conn = get_db()
    cur = conn.execute("""
        INSERT INTO products
          (name,cat,brand,style,emoji,photos,price,old_price,sizes,colors,variations,desc,is_new,is_sale,is_preorder,quality,source_links)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        body.name, body.cat, body.brand, body.style, body.emoji,
        json.dumps(body.photos, ensure_ascii=False),
        body.price, body.old_price,
        json.dumps(body.sizes, ensure_ascii=False),
        json.dumps(body.colors, ensure_ascii=False),
        json.dumps(body.variations, ensure_ascii=False),
        body.desc, int(body.is_new), 1 if body.old_price else 0,
        int(body.is_preorder), body.quality,
        json.dumps([l.dict() for l in body.source_links], ensure_ascii=False),
    ))
    new_id = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM products WHERE id=?", (new_id,)).fetchone()
    conn.close()
    log_action(user["username"], "product_create", body.name)
    return row_to_product(row, include_links=True)

@app.put("/products/{pid}")
def update_product(pid: int, body: ProductUpdate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "products")
    conn = get_db()
    if not conn.execute("SELECT id FROM products WHERE id=?", (pid,)).fetchone():
        conn.close(); raise HTTPException(404, "Not found")
    fields, vals = [], []
    def add(col, val): fields.append(f"{col}=?"); vals.append(val)
    if body.name         is not None: add("name", body.name)
    if body.cat          is not None: add("cat", body.cat)
    if body.brand        is not None: add("brand", body.brand)
    if body.emoji        is not None: add("emoji", body.emoji)
    if body.photos       is not None: add("photos", json.dumps(body.photos, ensure_ascii=False))
    if body.price        is not None: add("price", body.price)
    if body.old_price    is not None: add("old_price", body.old_price); add("is_sale", 1)
    if body.sizes        is not None: add("sizes", json.dumps(body.sizes, ensure_ascii=False))
    if body.colors       is not None: add("colors", json.dumps(body.colors, ensure_ascii=False))
    if body.variations   is not None: add("variations", json.dumps(body.variations, ensure_ascii=False))
    if body.style        is not None: add("style", body.style)
    if body.desc         is not None: add("desc", body.desc)
    if body.is_new       is not None: add("is_new", int(body.is_new))
    if body.is_preorder  is not None: add("is_preorder", int(body.is_preorder))
    if body.quality      is not None: add("quality", body.quality)
    if body.source_links is not None:
        add("source_links", json.dumps([l.dict() for l in body.source_links], ensure_ascii=False))
    if fields:
        vals.append(pid)
        conn.execute(f"UPDATE products SET {','.join(fields)} WHERE id=?", vals)
        conn.commit()
    row = conn.execute("SELECT * FROM products WHERE id=?", (pid,)).fetchone()
    conn.close()
    log_action(user["username"], "product_update", str(pid))
    return row_to_product(row, include_links=True)

class BulkPriceBody(BaseModel):
    action: str   # "increase" | "decrease" | "discount"
    amount: float # сумма в сум или процент для discount

@app.post("/products/bulk-price")
def bulk_price(body: BulkPriceBody, x_init_data: Optional[str] = Header(None)):
    """
    Массовое изменение цен:
    - increase : price += amount (сум)
    - decrease : price -= amount (сум), минимум 1
    - discount : old_price = price, price = round(price * (1 - amount/100)), is_sale = 1
    """
    user = require_admin(x_init_data, "products")
    if body.action not in ("increase", "decrease", "discount"):
        raise HTTPException(400, "Invalid action")
    if body.amount <= 0:
        raise HTTPException(400, "amount must be > 0")
    if body.action == "discount" and body.amount >= 100:
        raise HTTPException(400, "discount must be < 100%")

    conn = get_db()
    rows = conn.execute("SELECT id, price FROM products").fetchall()
    updated = 0
    for row in rows:
        pid_  = row["id"]
        price = row["price"]
        if body.action == "increase":
            new_price = price + int(body.amount)
            conn.execute("UPDATE products SET price=? WHERE id=?", (new_price, pid_))
        elif body.action == "decrease":
            new_price = max(1, price - int(body.amount))
            conn.execute("UPDATE products SET price=? WHERE id=?", (new_price, pid_))
        elif body.action == "discount":
            new_price = max(1, round(price * (1 - body.amount / 100)))
            if new_price < price:
                conn.execute(
                    "UPDATE products SET old_price=?, price=?, is_sale=1 WHERE id=?",
                    (price, new_price, pid_)
                )
        updated += 1
    conn.commit()
    conn.close()
    log_action(user["username"], f"bulk_price_{body.action}", f"amount={body.amount}, products={updated}")
    return {"updated": updated}

@app.delete("/products/{pid}", status_code=204)
def delete_product(pid: int, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "products")
    conn = get_db()
    row = conn.execute("SELECT name FROM products WHERE id=?", (pid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    conn.execute("DELETE FROM products WHERE id=?", (pid,))
    conn.commit(); conn.close()
    log_action(user["username"], "product_delete", row["name"])

# ═══════════════════════════════════════════════
# ROUTES — EVENTS & RECOMMENDATIONS
# ═══════════════════════════════════════════════
# ── STYLES CRUD ──────────────────────────────────────────
@app.get("/styles")
def list_styles():
    conn = get_db()
    rows = conn.execute("SELECT * FROM styles ORDER BY name").fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["name"]} for r in rows]

@app.post("/styles", status_code=201)
def create_style(body: StyleCreate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "brands")
    name = body.name.strip()
    if not name: raise HTTPException(400, "Name required")
    conn = get_db()
    try:
        cur = conn.execute("INSERT INTO styles (name) VALUES (?)", (name,))
        conn.commit()
        sid = cur.lastrowid; conn.close()
        log_action(user["username"], "style_create", name)
        return {"id": sid, "name": name}
    except:
        conn.close(); raise HTTPException(409, "Style already exists")

@app.delete("/styles/{sid}", status_code=204)
def delete_style(sid: int, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "brands")
    conn = get_db()
    row = conn.execute("SELECT name FROM styles WHERE id=?", (sid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    conn.execute("DELETE FROM styles WHERE id=?", (sid,))
    conn.commit(); conn.close()
    log_action(user["username"], "style_delete", row["name"])

# ═══════════════════════════════════════════════
# ROUTES — DESCRIPTION TEMPLATES
# ═══════════════════════════════════════════════
class DescTemplateCreate(BaseModel):
    title: str
    body:  str = ""

class DescTemplateUpdate(BaseModel):
    title: Optional[str] = None
    body:  Optional[str] = None

@app.get("/desc-templates")
def list_desc_templates(x_init_data: Optional[str] = Header(None)):
    # Читать шаблоны может любой авторизованный пользователь
    conn = get_db()
    rows = conn.execute("SELECT * FROM desc_templates ORDER BY created_at DESC").fetchall()
    conn.close()
    return [{"id": r["id"], "title": r["title"], "body": r["body"]} for r in rows]

@app.post("/desc-templates", status_code=201)
def create_desc_template(body: DescTemplateCreate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "products")
    title = body.title.strip()
    if not title: raise HTTPException(400, "Title required")
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO desc_templates (title, body) VALUES (?,?)",
        (title, body.body)
    )
    conn.commit()
    tid = cur.lastrowid; conn.close()
    log_action(user["username"], "template_create", title)
    return {"id": tid, "title": title, "body": body.body}

@app.patch("/desc-templates/{tid}")
def update_desc_template(tid: int, body: DescTemplateUpdate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "products")
    conn = get_db()
    row = conn.execute("SELECT * FROM desc_templates WHERE id=?", (tid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    fields, vals = [], []
    if body.title is not None: fields.append("title=?"); vals.append(body.title.strip())
    if body.body  is not None: fields.append("body=?");  vals.append(body.body)
    if fields:
        vals.append(tid)
        conn.execute(f"UPDATE desc_templates SET {','.join(fields)} WHERE id=?", vals)
        conn.commit()
    row = conn.execute("SELECT * FROM desc_templates WHERE id=?", (tid,)).fetchone()
    conn.close()
    log_action(user["username"], "template_update", row["title"])
    return {"id": row["id"], "title": row["title"], "body": row["body"]}

@app.delete("/desc-templates/{tid}", status_code=204)
def delete_desc_template(tid: int, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "products")
    conn = get_db()
    row = conn.execute("SELECT title FROM desc_templates WHERE id=?", (tid,)).fetchone()
    if not row: conn.close(); raise HTTPException(404, "Not found")
    conn.execute("DELETE FROM desc_templates WHERE id=?", (tid,))
    conn.commit(); conn.close()
    log_action(user["username"], "template_delete", row["title"])


def track_event(body: UserEvent, x_init_data: Optional[str] = Header(None)):
    """Track user interaction for recommendation engine."""
    user = get_user(x_init_data)
    uid = str(user["id"]) if user else "guest"
    if body.event_type not in ("view", "wish", "purchase"):
        raise HTTPException(400, "Invalid event_type")
    conn = get_db()
    conn.execute(
        "INSERT INTO user_events (user_id,product_id,event_type) VALUES (?,?,?)",
        (uid, body.product_id, body.event_type)
    )
    conn.commit(); conn.close()

@app.get("/recommendations")
def get_recommendations(x_init_data: Optional[str] = Header(None), limit: int = 10):
    """
    Returns personalized product recommendations using cosine similarity.
    Falls back to popular products if no user history.
    """
    user = get_user(x_init_data)
    is_admin = check_admin(x_init_data) is not None
    uid = str(user["id"]) if user else None

    conn = get_db()
    all_rows = conn.execute("SELECT * FROM products").fetchall()
    all_brands      = get_all_brand_names(conn)
    all_styles      = get_style_names(conn)
    brand_style_map = get_brand_style_map(conn)
    conn.close()

    all_products    = [row_to_product(r, include_links=is_admin) for r in all_rows]

    if uid:
        profile = get_user_profile_vector(uid, all_brands, all_styles, brand_style_map)
    else:
        profile = None

    if profile:
        scored = []
        for p in all_products:
            vec = product_vector(p, all_brands, all_styles, brand_style_map)
            # Align dimensions
            diff = len(profile) - len(vec)
            if diff > 0:   vec     = vec     + [0.0] * diff
            elif diff < 0: profile = profile + [0.0] * (-diff)
            sim = cosine_sim(profile, vec)
            scored.append((sim, p))
        scored.sort(key=lambda x: -x[0])
        return [p for _, p in scored[:limit]]
    else:
        return sorted(all_products, key=lambda p: -(p.get("sold") or 0))[:limit]

# ═══════════════════════════════════════════════
# ROUTES — ORDERS
# ═══════════════════════════════════════════════
@app.get("/orders")
def list_orders(x_init_data: Optional[str] = Header(None)):
    require_admin(x_init_data, "orders")
    conn = get_db()
    rows = conn.execute("SELECT * FROM orders ORDER BY created_at DESC").fetchall()
    conn.close()
    return [row_to_order(r) for r in rows]

@app.get("/orders/my")
def my_orders(x_init_data: Optional[str] = Header(None)):
    user = get_user(x_init_data)
    if not user: return []
    uid = str(user.get("id", ""))
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM orders WHERE user_id=? ORDER BY created_at DESC", (uid,)
    ).fetchall()
    conn.close()
    print(f"[my_orders] uid={uid!r} → {len(rows)} orders")
    return [row_to_order(r) for r in rows]

@app.post("/orders", status_code=201)
def create_order(body: OrderCreate, x_init_data: Optional[str] = Header(None)):
    user = get_user(x_init_data) if x_init_data else None
    uid  = str(user.get("id","guest")) if user else "guest"
    uname = (user.get("username") or uid) if user else "guest"
    conn = get_db()
    cur = conn.execute("""
        INSERT INTO orders (user_id,username,recipient,is_self,self_username,items,total,status,date_str)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (uid, uname, body.recipient, int(body.is_self),
          body.self_username, json.dumps(body.items, ensure_ascii=False),
          body.total, "active", body.date_str))
    new_id = cur.lastrowid
    conn.commit()
    row = conn.execute("SELECT * FROM orders WHERE id=?", (new_id,)).fetchone()
    conn.close()
    order = row_to_order(row)
    # Track purchase events
    for item in body.items:
        pid = item.get("product_id")
        if pid:
            try:
                conn2 = get_db()
                conn2.execute(
                    "INSERT INTO user_events (user_id,product_id,event_type) VALUES (?,?,?)",
                    (uid, pid, "purchase")
                )
                conn2.commit(); conn2.close()
            except: pass
    # Notify admin
    notify_new_order(order)
    return order

@app.get("/orders/{oid}/payment-status")
def get_payment_status(oid: int, x_init_data: Optional[str] = Header(None)):
    user = get_user(x_init_data)
    if not user:
        raise HTTPException(401, "Auth required")
    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    if not order:
        raise HTTPException(404, "Not found")
    is_owner = str(order["user_id"]) == str(user.get("id",""))
    is_adm   = check_admin(x_init_data) is not None
    if not is_owner and not is_adm:
        raise HTTPException(403, "Forbidden")
    paid = order["payment_status"] == "paid"
    return {"paid": paid, "order": row_to_order(order)}

@app.patch("/orders/{oid}/status")
def update_order_status(oid: int, body: OrderStatusUpdate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "orders")
    if body.status not in ORDER_STATUSES:
        raise HTTPException(400, f"Valid statuses: {ORDER_STATUSES}")
    conn = get_db()
    if body.payment_status:
        affected = conn.execute(
            "UPDATE orders SET status=?, payment_status=? WHERE id=?",
            (body.status, body.payment_status, oid)
        ).rowcount
    else:
        affected = conn.execute("UPDATE orders SET status=? WHERE id=?", (body.status, oid)).rowcount
    conn.commit()
    row = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    if not affected: raise HTTPException(404, "Not found")
    log_action(user["username"], "order_status", str(oid), body.status)
    return row_to_order(row)

class ReceiptUpload(BaseModel):
    receipt: str   # base64 image
    username: str  = ""

@app.post("/orders/{oid}/receipt", status_code=204)
def upload_receipt(oid: int, body: ReceiptUpload, x_init_data: Optional[str] = Header(None)):
    """Принимает чек от покупателя и отправляет его в Telegram."""
    user = get_user(x_init_data)
    if not user: raise HTTPException(401, "Auth required")

    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
    conn.close()
    if not order: raise HTTPException(404, "Order not found")

    # Проверяем что это заказ этого пользователя
    if str(order["user_id"]) != str(user.get("id","")) and not check_admin(x_init_data):
        raise HTTPException(403, "Forbidden")

    # Отправляем чек в Telegram
    if NOTIFY_CHAT_ID and BOT_TOKEN != "YOUR_BOT_TOKEN_HERE":
        try:
            caption = (
                "💳 Чек об оплате\n"
                f"Заказ #{oid}\n"
                f"👤 {body.username or order['username']}\n"
                f"💰 {order['total']:,} сум\n"
                "💳 Перевод на карту"
            )
            # Отправляем фото по URL (Cloudinary) — просто и надёжно
            data = json.dumps({
                "chat_id": NOTIFY_CHAT_ID,
                "photo": body.receipt,
                "caption": caption,
            }).encode()
            req = urllib.request.Request(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data=data,
                headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            print(f"Receipt notify error: {e}")

    # Сохраняем URL чека и помечаем заказ как ожидающий подтверждения
    conn = get_db()
    conn.execute(
        "UPDATE orders SET payment_method='card', payment_status='receipt_sent', receipt_url=? WHERE id=?",
        (body.receipt, oid)
    )
    conn.commit(); conn.close()

@app.delete("/orders/{oid}", status_code=204)
def delete_order(oid: int, x_init_data: Optional[str] = Header(None)):
    """Delete an order. Only superadmin can do this."""
    user = require_admin(x_init_data, "orders")
    if user.get("role") != "superadmin":
        raise HTTPException(403, "Only superadmin can delete orders")
    conn = get_db()
    row = conn.execute("SELECT id FROM orders WHERE id=?", (oid,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Order not found")
    conn.execute("DELETE FROM orders WHERE id=?", (oid,))
    conn.execute("DELETE FROM payments WHERE order_id=?", (oid,))
    conn.commit()
    conn.close()
    log_action(user["username"], "order_delete", str(oid))

# ═══════════════════════════════════════════════
# ROUTES — ADMIN MANAGEMENT
# ═══════════════════════════════════════════════
@app.get("/admins/me")
def my_role(x_init_data: Optional[str] = Header(None)):
    """Any authenticated user can check their own admin role."""
    user = get_user(x_init_data)
    if not user:
        raise HTTPException(401, "Auth required")
    uname = (user.get("username") or "").lower()
    role = get_admin_role(uname)
    return {"username": uname, "role": role, "is_admin": role is not None}

@app.get("/admins")
def list_admins(x_init_data: Optional[str] = Header(None)):
    require_admin(x_init_data, "admins")
    conn = get_db()
    rows = conn.execute("SELECT * FROM admin_roles ORDER BY added_at").fetchall()
    conn.close()
    return [{"username": r["username"], "role": r["role"], "addedBy": r["added_by"]} for r in rows]

@app.post("/admins", status_code=201)
def add_admin(body: AdminRoleUpdate, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "admins")
    if user["role"] != "superadmin":
        raise HTTPException(403, "Only superadmin can manage admins")
    if body.role not in ROLES:
        raise HTTPException(400, f"Valid roles: {ROLES}")
    uname = body.username.lower().lstrip("@")
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO admin_roles (username,role,added_by) VALUES (?,?,?)",
        (uname, body.role, user["username"])
    )
    conn.commit(); conn.close()
    log_action(user["username"], "admin_add", uname, body.role)
    return {"username": uname, "role": body.role}

@app.delete("/admins/{username}", status_code=204)
def remove_admin(username: str, x_init_data: Optional[str] = Header(None)):
    user = require_admin(x_init_data, "admins")
    if user["role"] != "superadmin":
        raise HTTPException(403, "Only superadmin can remove admins")
    uname = username.lower().lstrip("@")
    if uname == SUPER_ADMIN:
        raise HTTPException(400, "Cannot remove superadmin")
    conn = get_db()
    conn.execute("DELETE FROM admin_roles WHERE username=?", (uname,))
    conn.commit(); conn.close()
    log_action(user["username"], "admin_remove", uname)

@app.get("/admin/log")
def admin_log(x_init_data: Optional[str] = Header(None), limit: int = 50):
    require_admin(x_init_data, "admins")
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM admin_log ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [{"id": r["id"], "admin": r["admin"], "action": r["action"],
             "target": r["target"], "details": r["details"], "ts": r["created_at"]} for r in rows]

@app.get("/admin/stats")
def admin_stats(x_init_data: Optional[str] = Header(None)):
    require_admin(x_init_data, "orders")
    conn = get_db()
    total_orders  = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    total_revenue = conn.execute("SELECT COALESCE(SUM(total),0) FROM orders").fetchone()[0]
    active_orders = conn.execute("SELECT COUNT(*) FROM orders WHERE status='active'").fetchone()[0]
    total_products= conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    paid_orders   = conn.execute("SELECT COUNT(*) FROM orders WHERE payment_status='paid'").fetchone()[0]
    conn.close()
    return {
        "totalOrders":   total_orders,
        "totalRevenue":  total_revenue,
        "activeOrders":  active_orders,
        "totalProducts": total_products,
        "paidOrders":    paid_orders,
    }

# ═══════════════════════════════════════════════
# ROUTES — PAYMENTS: PAYME
# ═══════════════════════════════════════════════
@app.post("/payments/payme/webhook")
async def payme_webhook(request: Request):
    """
    Payme JSONRPC webhook handler.
    Docs: https://developer.payme.uz/documentation
    """
    # Verify Basic Auth
    auth = request.headers.get("Authorization","")
    if PAYME_KEY:
        expected = base64.b64encode(f"Paycom:{PAYME_KEY}".encode()).decode()
        if auth != f"Basic {expected}":
            return JSONResponse({"error": {"code": -32504, "message": "Forbidden"}})

    body = await request.json()
    method = body.get("method","")
    params = body.get("params", {})
    req_id = body.get("id", 1)

    def err(code, msg):
        return JSONResponse({"jsonrpc":"2.0","id":req_id,"error":{"code":code,"message":msg}})
    def ok(result):
        return JSONResponse({"jsonrpc":"2.0","id":req_id,"result":result})

    if method == "CheckPerformTransaction":
        order_id = int(params.get("account",{}).get("order_id",0))
        amount   = params.get("amount",0)
        conn = get_db()
        order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        conn.close()
        if not order: return err(-31050, "Order not found")
        if order["total"] * 100 != amount: return err(-31001, "Wrong amount")
        return ok({"allow": True})

    elif method == "CreateTransaction":
        order_id   = int(params.get("account",{}).get("order_id",0))
        trans_id   = params.get("id","")
        amount     = params.get("amount",0)
        create_time= params.get("time", int(time.time()*1000))
        conn = get_db()
        order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order: conn.close(); return err(-31050, "Order not found")
        if order["total"] * 100 != amount: conn.close(); return err(-31001, "Wrong amount")
        # Save payment record
        existing = conn.execute("SELECT id FROM payments WHERE provider_id=?", (trans_id,)).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO payments (order_id,provider,provider_id,amount,status,raw) VALUES (?,?,?,?,?,?)",
                (order_id,"payme",trans_id,amount,"pending",json.dumps(params))
            )
            conn.commit()
        conn.close()
        return ok({"create_time": create_time, "transaction": trans_id, "state": 1})

    elif method == "PerformTransaction":
        trans_id = params.get("id","")
        conn = get_db()
        pay = conn.execute("SELECT * FROM payments WHERE provider_id=?", (trans_id,)).fetchone()
        if not pay: conn.close(); return err(-31003, "Transaction not found")
        conn.execute("UPDATE payments SET status='paid' WHERE provider_id=?", (trans_id,))
        conn.execute("UPDATE orders SET payment_status='paid', payment_method='payme', payment_id=? WHERE id=?",
                     (trans_id, pay["order_id"]))
        conn.commit(); conn.close()
        return ok({"transaction": trans_id, "perform_time": int(time.time()*1000), "state": 2})

    elif method == "CancelTransaction":
        trans_id = params.get("id","")
        conn = get_db()
        conn.execute("UPDATE payments SET status='cancelled' WHERE provider_id=?", (trans_id,))
        conn.commit(); conn.close()
        return ok({"transaction": trans_id, "cancel_time": int(time.time()*1000), "state": -1})

    elif method == "CheckTransaction":
        trans_id = params.get("id","")
        conn = get_db()
        pay = conn.execute("SELECT * FROM payments WHERE provider_id=?", (trans_id,)).fetchone()
        conn.close()
        if not pay: return err(-31003, "Transaction not found")
        state_map = {"pending":1,"paid":2,"cancelled":-1}
        return ok({"create_time": pay["created_at"]*1000,
                   "perform_time": pay["created_at"]*1000,
                   "cancel_time": 0,
                   "transaction": trans_id,
                   "state": state_map.get(pay["status"],1),
                   "reason": None})

    return err(-32601, "Method not found")

@app.get("/payments/payme/url/{order_id}")
def payme_payment_url(order_id: int, x_init_data: Optional[str] = Header(None)):
    """Generate Payme payment URL for an order."""
    user = get_user(x_init_data)
    if not user: raise HTTPException(401, "Auth required")
    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    conn.close()
    if not order: raise HTTPException(404, "Order not found")
    if str(order["user_id"]) != str(user.get("id","")):
        if check_admin(x_init_data) is None:
            raise HTTPException(403, "Not your order")
    amount = order["total"] * 100  # Payme works in tiyins
    params = f"m={PAYME_ID};ac.order_id={order_id};a={amount}"
    encoded = base64.b64encode(params.encode()).decode()
    url = f"https://checkout.paycom.uz/{encoded}"
    return {"url": url, "amount": amount, "order_id": order_id}

# ═══════════════════════════════════════════════
# ROUTES — PAYMENTS: CLICK
# ═══════════════════════════════════════════════
def verify_click_sign(body: ClickWebhook, action: int) -> bool:
    if not CLICK_SECRET: return True  # dev mode
    sign_str = (
        f"{body.click_trans_id}{body.service_id}"
        f"{CLICK_SECRET}{body.merchant_trans_id}"
        f"{body.merchant_prepare_id or ''}"
        f"{body.amount}{action}{body.sign_time}"
    )
    computed = hashlib.md5(sign_str.encode()).hexdigest()
    return computed == body.sign_string

@app.post("/payments/click/prepare")
async def click_prepare(body: ClickWebhook):
    """Click prepare step."""
    if not verify_click_sign(body, 0):
        return {"error": -1, "error_note": "Invalid sign"}
    order_id = int(body.merchant_trans_id or 0)
    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    if not order: conn.close(); return {"error": -5, "error_note": "Order not found"}
    if abs(order["total"] - body.amount) > 1: conn.close(); return {"error": -2, "error_note": "Wrong amount"}
    pay_id = conn.execute("""
        INSERT INTO payments (order_id,provider,provider_id,amount,status,raw)
        VALUES (?,?,?,?,?,?)
    """, (order_id,"click",str(body.click_trans_id),int(body.amount),"pending","{}")).lastrowid
    conn.commit(); conn.close()
    return {"click_trans_id": body.click_trans_id, "merchant_trans_id": body.merchant_trans_id,
            "merchant_prepare_id": pay_id, "error": 0, "error_note": "Success"}

@app.post("/payments/click/complete")
async def click_complete(body: ClickWebhook):
    """Click complete step."""
    if not verify_click_sign(body, 1):
        return {"error": -1, "error_note": "Invalid sign"}
    if body.error < 0:
        conn = get_db()
        conn.execute("UPDATE payments SET status='cancelled' WHERE id=?", (body.merchant_prepare_id,))
        conn.commit(); conn.close()
        return {"error": 0, "error_note": "Cancelled"}
    order_id = int(body.merchant_trans_id or 0)
    conn = get_db()
    conn.execute("UPDATE payments SET status='paid' WHERE id=?", (body.merchant_prepare_id,))
    conn.execute(
        "UPDATE orders SET payment_status='paid', payment_method='click', payment_id=? WHERE id=?",
        (str(body.click_trans_id), order_id)
    )
    conn.commit(); conn.close()
    return {"click_trans_id": body.click_trans_id, "merchant_trans_id": body.merchant_trans_id,
            "merchant_confirm_id": body.merchant_prepare_id, "error": 0, "error_note": "Success"}

@app.get("/payments/click/url/{order_id}")
def click_payment_url(order_id: int, x_init_data: Optional[str] = Header(None)):
    """Generate Click payment URL for an order."""
    user = get_user(x_init_data)
    if not user: raise HTTPException(401, "Auth required")
    conn = get_db()
    order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
    conn.close()
    if not order: raise HTTPException(404, "Order not found")
    url = (
        f"https://my.click.uz/services/pay"
        f"?service_id={CLICK_SERVICE_ID}"
        f"&merchant_id={CLICK_SERVICE_ID}"
        f"&amount={order['total']}"
        f"&transaction_param={order_id}"
        f"&return_url=https://t.me/"
    )
    return {"url": url, "amount": order["total"], "order_id": order_id}
