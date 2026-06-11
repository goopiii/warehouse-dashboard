from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import databricks.sql, os, threading, time, logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

cache = {"b2c": None, "forecast": None, "last_updated": None, "is_loading": False}
REFRESH_INTERVAL = 600

def query(sql):
    with databricks.sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

B2C_WHERE = """
    SUBSTRING(o.cut_off_no, 1, 10)
        BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd'), '23')
            AND CONCAT(DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd'), '22')
    AND o.mst_warehouse_id = 1
    AND o.order_type NOT IN ('IN_HOUSE','ETC')
    AND o.delivery_type != 'LOADED_FREIGHT'
    AND o.outbound_status != 'CANCEL'
    AND CONCAT(o.assign_status, o.picking_status, o.packing_status, o.outbound_status)
        != 'NOTHINGREADYNOTHINGCREATED_WAVE'
    AND s.shipper_name != 'MUSINSA_USED'
"""

AREA_JOIN = """
    JOIN pbo.logistics.mst_location l ON l.id = oa.mst_location_id
    JOIN pbo.logistics.mst_zone z ON z.id = l.mst_zone_id
    JOIN pbo.logistics.mst_area a ON a.id = z.mst_area_id
"""

AREA_WHERE = """
    AND a.code IN ('A1','A2','A3','B1','B2','B3')
    AND a.mst_warehouse_id = 1
"""

def fetch_b2c():
    # 총 주문수량
    total = query(f"""
        SELECT SUM(o.total_planned_quantity) AS total_qty
        FROM `pbo-rt`.logistics.outbound o
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        WHERE {B2C_WHERE}
    """)
    total_qty = total[0]["total_qty"] if total else 0

    # 할당/피킹/패킹
    summary = query(f"""
        SELECT
            SUM(oa.quantity) AS alloc_qty,
            SUM(CASE WHEN oi.picking_status = 'COMPLETE' THEN oa.quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN oi.packing_status = 'COMPLETE' THEN oa.quantity ELSE 0 END) AS pack_qty
        FROM `pbo-rt`.logistics.outbound_assign oa
        JOIN `pbo-rt`.logistics.outbound_item oi ON oi.id = oa.outbound_item_id
        JOIN `pbo-rt`.logistics.outbound o ON o.id = oi.outbound_id
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        {AREA_JOIN}
        WHERE {B2C_WHERE}
          {AREA_WHERE}
    """)

    result = summary[0] if summary else {}
    result["total_qty"] = total_qty

    # 구역별
    floors = query(f"""
        SELECT
            a.code AS floor,
            SUM(oa.quantity) AS total_qty,
            SUM(oa.quantity) AS alloc_qty,
            SUM(CASE WHEN oi.picking_status = 'COMPLETE' THEN oa.quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN oi.picking_status != 'COMPLETE' THEN oa.quantity ELSE 0 END) AS unpick_qty,
            SUM(CASE WHEN oi.packing_status = 'COMPLETE' THEN oa.quantity ELSE 0 END) AS pack_qty,
            SUM(CASE WHEN oi.packing_status != 'COMPLETE' THEN oa.quantity ELSE 0 END) AS unpack_qty
        FROM `pbo-rt`.logistics.outbound_assign oa
        JOIN `pbo-rt`.logistics.outbound_item oi ON oi.id = oa.outbound_item_id
        JOIN `pbo-rt`.logistics.outbound o ON o.id = oi.outbound_id
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        {AREA_JOIN}
        WHERE {B2C_WHERE}
          {AREA_WHERE}
        GROUP BY a.code
        ORDER BY a.code
    """)

    return {"status": "ok", "summary": result, "floors": floors}

def fetch_forecast():
    forecast = query("""
        SELECT
            SUM(fcst) AS fcst_total,
            SUM(CASE WHEN ord_type = 'MUSINSA' THEN fcst ELSE 0 END) AS fcst_musinsa,
            SUM(CASE WHEN ord_type = 'MFS' THEN fcst ELSE 0 END) AS fcst_mfs
        FROM TEAM.logistics.raw_logistics_snop_fc_forecast_daily
        WHERE dt = CURRENT_DATE
          AND wh_nm = '신여주1'
          AND fcst > 0
    """)
    return {"status": "ok", "forecast": forecast[0] if forecast else {}}

def refresh_cache():
    while True:
        try:
            logger.info("캐시 갱신 시작...")
            cache["is_loading"] = True
            cache["b2c"] = fetch_b2c()
            cache["forecast"] = fetch_forecast()
            cache["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            cache["is_loading"] = False
            logger.info(f"캐시 갱신 완료: {cache['last_updated']}")
        except Exception as e:
            cache["is_loading"] = False
            logger.error(f"캐시 갱신 실패: {e}")
        time.sleep(REFRESH_INTERVAL)

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=refresh_cache, daemon=True)
    thread.start()
    logger.info("백그라운드 캐시 갱신 스레드 시작")

@app.get("/api/b2c")
def get_b2c():
    if cache["b2c"] is None:
        try:
            cache["b2c"] = fetch_b2c()
            cache["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return {"status": "error", "message": str(e)}
    return {**cache["b2c"], "last_updated": cache["last_updated"], "from_cache": True}

@app.get("/api/forecast")
def get_forecast():
    if cache["forecast"] is None:
        try:
            cache["forecast"] = fetch_forecast()
        except Exception as e:
            return {"status": "error", "message": str(e)}
    fc = cache["forecast"]
    total = cache["b2c"]["summary"]["total_qty"] if cache["b2c"] else 0
    return {**fc, "total_qty": total, "last_updated": cache["last_updated"]}

@app.get("/api/status")
def get_status():
    return {
        "last_updated": cache["last_updated"],
        "is_loading": cache["is_loading"],
        "has_cache": cache["b2c"] is not None
    }

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open("C:/dashboard/index.html", encoding="utf-8") as f:
        return f.read()
