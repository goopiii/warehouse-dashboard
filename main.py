from fastapi import FastAPI, Query
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

# 센터 마스터
# dongs: 구역을 그룹핑할 동 구성 {"동이름": ["구역코드",...]}
CENTERS = {
    "NWH01": {"id": 1,  "title": "신여주1", "areas": ["A1","A2","A3","B1","B2","B3"],
              "dongs": {"A동": ["A1","A2","A3"], "B동": ["B1","B2","B3"]}, "wh_nm": "신여주1"},
    "NWH02": {"id": 5,  "title": "신여주2", "areas": ["A1","A3"],
              "dongs": {"A동": ["A1","A3"]}, "wh_nm": "신여주2"},
    "NWH03": {"id": 3,  "title": "신여주3", "areas": ["A1","A2","A3","A4","A5","A6","B1","B2"],
              "dongs": {"A동": ["A1","A2","A3","A4","A5","A6"], "B동": ["B1","B2"]}, "wh_nm": "신여주3"},
    "YEJ04": {"id": 12, "title": "신여주4", "areas": ["A2","A3","B2"],
              "dongs": {"A동": ["A2","A3"], "B동": ["B2"]}, "wh_nm": "신여주4"},
    "ANS01": {"id": 17, "title": "안성1",   "areas": ["A1","A2","A3"],
              "dongs": {"A동": ["A1","A2","A3"]}, "wh_nm": "안성1"},
}

cache = {}
for code in CENTERS:
    cache[code] = {"b2c": None, "forecast": None, "last_updated": None}

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

def b2c_where(wh_id):
    return f"""
    SUBSTRING(o.cut_off_no, 1, 10)
        BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd'), '23')
            AND CONCAT(DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd'), '22')
    AND o.mst_warehouse_id = {wh_id}
    AND o.order_type NOT IN ('IN_HOUSE','ETC')
    AND o.delivery_type != 'LOADED_FREIGHT'
    AND o.outbound_status != 'CANCEL'
    AND CONCAT(o.assign_status, o.picking_status, o.packing_status, o.outbound_status)
        != 'NOTHINGREADYNOTHINGCREATED_WAVE'
    AND s.shipper_name != 'MUSINSA_USED'
    """

def area_join():
    return """
    JOIN pbo.logistics.mst_location l ON l.id = oa.mst_location_id
    JOIN pbo.logistics.mst_zone z ON z.id = l.mst_zone_id
    JOIN pbo.logistics.mst_area a ON a.id = z.mst_area_id
    """

def fetch_b2c(center_code, target_date=None):
    c = CENTERS[center_code]
    wh_id = c["id"]
    areas = c["areas"]
    area_list = "'" + "','".join(areas) + "'"
    where = b2c_where(wh_id)
    ajoin = area_join()

    # 날짜별 무배당발 조건 생성
    if target_date:
        from datetime import datetime, timedelta
        dt = datetime.strptime(target_date, "%Y-%m-%d")
        dt_prev = (dt - timedelta(days=1)).strftime("%Y%m%d")
        dt_curr = dt.strftime("%Y%m%d")
        date_cond = f"SUBSTRING(o.cut_off_no, 1, 10) BETWEEN '{dt_prev}23' AND '{dt_curr}22'"
        where = where.replace(
            "SUBSTRING(o.cut_off_no, 1, 10)\n        BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyyMMdd'), '23')\n            AND CONCAT(DATE_FORMAT(CURRENT_DATE, 'yyyyMMdd'), '22')",
            date_cond
        )

    total = query(f"""
        SELECT SUM(o.total_planned_quantity) AS total_qty
        FROM `pbo-rt`.logistics.outbound o
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        WHERE {where}
    """)
    total_qty = total[0]["total_qty"] if total else 0

    summary = query(f"""
        SELECT
            SUM(oa.quantity) AS alloc_qty,
            SUM(CASE WHEN oi.picking_status = 'COMPLETE' THEN oa.quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN oi.packing_status = 'COMPLETE' THEN oa.quantity ELSE 0 END) AS pack_qty
        FROM `pbo-rt`.logistics.outbound_assign oa
        JOIN `pbo-rt`.logistics.outbound_item oi ON oi.id = oa.outbound_item_id
        JOIN `pbo-rt`.logistics.outbound o ON o.id = oi.outbound_id
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        {ajoin}
        WHERE {where}
          AND a.code IN ({area_list})
          AND a.mst_warehouse_id = {wh_id}
    """)

    result = summary[0] if summary else {}
    result["total_qty"] = total_qty

    floors = query(f"""
        SELECT
            a.code AS floor,
            a.title AS floor_title,
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
        {ajoin}
        WHERE {where}
          AND a.code IN ({area_list})
          AND a.mst_warehouse_id = {wh_id}
        GROUP BY a.code, a.title
        ORDER BY a.code
    """)

    return {
        "status": "ok",
        "summary": result,
        "floors": floors,
        "dongs": c["dongs"]
    }

def fetch_forecast(center_code, target_date=None):
    c = CENTERS[center_code]
    wh_nm = c["wh_nm"]
    fc_date = target_date if target_date else "CURRENT_DATE"
    fc_date_expr = f"'{fc_date}'" if target_date else "CURRENT_DATE"
    forecast = query(f"""
        SELECT SUM(fcst) AS fcst_total
        FROM TEAM.logistics.raw_logistics_snop_fc_forecast_daily
        WHERE dt = {fc_date_expr}
          AND wh_nm = '{wh_nm}'
          AND fcst > 0
    """)
    return {"status": "ok", "forecast": forecast[0] if forecast else {}}

def refresh_all():
    while True:
        for code in CENTERS:
            try:
                logger.info(f"캐시 갱신: {code}")
                cache[code]["b2c"] = fetch_b2c(code)
                cache[code]["forecast"] = fetch_forecast(code)
                cache[code]["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"완료: {code}")
            except Exception as e:
                logger.error(f"실패 {code}: {e}")
        time.sleep(REFRESH_INTERVAL)

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=refresh_all, daemon=True)
    thread.start()
    logger.info("백그라운드 캐시 갱신 시작")

@app.get("/api/b2c")
def get_b2c(center: str = Query(default="NWH01"), date: str = Query(default="")):
    if center not in CENTERS:
        return {"status": "error", "message": "센터 코드 오류"}
    # 날짜 선택 시 캐시 무시하고 직접 조회
    if date and date != time.strftime("%Y-%m-%d"):
        try:
            data = fetch_b2c(center, date)
            return {**data, "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"), "selected_date": date}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    if cache[center]["b2c"] is None:
        try:
            cache[center]["b2c"] = fetch_b2c(center)
            cache[center]["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return {"status": "error", "message": str(e)}
    return {**cache[center]["b2c"], "last_updated": cache[center]["last_updated"]}

@app.get("/api/forecast")
def get_forecast(center: str = Query(default="NWH01"), date: str = Query(default="")):
    if center not in CENTERS:
        return {"status": "error", "message": "센터 코드 오류"}
    today = time.strftime("%Y-%m-%d")
    target_date = date if date else today
    # 날짜가 있으면 항상 직접 조회 (예측치는 날짜마다 다름)
    if target_date != today or cache[center]["forecast"] is None:
        try:
            fc = fetch_forecast(center, target_date if target_date != today else None)
            b2c_data = cache[center]["b2c"] if target_date == today else fetch_b2c(center, target_date)
            total = b2c_data["summary"]["total_qty"] if b2c_data else 0
            return {**fc, "total_qty": total, "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"), "selected_date": target_date}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    fc = cache[center]["forecast"]
    total = cache[center]["b2c"]["summary"]["total_qty"] if cache[center]["b2c"] else 0
    return {**fc, "total_qty": total, "last_updated": cache[center]["last_updated"], "selected_date": today}

@app.get("/api/centers")
def get_centers():
    return [{"code": k, "title": v["title"]} for k, v in CENTERS.items()]

@app.get("/api/status")
def get_status():
    return {k: {"last_updated": v["last_updated"], "has_cache": v["b2c"] is not None}
            for k, v in cache.items()}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open("C:/dashboard/index.html", encoding="utf-8") as f:
        return f.read()
