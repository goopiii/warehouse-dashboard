from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import ThreadPoolExecutor
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

# 캐시: 센터별 dashboard 전체 데이터 + 갱신 시각
cache = {code: {"data": None, "last_updated": None} for code in CENTERS}
REFRESH_INTERVAL = 600


# ── DB 유틸 ──────────────────────────────────────────────────────────────────
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


# ── 쿼리 빌더 ────────────────────────────────────────────────────────────────
def make_date_cond(target_date=None, start_date=None, end_date=None):
    from datetime import datetime, timedelta
    if start_date and end_date:
        dt_start = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
        dt_end   = datetime.strptime(end_date,   "%Y-%m-%d")
        return (f"SUBSTRING(o.cut_off_no, 1, 10)"
                f" BETWEEN '{dt_start.strftime('%Y%m%d')}23'"
                f" AND '{dt_end.strftime('%Y%m%d')}22'")
    elif target_date:
        from datetime import datetime, timedelta
        dt      = datetime.strptime(target_date, "%Y-%m-%d")
        dt_prev = (dt - timedelta(days=1)).strftime("%Y%m%d")
        dt_curr = dt.strftime("%Y%m%d")
        return (f"SUBSTRING(o.cut_off_no, 1, 10)"
                f" BETWEEN '{dt_prev}23' AND '{dt_curr}22'")
    # 오늘 기본
    return ("SUBSTRING(o.cut_off_no, 1, 10)"
            " BETWEEN CONCAT(DATE_FORMAT(DATE_SUB(CURRENT_DATE,1),'yyyyMMdd'),'23')"
            " AND CONCAT(DATE_FORMAT(CURRENT_DATE,'yyyyMMdd'),'22')")

def b2c_where(wh_id, date_cond):
    return f"""
    {date_cond}
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


# ── 실제 조회 (4개 쿼리 병렬) ────────────────────────────────────────────────
def fetch_dashboard(center_code, target_date=None, start_date=None, end_date=None):
    c         = CENTERS[center_code]
    wh_id     = c["id"]
    wh_nm     = c["wh_nm"]
    area_list = "'" + "','".join(c["areas"]) + "'"
    date_cond = make_date_cond(target_date=target_date, start_date=start_date, end_date=end_date)
    where     = b2c_where(wh_id, date_cond)
    ajoin     = area_join()

    # 예측치 날짜
    if target_date:
        fc_date_expr = f"'{target_date}'"
    else:
        fc_date_expr = "CURRENT_DATE"

    def q_total():
        return query(
            f"SELECT SUM(o.total_planned_quantity) AS total_qty"
            f" FROM `pbo-rt`.logistics.outbound o"
            f" JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id"
            f" WHERE {where}"
        )

    def q_summary():
        return query(
            f"SELECT SUM(oa.quantity) AS alloc_qty,"
            f" SUM(CASE WHEN oi.picking_status='COMPLETE' THEN oa.quantity ELSE 0 END) AS pick_qty,"
            f" SUM(CASE WHEN oi.packing_status='COMPLETE' THEN oa.quantity ELSE 0 END) AS pack_qty"
            f" FROM `pbo-rt`.logistics.outbound_assign oa"
            f" JOIN `pbo-rt`.logistics.outbound_item oi ON oi.id = oa.outbound_item_id"
            f" JOIN `pbo-rt`.logistics.outbound o ON o.id = oi.outbound_id"
            f" JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id"
            f" {ajoin}"
            f" WHERE {where} AND a.code IN ({area_list}) AND a.mst_warehouse_id = {wh_id}"
        )

    def q_floors():
        return query(
            f"SELECT a.code AS floor, a.title AS floor_title,"
            f" SUM(oa.quantity) AS total_qty, SUM(oa.quantity) AS alloc_qty,"
            f" SUM(CASE WHEN oi.picking_status='COMPLETE' THEN oa.quantity ELSE 0 END) AS pick_qty,"
            f" SUM(CASE WHEN oi.picking_status!='COMPLETE' THEN oa.quantity ELSE 0 END) AS unpick_qty,"
            f" SUM(CASE WHEN oi.packing_status='COMPLETE' THEN oa.quantity ELSE 0 END) AS pack_qty,"
            f" SUM(CASE WHEN oi.packing_status!='COMPLETE' THEN oa.quantity ELSE 0 END) AS unpack_qty"
            f" FROM `pbo-rt`.logistics.outbound_assign oa"
            f" JOIN `pbo-rt`.logistics.outbound_item oi ON oi.id = oa.outbound_item_id"
            f" JOIN `pbo-rt`.logistics.outbound o ON o.id = oi.outbound_id"
            f" JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id"
            f" {ajoin}"
            f" WHERE {where} AND a.code IN ({area_list}) AND a.mst_warehouse_id = {wh_id}"
            f" GROUP BY a.code, a.title ORDER BY a.code"
        )

    def q_forecast():
        # 누적 범위 조회 시 예측치는 의미 없으므로 skip
        if start_date and end_date:
            return [{"fcst_total": None}]
        return query(
            f"SELECT SUM(fcst) AS fcst_total"
            f" FROM TEAM.logistics.raw_logistics_snop_fc_forecast_daily"
            f" WHERE dt = {fc_date_expr} AND wh_nm = '{wh_nm}' AND fcst > 0"
        )

    # 4개 병렬 실행
    with ThreadPoolExecutor(max_workers=4) as executor:
        f_total    = executor.submit(q_total)
        f_summary  = executor.submit(q_summary)
        f_floors   = executor.submit(q_floors)
        f_forecast = executor.submit(q_forecast)
        total_rows    = f_total.result()
        summary_rows  = f_summary.result()
        floors_rows   = f_floors.result()
        forecast_rows = f_forecast.result()

    total_qty = total_rows[0]["total_qty"] if total_rows else 0
    summary   = summary_rows[0] if summary_rows else {}
    summary["total_qty"] = total_qty
    fcst_total = forecast_rows[0]["fcst_total"] if forecast_rows else None

    return {
        "status":    "ok",
        "summary":   summary,
        "floors":    floors_rows,
        "dongs":     c["dongs"],
        "fcst_total": fcst_total,
    }


# ── 전체 센터 한번에 조회 (overview 전용) ────────────────────────────────────
def fetch_dashboard_all(target_date=None, start_date=None, end_date=None):
    """센터 5개를 쿼리 3개로 한번에 조회. GROUP BY mst_warehouse_id."""
    all_ids   = [c["id"] for c in CENTERS.values()]
    id_list   = ",".join(str(i) for i in all_ids)
    date_cond = make_date_cond(target_date=target_date, start_date=start_date, end_date=end_date)
    ajoin     = area_join()
    all_areas = list({a for c in CENTERS.values() for a in c["areas"]})
    area_list = "'" + "','".join(all_areas) + "'"

    where = f"""
        {date_cond}
        AND o.mst_warehouse_id IN ({id_list})
        AND o.order_type NOT IN ('IN_HOUSE','ETC')
        AND o.delivery_type != 'LOADED_FREIGHT'
        AND o.outbound_status != 'CANCEL'
        AND CONCAT(o.assign_status, o.picking_status, o.packing_status, o.outbound_status)
            != 'NOTHINGREADYNOTHINGCREATED_WAVE'
        AND s.shipper_name != 'MUSINSA_USED'
    """

    def q_total():
        return query(
            f"SELECT o.mst_warehouse_id, SUM(o.total_planned_quantity) AS total_qty"
            f" FROM `pbo-rt`.logistics.outbound o"
            f" JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id"
            f" WHERE {where} GROUP BY o.mst_warehouse_id"
        )

    def q_summary():
        return query(
            f"SELECT o.mst_warehouse_id,"
            f" SUM(oa.quantity) AS alloc_qty,"
            f" SUM(CASE WHEN oi.picking_status='COMPLETE' THEN oa.quantity ELSE 0 END) AS pick_qty,"
            f" SUM(CASE WHEN oi.packing_status='COMPLETE' THEN oa.quantity ELSE 0 END) AS pack_qty"
            f" FROM `pbo-rt`.logistics.outbound_assign oa"
            f" JOIN `pbo-rt`.logistics.outbound_item oi ON oi.id = oa.outbound_item_id"
            f" JOIN `pbo-rt`.logistics.outbound o ON o.id = oi.outbound_id"
            f" JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id"
            f" {ajoin}"
            f" WHERE {where} AND a.code IN ({area_list})"
            f" GROUP BY o.mst_warehouse_id"
        )

    def q_forecast():
        if start_date and end_date:
            return []
        fc_expr  = f"\'{target_date}\'" if target_date else "CURRENT_DATE"
        wh_list  = "'" + "','".join(c["wh_nm"] for c in CENTERS.values()) + "'"
        return query(
            f"SELECT wh_nm, SUM(fcst) AS fcst_total"
            f" FROM TEAM.logistics.raw_logistics_snop_fc_forecast_daily"
            f" WHERE dt = {fc_expr} AND wh_nm IN ({wh_list}) AND fcst > 0"
            f" GROUP BY wh_nm"
        )

    with ThreadPoolExecutor(max_workers=3) as executor:
        f_t = executor.submit(q_total)
        f_s = executor.submit(q_summary)
        f_f = executor.submit(q_forecast)
        total_rows    = f_t.result()
        summary_rows  = f_s.result()
        forecast_rows = f_f.result()

    total_map   = {r["mst_warehouse_id"]: r["total_qty"] for r in total_rows}
    summary_map = {r["mst_warehouse_id"]: r              for r in summary_rows}
    fcst_map    = {r["wh_nm"]: r["fcst_total"]           for r in forecast_rows}

    result = {}
    for code, c in CENTERS.items():
        wh_id = c["id"]
        total = int(total_map.get(wh_id) or 0)
        s     = summary_map.get(wh_id, {})
        alloc = int(s.get("alloc_qty") or 0)
        pick  = int(s.get("pick_qty")  or 0)
        pack  = int(s.get("pack_qty")  or 0)
        fcst  = int(fcst_map.get(c["wh_nm"]) or 0)
        result[code] = {
            "code":        code,
            "title":       c["title"],
            "status":      "ok",
            "total_qty":   total,
            "alloc_qty":   alloc,
            "pick_qty":    pick,
            "pack_qty":    pack,
            "fcst_total":  fcst,
            "unalloc_qty": total - alloc,
            "unpick_qty":  alloc - pick,
            "unpack_qty":  alloc - pack,
            "alloc_pct":   round(alloc / total * 100) if total else 0,
            "pick_pct":    round(pick  / alloc * 100) if alloc else 0,
            "pack_pct":    round(pack  / alloc * 100) if alloc else 0,
            "fcst_pct":    round(total / fcst  * 100) if fcst  else 0,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
    return result


# ── 백그라운드 캐시 갱신 ──────────────────────────────────────────────────────
def refresh_all():
    while True:
        for code in CENTERS:
            try:
                logger.info(f"캐시 갱신: {code}")
                cache[code]["data"]         = fetch_dashboard(code)
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


# ── API ───────────────────────────────────────────────────────────────────────
@app.get("/api/dashboard")
def get_dashboard(center: str = Query(default="NWH01"), date: str = Query(default="")):
    """오늘 탭 전용. 오늘 날짜 → 캐시 즉시 반환 / 다른 날짜 → 직접 조회."""
    if center not in CENTERS:
        return {"status": "error", "message": "센터 코드 오류"}
    today = time.strftime("%Y-%m-%d")
    # 오늘 날짜(또는 date 미전달) → 캐시 반환
    if not date or date == today:
        if cache[center]["data"] is None:
            try:
                cache[center]["data"]         = fetch_dashboard(center)
                cache[center]["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                return {"status": "error", "message": str(e)}
        return {**cache[center]["data"], "last_updated": cache[center]["last_updated"]}
    # 다른 날짜 → 직접 조회
    try:
        data = fetch_dashboard(center, target_date=date)
        return {**data, "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"), "selected_date": date}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/dashboard_range")
def get_dashboard_range(center: str = Query(default="NWH01"),
                        start: str = Query(...),
                        end:   str = Query(...)):
    """누적 탭 전용. 날짜 범위 직접 조회, 캐시 없음."""
    if center not in CENTERS:
        return {"status": "error", "message": "센터 코드 오류"}
    try:
        data = fetch_dashboard(center, start_date=start, end_date=end)
        return {**data, "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
                "start_date": start, "end_date": end}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 하위 호환 유지 (기존 /api/b2c, /api/forecast 엔드포인트)
@app.get("/api/b2c")
def get_b2c(center: str = Query(default="NWH01"), date: str = Query(default="")):
    return get_dashboard(center=center, date=date)

@app.get("/api/forecast")
def get_forecast(center: str = Query(default="NWH01"), date: str = Query(default="")):
    return get_dashboard(center=center, date=date)

@app.get("/api/b2c_range")
def get_b2c_range(center: str = Query(default="NWH01"),
                  start: str = Query(...), end: str = Query(...)):
    return get_dashboard_range(center=center, start=start, end=end)

@app.get("/api/centers")
def get_centers():
    return [{"code": k, "title": v["title"]} for k, v in CENTERS.items()]

@app.get("/api/status")
def get_status():
    return {k: {"last_updated": v["last_updated"], "has_cache": v["data"] is not None}
            for k, v in cache.items()}

@app.get("/api/overview")
def get_overview(date: str = Query(default="")):
    """전체 센터 종합 현황.
    date 미전달 → 캐시 즉시 반환.
    date 전달(오늘 포함) → 전체 센터 병렬 직접 조회."""

    def make_item(code, d, upd):
        c = CENTERS[code]
        if d is None:
            return {"code": code, "title": c["title"], "status": "loading"}
        s     = d.get("summary", {})
        total = int(s.get("total_qty")  or 0)
        alloc = int(s.get("alloc_qty")  or 0)
        pick  = int(s.get("pick_qty")   or 0)
        pack  = int(s.get("pack_qty")   or 0)
        fcst  = int(d.get("fcst_total") or 0)
        return {
            "code":        code,
            "title":       c["title"],
            "status":      "ok",
            "total_qty":   total,
            "alloc_qty":   alloc,
            "pick_qty":    pick,
            "pack_qty":    pack,
            "fcst_total":  fcst,
            "unalloc_qty": total - alloc,
            "unpick_qty":  alloc - pick,
            "unpack_qty":  alloc - pack,
            "alloc_pct":   round(alloc / total * 100) if total else 0,
            "pick_pct":    round(pick  / alloc * 100) if alloc else 0,
            "pack_pct":    round(pack  / alloc * 100) if alloc else 0,
            "fcst_pct":    round(total / fcst  * 100) if fcst  else 0,
            "last_updated": upd,
        }

    # date 미전달 → 캐시 즉시 반환
    if not date:
        return [make_item(code, cache[code]["data"], cache[code]["last_updated"])
                for code in CENTERS]

    # date 전달 → 전체 센터 한번에 조회 (쿼리 3개)
    try:
        results = fetch_dashboard_all(target_date=date)
        return [results[code] for code in CENTERS]
    except Exception as e:
        return [{"code": code, "title": CENTERS[code]["title"],
                 "status": "error", "message": str(e)} for code in CENTERS]

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open("C:/dashboard/index.html", encoding="utf-8") as f:
        return f.read()

@app.get("/api/overview_range")
def get_overview_range(start: str = Query(...), end: str = Query(...)):
    """전체 센터 누적 범위 조회. 쿼리 3개로 한번에 처리."""
    try:
        results = fetch_dashboard_all(start_date=start, end_date=end)
        return [results[code] for code in CENTERS]
    except Exception as e:
        return [{"code": code, "title": CENTERS[code]["title"],
                 "status": "error", "message": str(e)} for code in CENTERS]

@app.get("/overview", response_class=HTMLResponse)
def overview():
    with open("C:/dashboard/overview.html", encoding="utf-8") as f:
        return f.read()
