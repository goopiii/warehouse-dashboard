from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import databricks.sql, os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.get("/api/inbound")
def get_inbound():
    rows = query("""
        SELECT
            COUNT(DISTINCT ip.id)                                         AS asn_count,
            SUM(ip.total_quantity)                                        AS planned_qty,
            SUM(CASE WHEN f.id IS NOT NULL THEN f.fixed_quantity ELSE 0 END) AS fixed_qty,
            SUM(ip.total_quantity)
              - SUM(CASE WHEN f.id IS NOT NULL THEN f.fixed_quantity ELSE 0 END) AS remaining_qty
        FROM pbo.logistics.inbound_plan ip
        LEFT JOIN pbo.logistics.inbound_fixed f
            ON f.inbound_plan_id = ip.id
            AND DATE(f.inbound_fixed_at) = CURRENT_DATE
        WHERE DATE(ip.expected_date) = CURRENT_DATE
          AND ip.mst_warehouse_id = 1
    """)
    inbound = rows[0] if rows else {}

    putaway = query("""
        SELECT
            SUM(CASE WHEN l.code LIKE 'RECV%' THEN i.total_qty ELSE 0 END) AS recv_qty,
            SUM(CASE WHEN h.inventory_history_type = 'PUTAWAY'
                      AND DATE(h.tx_date) = CURRENT_DATE THEN h.quantity ELSE 0 END) AS putaway_qty
        FROM pbo.logistics.inventory i
        JOIN pbo.logistics.mst_location l ON l.id = i.mst_location_id
        LEFT JOIN pbo.logistics.inventory_history h ON h.mst_lot_id = i.mst_lot_id
        WHERE i.mst_warehouse_id = 1
    """)
    putaway_data = putaway[0] if putaway else {}

    issues = query("""
        SELECT
            ip.inbound_plan_number,
            s.shipper_name,
            ip.total_quantity                                              AS planned_qty,
            COALESCE(SUM(f.fixed_quantity), 0)                            AS fixed_qty,
            ip.total_quantity - COALESCE(SUM(f.fixed_quantity), 0)        AS remaining_qty,
            CASE WHEN COALESCE(SUM(f.fixed_quantity), 0) = 0 THEN '미시작'
                 ELSE '지연' END                                           AS issue_type
        FROM pbo.logistics.inbound_plan ip
        JOIN pbo.logistics.mst_shipper s ON s.id = ip.mst_shipper_id
        LEFT JOIN pbo.logistics.inbound_fixed f
            ON f.inbound_plan_id = ip.id
            AND DATE(f.inbound_fixed_at) = CURRENT_DATE
        WHERE DATE(ip.expected_date) = CURRENT_DATE
          AND ip.mst_warehouse_id = 1
        GROUP BY ip.id, ip.inbound_plan_number, s.shipper_name, ip.total_quantity
        HAVING ip.total_quantity - COALESCE(SUM(f.fixed_quantity), 0) > 0
        ORDER BY remaining_qty DESC
        LIMIT 10
    """)

    return {
        "status": "ok",
        "inbound": inbound,
        "putaway": putaway_data,
        "issues": issues
    }

@app.get("/api/b2c")
def get_b2c():
    summary = query("""
        SELECT
            SUM(o.total_planned_quantity)                                              AS total_qty,
            SUM(CASE WHEN o.assign_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS alloc_qty,
            SUM(CASE WHEN o.picking_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN o.packing_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pack_qty
        FROM pbo.logistics.outbound o
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        WHERE DATE(o.ordered_date) = CURRENT_DATE
          AND o.mst_warehouse_id = 1
          AND o.order_type NOT IN ('IN_HOUSE','ETC')
          AND o.mst_warehouse_id != 2
          AND o.delivery_type != 'LOADED_FREIGHT'
          AND o.outbound_status != 'CANCEL'
          AND CONCAT(o.assign_status, o.picking_status, o.packing_status, o.outbound_status)
              != 'NOTHINGREADYNOTHINGCREATED_WAVE'
          AND s.shipper_name != 'MUSINSA_USED'
    """)

    floors = query("""
        SELECT
            SUBSTRING(l.code, 1, 2)                                                   AS floor,
            SUM(o.total_planned_quantity)                                              AS total_qty,
            SUM(CASE WHEN o.assign_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS alloc_qty,
            SUM(CASE WHEN o.picking_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN o.packing_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pack_qty
        FROM pbo.logistics.outbound o
        JOIN pbo.logistics.mst_shipper s ON s.id = o.mst_shipper_id
        JOIN pbo.logistics.outbound_item oi ON oi.outbound_id = o.id
        JOIN pbo.logistics.mst_location l ON l.id = oi.mst_location_id
        WHERE DATE(o.ordered_date) = CURRENT_DATE
          AND o.mst_warehouse_id = 1
          AND o.order_type NOT IN ('IN_HOUSE','ETC')
          AND o.delivery_type != 'LOADED_FREIGHT'
          AND o.outbound_status != 'CANCEL'
          AND CONCAT(o.assign_status, o.picking_status, o.packing_status, o.outbound_status)
              != 'NOTHINGREADYNOTHINGCREATED_WAVE'
          AND s.shipper_name != 'MUSINSA_USED'
          AND SUBSTRING(l.code, 1, 2) IN ('A1','A2','A3','B1','B2','B3')
        GROUP BY SUBSTRING(l.code, 1, 2)
        ORDER BY floor
    """)

    return {"status": "ok", "summary": summary[0] if summary else {}, "floors": floors}

@app.get("/api/b2b")
def get_b2b():
    summary = query("""
        SELECT
            SUM(o.total_planned_quantity)                                              AS total_qty,
            SUM(CASE WHEN o.picking_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN o.packing_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pack_qty,
            SUM(CASE WHEN o.outbound_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS ship_qty,
            COUNT(DISTINCT o.to_shop_code)                                            AS store_count
        FROM pbo.logistics.outbound o
        WHERE DATE(o.planned_date) = CURRENT_DATE
          AND o.mst_warehouse_id = 1
          AND o.outbound_type = 'IN_HOUSE'
          AND o.delivery_type = 'LOADED_FREIGHT'
          AND o.group_name LIKE '%그룹%'
          AND o.outbound_status != 'CANCEL'
    """)

    groups = query("""
        SELECT
            o.group_name,
            SUM(o.total_planned_quantity)                                              AS total_qty,
            SUM(CASE WHEN o.picking_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pick_qty,
            SUM(CASE WHEN o.packing_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS pack_qty,
            SUM(CASE WHEN o.outbound_status = 'COMPLETE' THEN o.total_planned_quantity ELSE 0 END) AS ship_qty
        FROM pbo.logistics.outbound o
        WHERE DATE(o.planned_date) = CURRENT_DATE
          AND o.mst_warehouse_id = 1
          AND o.outbound_type = 'IN_HOUSE'
          AND o.delivery_type = 'LOADED_FREIGHT'
          AND o.group_name LIKE '%그룹%'
          AND o.outbound_status != 'CANCEL'
        GROUP BY o.group_name
        ORDER BY total_qty DESC
    """)

    return {"status": "ok", "summary": summary[0] if summary else {}, "groups": groups}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open("C:/dashboard/index.html", encoding="utf-8") as f:
        return f.read()
