import os
import jinja2
import pandas as pd
import json
import gc
import re
import zipfile
import unicodedata
import random
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect, or_, and_
from itertools import zip_longest
from collections import defaultdict

# ==============================================================================
# 1. APP CONFIGURATION
# ==============================================================================

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 280, 'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==============================================================================
# 2. UTILS
# ==============================================================================

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    s = ''
    for c in input_str:
        if c in s1: s += s0[s1.index(c)]
        else: s += c
    return s

def clean_header(col_name):
    col_name = str(col_name).strip()
    special_map = {
        'ENodeBID': 'enodeb_id', 'gNodeB ID': 'gnodeb_id', 'GNODEB_ID': 'gnodeb_id',
        'CELL_ID': 'cell_id', 'SITE_NAME': 'site_name', 'CELL_NAME': 'cell_name',
        'Frenquency': 'frequency', 'Frequency': 'frequency',
        'PCI': 'pci', 'TAC': 'tac', 'MIMO': 'mimo',
        'UL Traffic Volume (GB)': 'ul_traffic_volume_gb',
        'DL Traffic Volume (GB)': 'dl_traffic_volume_gb',
        'Total Data Traffic Volume (GB)': 'traffic',
        'Cell Uplink Average Throughput': 'cell_uplink_average_throughput',
        'Cell Downlink Average Throughput': 'cell_downlink_average_throughput',
        'A User Downlink Average Throughput': 'user_dl_avg_throughput',
        'Cell avaibility rate': 'cell_avaibility_rate',
        'SgNB Addition Success Rate': 'sgnb_addition_success_rate',
        'SgNB Abnormal Release Rate': 'sgnb_abnormal_release_rate',
        'CQI_5G': 'cqi_5g', 'CQI_4G': 'cqi_4g',
        'POI': 'poi_name', 'Cell_Code': 'cell_code', 'Site_Code': 'site_code'
    }
    
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper: return val

    no_accent = remove_accents(col_name)
    lower = no_accent.lower()
    clean = re.sub(r'[^a-z0-9]', '_', lower)
    clean = re.sub(r'_+', '_', clean)
    
    common_map = {
        'hang_sx': 'hang_sx', 'ghi_chu': 'ghi_chu', 'dong_bo': 'dong_bo',
        'ten_cell': 'ten_cell', 'thoi_gian': 'thoi_gian', 'nha_cung_cap': 'nha_cung_cap',
        'traffic_vol_dl': 'traffic_vol_dl', 'res_blk_dl': 'res_blk_dl',
        'pstraffic': 'pstraffic', 'csconges': 'csconges', 'psconges': 'psconges',
        'cs_so_att': 'cs_so_att', 'ps_so_att': 'ps_so_att',
        'service_drop_all': 'service_drop_all', 'user_dl_avg_thput': 'user_dl_avg_thput',
        'poi': 'poi_name', 'cell_code': 'cell_code', 'site_code': 'site_code'
    }
    return common_map.get(clean, clean)

def generate_colors(n):
    base = ['#0078d4', '#107c10', '#d13438', '#ffaa44', '#00bcf2', '#5c2d91', '#e3008c', '#b4009e']
    if n <= len(base): return base[:n]
    return base + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base))]

# ==============================================================================
# 3. MODELS
# ==============================================================================

class User(UserMixin, db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)

class RF3G(db.Model):
    __tablename__ = 'rf_3g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    
    # Optional fields to prevent errors if missing
    psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    bsc_lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    hang_sx = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF4G(db.Model):
    __tablename__ = 'rf_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    
    dl_uarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF5G(db.Model):
    __tablename__ = 'rf_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50), index=True)
    site_code = db.Column(db.String(50))
    site_name = db.Column(db.String(100)) # 5G often uses site_name
    csht_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    antena = db.Column(db.String(100))
    azimuth = db.Column(db.Integer)
    total_tilt = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    
    nrarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    dong_bo = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class POI4G(db.Model):
    __tablename__ = 'poi_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200), index=True)

class POI5G(db.Model):
    __tablename__ = 'poi_5g'
    id = db.Column(db.Integer, primary_key=True)
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    poi_name = db.Column(db.String(200), index=True)

class KPI3G(db.Model):
    __tablename__ = 'kpi_3g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    pstraffic = db.Column(db.Float)
    cssr = db.Column(db.Float)
    dcr = db.Column(db.Float)
    ps_cssr = db.Column(db.Float)
    ps_dcr = db.Column(db.Float)
    hsdpa_throughput = db.Column(db.Float)
    hsupa_throughput = db.Column(db.Float)
    cs_so_att = db.Column(db.Float)
    ps_so_att = db.Column(db.Float)
    csconges = db.Column(db.Float)
    psconges = db.Column(db.Float)
    stt = db.Column(db.String(50))

class KPI4G(db.Model):
    __tablename__ = 'kpi_4g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    traffic_vol_dl = db.Column(db.Float)
    traffic_vol_ul = db.Column(db.Float)
    cell_dl_avg_thputs = db.Column(db.Float)
    cell_ul_avg_thput = db.Column(db.Float)
    user_dl_avg_thput = db.Column(db.Float)
    user_ul_avg_thput = db.Column(db.Float)
    erab_ssrate_all = db.Column(db.Float)
    service_drop_all = db.Column(db.Float)
    unvailable = db.Column(db.Float)
    res_blk_dl = db.Column(db.Float)
    cqi_4g = db.Column(db.Float)
    stt = db.Column(db.String(50))

class KPI5G(db.Model):
    __tablename__ = 'kpi_5g'
    id = db.Column(db.Integer, primary_key=True)
    ten_cell = db.Column(db.String(100), index=True)
    thoi_gian = db.Column(db.String(50))
    traffic = db.Column(db.Float)
    dl_traffic_volume_gb = db.Column(db.Float)
    ul_traffic_volume_gb = db.Column(db.Float)
    cell_downlink_average_throughput = db.Column(db.Float)
    cell_uplink_average_throughput = db.Column(db.Float)
    user_dl_avg_throughput = db.Column(db.Float)
    cqi_5g = db.Column(db.Float)
    cell_avaibility_rate = db.Column(db.Float)
    sgnb_addition_success_rate = db.Column(db.Float)
    sgnb_abnormal_release_rate = db.Column(db.Float)

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin123')
            db.session.add(u); db.session.commit()
init_database()

# ==============================================================================
# 4. TEMPLATES (DEFINED BEFORE USE)
# ==============================================================================

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --sidebar-width: 260px;
            --primary-color: #0078d4;
            --acrylic-bg: rgba(255, 255, 255, 0.85);
            --blur: blur(20px);
        }
        body { background: #f3f4f6; font-family: 'Segoe UI', sans-serif; overflow-x: hidden; }
        .sidebar { width: var(--sidebar-width); height: 100vh; position: fixed; top: 0; left: 0; background: rgba(240, 240, 245, 0.9); backdrop-filter: var(--blur); border-right: 1px solid rgba(0,0,0,0.1); z-index: 1000; transition: 0.3s; padding-top: 1rem; overflow-y: auto; }
        .sidebar-header { padding: 1.5rem; color: var(--primary-color); font-weight: 700; font-size: 1.4rem; text-align: center; }
        .sidebar-menu a { display: flex; align-items: center; padding: 12px 20px; color: #555; text-decoration: none; font-weight: 500; margin: 4px 12px; border-radius: 6px; transition: 0.2s; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background: #fff; color: var(--primary-color); box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        .sidebar-menu i { margin-right: 12px; width: 24px; text-align: center; }
        .main-content { margin-left: var(--sidebar-width); padding: 30px; transition: 0.3s; min-height: 100vh; }
        .card { border: none; border-radius: 12px; background: rgba(255,255,255,0.8); backdrop-filter: var(--blur); box-shadow: 0 4px 12px rgba(0,0,0,0.05); margin-bottom: 20px; }
        .card-header { background: rgba(255,255,255,0.5); border-bottom: 1px solid rgba(0,0,0,0.05); padding: 15px 20px; font-weight: 600; color: #333; }
        @media(max-width: 768px) { .sidebar { margin-left: calc(-1 * var(--sidebar-width)); } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; } }
        .btn-primary { background: var(--primary-color); border: none; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .table thead th { background: rgba(248,249,250,0.8); border-bottom: 2px solid #eee; text-transform: uppercase; font-size: 0.85rem; color: #666; }
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired me-2"></i>NetOps</div>
        <ul class="sidebar-menu list-unstyled">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-location-dot"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cell</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Import Data</a></li>
            {% if current_user.role == 'admin' %}
            <li class="mt-3 text-muted px-4 small fw-bold">SYSTEM</li>
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup/Restore</a></li>
            {% endif %}
            <li><a href="/logout" class="mt-3"><i class="fa-solid fa-right-from-bracket"></i> Logout</a></li>
        </ul>
    </div>
    <div class="main-content">
        <button class="btn btn-light d-md-none mb-3 shadow-sm" onclick="document.getElementById('sidebar').classList.toggle('active')"><i class="fa-solid fa-bars"></i></button>
        <div class="container-fluid p-0">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}{% for category, message in messages %}<div class="alert alert-{{ category }} shadow-sm border-0">{{ message }}</div>{% endfor %}{% endif %}
            {% endwith %}
            {% block content %}{% endblock %}
        </div>
    </div>
    
    <div class="modal fade" id="chartModal" tabindex="-1"><div class="modal-dialog modal-xl modal-dialog-centered"><div class="modal-content"><div class="modal-header border-0"><h5 class="modal-title fw-bold text-primary" id="modalTitle"></h5><button class="btn-close" data-bs-dismiss="modal"></button></div><div class="modal-body"><div style="height:60vh"><canvas id="modalChart"></canvas></div></div></div></div></div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        let mChart = null;
        function showDetailModal(title, labels, datasets) {
            document.getElementById('modalTitle').innerText = title;
            const ctx = document.getElementById('modalChart').getContext('2d');
            if(mChart) mChart.destroy();
            mChart = new Chart(ctx, { type: 'line', data: { labels: labels, datasets: datasets }, options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false } } });
            new bootstrap.Modal(document.getElementById('chartModal')).show();
        }
    </script>
</body>
</html>
"""

LOGIN_PAGE = """<!DOCTYPE html><html><head><title>Login</title><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet"><style>body{background:#f0f2f5;display:flex;align-items:center;justify-content:center;height:100vh}.card{width:350px;border:none;box-shadow:0 10px 30px rgba(0,0,0,0.1);border-radius:15px;padding:40px}</style></head><body><div class="card"><h3 class="text-center mb-4 text-primary">Login</h3><form method="POST"><input class="form-control mb-3" name="username" placeholder="Username" required><input class="form-control mb-3" type="password" name="password" placeholder="Password" required><button class="btn btn-primary w-100">Sign In</button></form></div></body></html>"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header d-flex justify-content-between align-items-center">
        <span class="fs-5">{{ title }}</span>
        <span class="badge bg-primary rounded-pill">{{ current_user.role }}</span>
    </div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <div class="row g-4 text-center">
                <div class="col-md-3"><div class="p-4 bg-white rounded shadow-sm border"><h2 class="text-primary mb-0">98%</h2><small class="text-muted fw-bold">KPI Score</small></div></div>
                <div class="col-md-3"><div class="p-4 bg-white rounded shadow-sm border"><h2 class="text-danger mb-0">12</h2><small class="text-muted fw-bold">Worst Cells</small></div></div>
                <div class="col-md-3"><div class="p-4 bg-white rounded shadow-sm border"><h2 class="text-warning mb-0">5</h2><small class="text-muted fw-bold">Congestion</small></div></div>
                <div class="col-md-3"><div class="p-4 bg-white rounded shadow-sm border"><h2 class="text-success mb-0">OK</h2><small class="text-muted fw-bold">System Status</small></div></div>
            </div>
            <div class="row mt-4">
                <div class="col-md-6"><div class="p-3 border rounded bg-light"><h6>RF Database</h6><p>3G: {{ count_rf3g }} | 4G: {{ count_rf4g }} | 5G: {{ count_rf5g }}</p></div></div>
                <div class="col-md-6"><div class="p-3 border rounded bg-light"><h6>KPI Records</h6><p>3G: {{ count_kpi3g }} | 4G: {{ count_kpi4g }} | 5G: {{ count_kpi5g }}</p></div></div>
            </div>

        {% elif active_page == 'kpi' %}
            <form class="row g-3 mb-4 bg-light p-3 rounded border">
                <div class="col-md-2"><label class="small fw-bold text-muted">Tech</label><select name="tech" class="form-select"><option value="3g" {% if selected_tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech=='5g' %}selected{% endif %}>5G</option></select></div>
                <div class="col-md-4"><label class="small fw-bold text-muted">POI</label><input name="poi_name" list="pois" class="form-control" placeholder="Select POI..." value="{{ selected_poi }}"><datalist id="pois">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                <div class="col-md-4"><label class="small fw-bold text-muted">Cell</label><input name="cell_name" class="form-control" placeholder="Enter Cell/Site..." value="{{ cell_name_input }}"></div>
                <div class="col-md-2 align-self-end"><button class="btn btn-primary w-100">Visualize</button></div>
            </form>
            {% if charts %}
                {% for id, c in charts.items() %}
                <div class="card mb-3"><div class="card-body"><canvas id="{{ id }}" style="height:350px"></canvas></div></div>
                <script>(function(){
                    const ctx = document.getElementById('{{ id }}').getContext('2d');
                    const data = {{ c | tojson }};
                    new Chart(ctx, {type:'line',data:data, options:{responsive:true,maintainAspectRatio:false, interaction:{mode:'nearest',intersect:false}, onClick:(e,el)=>{if(el.length) showDetailModal('{{ c.title }}', data.labels[el[0].index], data.datasets, data.labels)}}});
                })();</script>
                {% endfor %}
            {% endif %}

        {% elif active_page == 'worst_cell' %}
            <form class="row g-3 mb-4 bg-light p-3 rounded border">
                <div class="col-auto"><label class="col-form-label fw-bold">Thời gian:</label></div>
                <div class="col-auto"><select name="duration" class="form-select"><option value="1" {% if duration==1 %}selected{% endif %}>1 ngày</option><option value="3" {% if duration==3 %}selected{% endif %}>3 ngày</option><option value="7" {% if duration==7 %}selected{% endif %}>7 ngày</option><option value="15" {% if duration==15 %}selected{% endif %}>15 ngày</option><option value="30" {% if duration==30 %}selected{% endif %}>30 ngày</option></select></div>
                <div class="col-auto"><button class="btn btn-danger">Lọc Worst Cell</button></div>
            </form>
            {% if dates %}<div class="alert alert-info py-2 small">Xét duyệt: {% for d in dates %}{{ d }} {% endfor %}</div>{% endif %}
            <div class="table-responsive"><table class="table table-hover table-sm"><thead><tr><th>Cell</th><th>Thput</th><th>PRB</th><th>CQI</th><th>Drop</th><th>Action</th></tr></thead><tbody>
            {% for r in worst_cells %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="{{ 'text-danger fw-bold' if r.avg_thput < 7000 }}">{{ r.avg_thput|round(2) }}</td><td class="{{ 'text-danger fw-bold' if r.avg_res_blk > 20 }}">{{ r.avg_res_blk|round(2) }}</td><td class="{{ 'text-danger fw-bold' if r.avg_cqi < 93 }}">{{ r.avg_cqi|round(2) }}</td><td class="{{ 'text-danger fw-bold' if r.avg_drop > 0.3 }}">{{ r.avg_drop|round(2) }}</td><td><a href="/kpi?tech=4g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% else %}<tr><td colspan="6" class="text-center text-muted">Không có dữ liệu</td></tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'conges_3g' %}
            <form class="mb-4"><button name="action" value="execute" class="btn btn-primary"><i class="fa-solid fa-play me-2"></i>Thực hiện Lọc Nghẽn</button></form>
            {% if dates %}<div class="alert alert-info py-2 small">Xét duyệt 3 ngày: {% for d in dates %}{{ d }} {% endfor %}</div>{% endif %}
            <table class="table table-bordered table-sm"><thead><tr><th>Cell</th><th>CS Traf</th><th>CS Cong</th><th>PS Traf</th><th>PS Cong</th><th>Action</th></tr></thead><tbody>
            {% for r in conges_data %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td>{{ r.avg_cs_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_cs_conges > 2 }}">{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_ps_conges > 2 }}">{{ r.avg_ps_conges }}</td><td><a href="/kpi?tech=3g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% else %}<tr><td colspan="6" class="text-center text-muted">Nhấn nút Thực hiện để xem kết quả</td></tr>{% endfor %}</tbody></table>

        {% elif active_page == 'traffic_down' %}
            <form class="row g-3 mb-4 bg-light p-3 rounded border">
                <div class="col-auto"><select name="tech" class="form-select"><option value="3g" {% if tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if tech=='5g' %}selected{% endif %}>5G</option></select></div>
                <div class="col-auto"><button name="action" value="execute" class="btn btn-primary"><i class="fa-solid fa-play me-2"></i>Thực hiện</button></div>
                <div class="col-auto ms-auto"><span class="badge bg-info text-dark">Ngày: {{ analysis_date }}</span></div>
            </form>
            <div class="row">
                <div class="col-md-6"><div class="card border-danger h-100"><div class="card-header bg-danger text-white">Không Lưu Lượng (Today < 0.1, Avg7 > 2)</div><div class="table-responsive"><table class="table table-sm mb-0"><thead><tr><th>Cell</th><th>Today</th><th>Avg7</th><th>View</th></tr></thead><tbody>{% for r in zero_traffic %}<tr><td>{{ r.cell_name }}</td><td>{{ r.traffic_today }}</td><td>{{ r.avg_last_7 }}</td><td><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-sm btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                <div class="col-md-6"><div class="card border-warning h-100"><div class="card-header bg-warning text-dark">Suy Giảm (> 30% vs Last Week)</div><div class="table-responsive"><table class="table table-sm mb-0"><thead><tr><th>Cell</th><th>Today</th><th>LastWk</th><th>%</th><th>View</th></tr></thead><tbody>{% for r in degraded %}<tr><td>{{ r.cell_name }}</td><td>{{ r.traffic_today }}</td><td>{{ r.traffic_last_week }}</td><td class="text-danger fw-bold">-{{ r.degrade_percent }}%</td><td><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-sm btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
            </div>

        {% elif active_page == 'import' %}
            <div class="row">
                <div class="col-md-8">
                     <ul class="nav nav-pills mb-3"><li class="nav-item"><a class="nav-link active" data-bs-toggle="tab" href="#rf3">RF 3G</a></li><li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#rf4">RF 4G</a></li><li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#rf5">RF 5G</a></li><li class="nav-item"><a class="nav-link text-success" data-bs-toggle="tab" href="#k3">KPI 3G</a></li><li class="nav-item"><a class="nav-link text-success" data-bs-toggle="tab" href="#k4">KPI 4G</a></li><li class="nav-item"><a class="nav-link text-success" data-bs-toggle="tab" href="#k5">KPI 5G</a></li></ul>
                     <div class="tab-content p-4 border rounded bg-white">
                        <div class="tab-pane active" id="rf3"><form action="/import?type=3g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-primary">Upload RF 3G</button></form></div>
                        <div class="tab-pane" id="rf4"><form action="/import?type=4g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-primary">Upload RF 4G</button></form></div>
                        <div class="tab-pane" id="rf5"><form action="/import?type=5g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-primary">Upload RF 5G</button></form></div>
                        <div class="tab-pane" id="k3"><form action="/import?type=kpi3g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required multiple><button class="btn btn-success">Upload KPI 3G</button></form></div>
                        <div class="tab-pane" id="k4"><form action="/import?type=kpi4g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required multiple><button class="btn btn-success">Upload KPI 4G</button></form></div>
                        <div class="tab-pane" id="k5"><form action="/import?type=kpi5g" method="POST" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required multiple><button class="btn btn-success">Upload KPI 5G</button></form></div>
                     </div>
                </div>
                <div class="col-md-4"><div class="card"><div class="card-header">Data History</div><ul class="list-group list-group-flush text-center small"><li class="list-group-item bg-light fw-bold"><div class="row"><div class="col">3G</div><div class="col">4G</div><div class="col">5G</div></div></li>{% for d3,d4,d5 in kpi_rows %}<li class="list-group-item"><div class="row"><div class="col">{{ d3 or '-' }}</div><div class="col">{{ d4 or '-' }}</div><div class="col">{{ d5 or '-' }}</div></div></li>{% endfor %}</ul></div></div>
            </div>

        {% elif active_page == 'rf' %}
            <div class="d-flex justify-content-between mb-3"><div class="btn-group"><a href="/rf?tech=3g" class="btn btn-outline-primary {{ 'active' if current_tech=='3g' }}">3G</a><a href="/rf?tech=4g" class="btn btn-outline-primary {{ 'active' if current_tech=='4g' }}">4G</a><a href="/rf?tech=5g" class="btn btn-outline-primary {{ 'active' if current_tech=='5g' }}">5G</a></div><div><a href="/rf/add?tech={{ current_tech }}" class="btn btn-primary me-2">New</a><a href="/rf?tech={{ current_tech }}&action=export" class="btn btn-success">Export</a></div></div>
            <div class="table-responsive" style="max-height:70vh"><table class="table table-hover table-sm"><thead><tr><th>Action</th>{% for c in rf_columns %}<th>{{ c }}</th>{% endfor %}</tr></thead><tbody>{% for r in rf_data %}<tr><td><a href="/rf/edit/{{ current_tech }}/{{ r.id }}" class="text-warning me-2"><i class="fa-solid fa-pen"></i></a><a href="/rf/delete/{{ current_tech }}/{{ r.id }}" class="text-danger" onclick="return confirm('Xoa?')"><i class="fa-solid fa-trash"></i></a></td>{% for c in rf_columns %}<td>{{ r[c] }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div>
        
        {% else %}
            <div class="text-center py-5 text-muted">Coming Soon</div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""
USER_MANAGEMENT_TEMPLATE = """{% extends "base" %}{% block content %}<div class="row"><div class="col-md-4"><div class="card"><div class="card-header">Add User</div><div class="card-body"><form method="POST" action="/users/add"><input name="username" class="form-control mb-2" placeholder="User" required><input name="password" type="password" class="form-control mb-2" placeholder="Pass" required><select name="role" class="form-select mb-3"><option value="user">User</option><option value="admin">Admin</option></select><button class="btn btn-success w-100">Create</button></form></div></div></div><div class="col-md-8"><div class="card"><div class="card-header">Users</div><table class="table"><thead><tr><th>ID</th><th>User</th><th>Role</th><th>Action</th></tr></thead><tbody>{% for u in users %}<tr><td>{{ u.id }}</td><td>{{ u.username }}</td><td>{{ u.role }}</td><td>{% if u.username!='admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-danger">Del</a>{% endif %}</td></tr>{% endfor %}</tbody></table></div></div></div>{% endblock %}"""
PROFILE_TEMPLATE = """{% extends "base" %}{% block content %}<div class="row justify-content-center"><div class="col-md-6"><div class="card"><div class="card-header">Change Password</div><div class="card-body"><form method="POST" action="/change-password"><input type="password" name="current_password" class="form-control mb-3" placeholder="Current Pass" required><input type="password" name="new_password" class="form-control mb-3" placeholder="New Pass" required><button class="btn btn-primary w-100">Save</button></form></div></div></div></div>{% endblock %}"""
BACKUP_RESTORE_TEMPLATE = """{% extends "base" %}{% block content %}<div class="row"><div class="col-md-6"><div class="card"><div class="card-header">Backup</div><div class="card-body text-center"><form method="POST" action="/backup"><button class="btn btn-primary btn-lg">Download Backup</button></form></div></div></div><div class="col-md-6"><div class="card"><div class="card-header">Restore</div><div class="card-body"><form method="POST" action="/restore" enctype="multipart/form-data"><input type="file" name="file" class="form-control mb-3" required><button class="btn btn-warning w-100">Restore</button></form></div></div></div></div>{% endblock %}"""
RF_FORM_TEMPLATE = """{% extends "base" %}{% block content %}<div class="card"><div class="card-header">{{ title }}</div><div class="card-body"><form method="POST"><div class="row">{% for col in columns %}<div class="col-md-4 mb-3"><label class="small fw-bold text-muted">{{ col }}</label><input type="text" name="{{ col }}" class="form-control" value="{{ obj[col] if obj else '' }}"></div>{% endfor %}</div><button class="btn btn-primary">Save</button></form></div></div>{% endblock %}"""
RF_DETAIL_TEMPLATE = """{% extends "base" %}{% block content %}<div class="card"><div class="card-header">Detail</div><div class="card-body"><table class="table table-bordered">{% for k,v in obj.items() %}<tr><th>{{ k }}</th><td>{{ v }}</td></tr>{% endfor %}</table></div></div>{% endblock %}"""

app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})
def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE: return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

# ==============================================================================
# 5. ROUTES
# ==============================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']):
            login_user(user)
            return redirect(url_for('index'))
        flash('Login failed', 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    try:
        cnt = {
            'rf3g': db.session.query(func.count(RF3G.id)).scalar(),
            'rf4g': db.session.query(func.count(RF4G.id)).scalar(),
            'rf5g': db.session.query(func.count(RF5G.id)).scalar(),
            'kpi3g': db.session.query(func.count(KPI3G.id)).scalar(),
            'kpi4g': db.session.query(func.count(KPI4G.id)).scalar(),
            'kpi5g': db.session.query(func.count(KPI5G.id)).scalar(),
        }
    except: cnt = defaultdict(int)
    return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard', **cnt)

@app.route('/kpi')
@login_required
def kpi():
    tech = request.args.get('tech', '3g')
    cell = request.args.get('cell_name', '').strip()
    poi_in = request.args.get('poi_name', '').strip()
    charts = {}
    
    target_cells = []
    if poi_in:
        p4 = db.session.query(POI4G.cell_code).filter_by(poi_name=poi_in).all()
        p5 = db.session.query(POI5G.cell_code).filter_by(poi_name=poi_in).all()
        target_cells = [c[0] for c in p4 + p5]
    elif cell:
        target_cells = [c.strip() for c in re.split(r'[,\s]+', cell) if c.strip()]
    
    if target_cells:
        Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
        if Model:
            data = Model.query.filter(Model.ten_cell.in_(target_cells)).all()
            try: data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
            except: pass
            
            if data:
                dates = sorted(list(set(d.thoi_gian for d in data)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                grouped = defaultdict(list)
                for d in data: grouped[d.ten_cell].append(d)
                
                cfgs = {
                    '3g': [('traffic', 'Traffic'), ('cssr', 'CSSR')],
                    '4g': [('traffic', 'Traffic'), ('user_dl_avg_thput', 'Thput')],
                    '5g': [('traffic', 'Traffic'), ('cqi_5g', 'CQI')]
                }.get(tech, [])
                
                colors = generate_colors(20)
                for key, label in cfgs:
                    ds = []
                    for i, (c_name, rows) in enumerate(grouped.items()):
                        row_map = {r.thoi_gian: getattr(r, key) for r in rows}
                        ds.append({'label': c_name, 'data': [row_map.get(d) for d in dates], 'borderColor': colors[i%20], 'fill': False})
                    charts[key] = {'title': label, 'labels': dates, 'datasets': ds}
    
    # POI List
    poi_list = []
    try: poi_list = sorted(list(set([r[0] for r in db.session.query(POI4G.poi_name).distinct()] + [r[0] for r in db.session.query(POI5G.poi_name).distinct()])))
    except: pass
    
    return render_page(CONTENT_TEMPLATE, title="KPI", active_page='kpi', selected_tech=tech, cell_name_input=cell, selected_poi=poi_in, poi_list=poi_list, charts=charts)

@app.route('/worst-cell')
@login_required
def worst_cell():
    duration = int(request.args.get('duration', 1))
    # 1. Dates
    all_dates = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().all()]
    date_objs = sorted([datetime.strptime(d, '%d/%m/%Y') for d in all_dates if d], reverse=True)
    target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:duration]]
    
    if not target_dates: return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=[], dates=[])
    
    # 2. Query
    records = KPI4G.query.filter(
        KPI4G.thoi_gian.in_(target_dates),
        ~KPI4G.ten_cell.startswith('MBF_TH'), ~KPI4G.ten_cell.startswith('VNP-4G'),
        ((KPI4G.user_dl_avg_thput < 7000) | (KPI4G.res_blk_dl > 20) | (KPI4G.cqi_4g < 93) | (KPI4G.service_drop_all > 0.3))
    ).all()
    
    # 3. Process
    groups = defaultdict(list)
    for r in records: groups[r.ten_cell].append(r)
    
    results = []
    for cell, rows in groups.items():
        if len(rows) == duration:
            results.append({
                'cell_name': cell,
                'avg_thput': sum(r.user_dl_avg_thput or 0 for r in rows)/duration,
                'avg_res_blk': sum(r.res_blk_dl or 0 for r in rows)/duration,
                'avg_cqi': sum(r.cqi_4g or 0 for r in rows)/duration,
                'avg_drop': sum(r.service_drop_all or 0 for r in rows)/duration
            })
    return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=results, dates=target_dates, duration=duration)

@app.route('/conges-3g')
@login_required
def conges_3g():
    conges_data, target_dates = [], []
    if request.args.get('action') == 'execute':
        all_dates = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().all()]
        date_objs = sorted([datetime.strptime(d, '%d/%m/%Y') for d in all_dates if d], reverse=True)
        if len(date_objs) >= 3:
            target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:3]]
            records = KPI3G.query.filter(
                KPI3G.thoi_gian.in_(target_dates),
                ((KPI3G.csconges > 2) & (KPI3G.cs_so_att > 100)) | ((KPI3G.psconges > 2) & (KPI3G.ps_so_att > 500))
            ).all()
            groups = defaultdict(list)
            for r in records: groups[r.ten_cell].append(r)
            for cell, rows in groups.items():
                if len(rows) == 3:
                    conges_data.append({
                        'cell_name': cell,
                        'avg_cs_traffic': sum(r.traffic or 0 for r in rows)/3,
                        'avg_cs_conges': sum(r.csconges or 0 for r in rows)/3,
                        'avg_ps_traffic': sum(r.pstraffic or 0 for r in rows)/3,
                        'avg_ps_conges': sum(r.psconges or 0 for r in rows)/3
                    })
    return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=conges_data, dates=target_dates)

@app.route('/traffic-down')
@login_required
def traffic_down():
    tech = request.args.get('tech', '4g')
    action = request.args.get('action')
    zero_traffic, degraded, analysis_date = [], [], "N/A"
    
    if action == 'execute':
        Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
        if Model:
            dates_raw = [d[0] for d in db.session.query(Model.thoi_gian).distinct().all()]
            dates_obj = sorted([datetime.strptime(d, '%d/%m/%Y') for d in dates_raw if d], reverse=True)
            if dates_obj:
                latest = dates_obj[0]
                analysis_date = latest.strftime('%d/%m/%Y')
                # Need data for T0 and T-7, plus T-1..T-7 for avg
                needed = [latest] + [latest - timedelta(days=i) for i in range(1, 8)]
                needed_str = [d.strftime('%d/%m/%Y') for d in needed]
                
                records = Model.query.filter(Model.thoi_gian.in_(needed_str)).all()
                data_map = defaultdict(dict)
                for r in records:
                    if r.ten_cell.startswith('MBF_TH') or r.ten_cell.startswith('VNP-4G'): continue
                    try: data_map[r.ten_cell][datetime.strptime(r.thoi_gian, '%d/%m/%Y')] = r.traffic or 0
                    except: pass
                
                last_week = latest - timedelta(days=7)
                for cell, d_map in data_map.items():
                    t0 = d_map.get(latest, 0)
                    t_last = d_map.get(last_week, 0)
                    # Zero
                    if t0 < 0.1:
                        avg7 = sum(d_map.get(latest - timedelta(days=i), 0) for i in range(1,8)) / 7
                        if avg7 > 2: zero_traffic.append({'cell_name': cell, 'traffic_today': round(t0,3), 'avg_last_7': round(avg7,3)})
                    # Degraded
                    if t_last > 1 and t0 < 0.7 * t_last:
                        degraded.append({'cell_name': cell, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})

    return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', zero_traffic=zero_traffic, degraded=degraded, tech=tech, analysis_date=analysis_date)

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '3g')
    action = request.args.get('action')
    search = request.args.get('cell_search', '').strip()
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    
    if action == 'export':
         def generate():
            yield '\ufeff'.encode('utf-8')
            cols = [c.key for c in Model.__table__.columns]
            yield (','.join(cols) + '\n').encode('utf-8')
            q = db.select(Model)
            if search: q = q.filter(Model.cell_code.like(f"%{search}%"))
            for row in db.session.execute(q).scalars():
                yield (','.join([str(getattr(row, c, '') or '').replace(',', ';') for c in cols]) + '\n').encode('utf-8')
         return Response(stream_with_context(generate()), mimetype='text/csv', headers={"Content-Disposition": f"attachment; filename=RF_{tech}.csv"})

    query = Model.query
    if search: query = query.filter(Model.cell_code.like(f"%{search}%"))
    rows = query.limit(500).all()
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    data = [{c: getattr(r, c) for c in cols} | {'id': r.id} for r in rows]
    return render_page(CONTENT_TEMPLATE, title="RF", active_page='rf', rf_data=data, rf_columns=cols, current_tech=tech)

@app.route('/rf/add', methods=['GET', 'POST'])
@login_required
def rf_add():
    tech = request.args.get('tech', '3g')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if request.method == 'POST':
        data = {k: v for k, v in request.form.items() if k in Model.__table__.columns.keys()}
        db.session.add(Model(**data)); db.session.commit(); flash('Added', 'success')
        return redirect(url_for('rf', tech=tech))
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Add RF {tech}", columns=cols, tech=tech, obj={})

@app.route('/rf/edit/<tech>/<int:id>', methods=['GET', 'POST'])
@login_required
def rf_edit(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    if request.method == 'POST':
        for k,v in request.form.items(): setattr(obj, k, v)
        db.session.commit(); flash('Updated', 'success'); return redirect(url_for('rf', tech=tech))
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Edit RF {tech}", columns=cols, tech=tech, obj=obj.__dict__)

@app.route('/rf/delete/<tech>/<int:id>')
@login_required
def rf_delete(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db.session.delete(db.session.get(Model, id)); db.session.commit(); flash('Deleted', 'success')
    return redirect(url_for('rf', tech=tech))

@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.args.get('type')
        cfg = {'3g': RF3G, '4g': RF4G, '5g': RF5G, 'kpi3g': KPI3G, 'kpi4g': KPI4G, 'kpi5g': KPI5G, 'poi4g': POI4G, 'poi5g': POI5G}
        Model = cfg.get(itype)
        
        if Model:
            valid_cols = [c.key for c in Model.__table__.columns if c.key != 'id']
            for file in files:
                try:
                    if file.filename.endswith('.csv'): chunks = pd.read_csv(file, chunksize=2000, encoding='utf-8-sig', on_bad_lines='skip')
                    else: chunks = [pd.read_excel(file)]
                    
                    for df in chunks:
                        df.columns = [clean_header(c) for c in df.columns]
                        records = []
                        for row in df.to_dict('records'):
                            clean_row = {k: v for k, v in row.items() if k in valid_cols and not pd.isna(v)}
                            # Specific fix for KPI4G traffic column mismatch
                            if itype == 'kpi4g' and 'traffic' not in clean_row and 'traffic_vol_dl' in clean_row:
                                clean_row['traffic'] = clean_row['traffic_vol_dl']
                            records.append(clean_row)
                        if records: db.session.bulk_insert_mappings(Model, records); db.session.commit()
                    flash(f'Imported {file.filename}', 'success')
                except Exception as e: flash(f'Error {file.filename}: {e}', 'danger')
        return redirect(url_for('import_data'))

    # History
    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    return render_page(CONTENT_TEMPLATE, title="Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)))

# Placeholder routes for script/users/profile
@app.route('/script')
@login_required
def script(): return render_page(CONTENT_TEMPLATE, title="Script", active_page='script')
@app.route('/users')
@login_required
def manage_users(): return render_page(USER_MANAGEMENT_TEMPLATE, users=User.query.all(), active_page='users')
@app.route('/users/add', methods=['POST'])
@login_required
def add_user(): 
    u = User(username=request.form['username'], role=request.form['role']); u.set_password(request.form['password'])
    db.session.add(u); db.session.commit(); return redirect(url_for('manage_users'))
@app.route('/users/delete/<int:id>')
@login_required
def delete_user(id): db.session.delete(db.session.get(User, id)); db.session.commit(); return redirect(url_for('manage_users'))
@app.route('/users/reset-pass/<int:id>')
@login_required
def reset_pass(id): u=db.session.get(User, id); u.set_password(request.args.get('new_pass')); db.session.commit(); return redirect(url_for('manage_users'))
@app.route('/profile')
@login_required
def profile(): return render_page(PROFILE_TEMPLATE, active_page='profile')
@app.route('/change-password', methods=['POST'])
@login_required
def change_password(): 
    if current_user.check_password(request.form['current_password']): current_user.set_password(request.form['new_password']); db.session.commit(); flash('Done', 'success')
    return redirect(url_for('profile'))

if __name__ == '__main__':
    app.run(debug=True)
