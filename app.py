import os, json, gc, re, zipfile, random, math, requests, urllib.parse, jinja2, pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, or_
from itertools import zip_longest
from collections import defaultdict

# ==============================================================================
# 1. CẤU HÌNH APP & DATABASE
# ==============================================================================
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 280, 'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '8684244776:AAEjz9Lv8Zc5u-o6BJoHM3eCGXDBQE6hRUU')

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# ==============================================================================
# 2. CÁC HÀM TIỆN ÍCH (Lọc rác, ép kiểu, xử lý Header 3G độc quyền)
# ==============================================================================
def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    return ''.join(s0[s1.index(c)] if c in s1 else c for c in input_str)

def clean_header(col_name, itype=None, raw_headers=None):
    c = str(col_name).strip().lower()
    
    # BỘ MAPPING CHUẨN ĐỘC QUYỀN CHO CONFIG_3G VÀ CELL_3G
    if itype == 'config3g':
        mapping = {'mã csht':'csht_code', 'cell name (alias)':'cell_name', 'mã cell':'cell_code', 'mã trạm':'site_code', 'latitude':'latitude', 'longitude':'longitude', 'longtitude':'longitude', 'thiết bị':'equipment', 'tên thiết bị':'equipment', 'băng tần':'frequency', 'dlpsc':'psc', 'dl_psc':'psc', 'dl_uarfcn':'dl_uarfcn', 'lac':'bsc_lac', 'bsc_lac':'bsc_lac', 'ci':'ci', 'antennahigh':'anten_height', 'antenna high':'anten_height', 'azimuth':'azimuth', 'mechanicaltilt':'m_t', 'mechanical tilt':'m_t', 'electricaltilt':'e_t', 'electrical tilt':'e_t', 'totaltilt':'total_tilt', 'total tilt':'total_tilt', 'antennatype':'antena', 'model ăn ten':'antena'}
        return mapping.get(c, f"ignore_cfg_{re.sub(r'[^a-z0-9]', '_', remove_accents(c))}")
        
    if itype == 'cell3g':
        mapping = {'tên trên hệ thống':'cell_code', 'mã cell':'cell_code', 'antenna tên hãng sx':'hang_sx', 'hãng sx':'hang_sx', 'antenna dùng chung':'swap', 'swap':'swap', 'ngày hoạt động':'start_day', 'hoàn cảnh ra đời':'ghi_chu', 'ghi chú':'ghi_chu'}
        return mapping.get(c, f"ignore_cell_{re.sub(r'[^a-z0-9]', '_', remove_accents(c))}")

    # Fallback cho 4G/5G/KPI
    mapping_all = {'mã node cha':'site_code', 'mã node':'cell_code', 'tên trên hệ thống':'cell_name', 'mã csht của trạm':'csht_code', 'mã csht':'csht_code', 'longtitude':'longitude', 'latitude':'latitude', 'model ăn ten':'antena', 'antenna model':'antena', 'antennatype':'antena', 'total tilt':'total_tilt', 'totaltilt':'total_tilt', 'mechanical tilt':'m_t', 'mechanicaltilt':'m_t', 'electrical tilt':'e_t', 'electricaltilt':'e_t', 'thiết bị':'equipment', 'tên thiết bị':'equipment', 'băng tần':'frequency', 'enodeb id':'enodeb_id', 'gnodeb id':'gnodeb_id', 'nrci':'lcrid', 'lcrid':'lcrid', 'antenna high':'anten_height', 'hãng sx':'hang_sx', 'tên hãng sx':'hang_sx', 'ngày hoạt động':'start_day', 'ghi chú':'ghi_chu', 'hoàn cảnh ra đời':'ghi_chu', 'lac':'bsc_lac', 'bsc_lac':'bsc_lac', 'dl_psc':'psc', 'psc':'psc', 'tên cell':'ten_cell', 'cell name':'ten_cell', 'thời gian':'thoi_gian', 'total data traffic volume':'traffic', 'user downlink average throughput':'user_dl_avg_thput', 'resource block untilizing rate downlink':'res_blk_dl', 'service drop (all service)':'service_drop_all', 'mimo':'mimo', 'nrarfcndl':'nrarfcn', 'pci':'pci', 'tac':'tac', 'ci':'ci', 'đồng bộ':'dong_bo', 'site name':'site_name', 'antenna dùng chung':'swap', 'dl_uarfcn':'dl_uarfcn'}
    for key, val in mapping_all.items():
        if key in c: return val
    return re.sub(r'_+', '_', re.sub(r'[^a-z0-9]', '_', remove_accents(c))).strip('_')

def generate_colors(n):
    base = ['#0078d4', '#107c10', '#d13438', '#ffaa44', '#00bcf2', '#5c2d91', '#e3008c', '#b4009e']
    if n <= len(base): return base[:n]
    return base + ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)]) for i in range(n - len(base))]

RF_COLS_ORDER = {
    '3g': ['csht_code', 'cell_name', 'cell_code', 'site_code', 'latitude', 'longitude', 'equipment', 'frequency', 'psc', 'dl_uarfcn', 'bsc_lac', 'ci', 'anten_height', 'azimuth', 'm_t', 'e_t', 'total_tilt', 'hang_sx', 'antena', 'swap', 'start_day', 'ghi_chu'],
    '4g': ['csht_code', 'cell_name', 'cell_code', 'site_code', 'latitude', 'longitude', 'equipment', 'frequency', 'dl_uarfcn', 'pci', 'tac', 'enodeb_id', 'lcrid', 'anten_height', 'azimuth', 'm_t', 'e_t', 'total_tilt', 'mimo', 'hang_sx', 'antena', 'swap', 'start_day', 'ghi_chu'],
    '5g': ['csht_code', 'site_name', 'cell_code', 'site_code', 'latitude', 'longitude', 'equipment', 'frequency', 'nrarfcn', 'pci', 'tac', 'gnodeb_id', 'lcrid', 'anten_height', 'azimuth', 'm_t', 'e_t', 'total_tilt', 'mimo', 'hang_sx', 'antena', 'dong_bo', 'start_day', 'ghi_chu']
}

# ==============================================================================
# 3. MODELS DATABASE
# ==============================================================================
class User(UserMixin, db.Model):
    __tablename__='user'; id=db.Column(db.Integer, primary_key=True); username=db.Column(db.String(50), unique=True, nullable=False); password_hash=db.Column(db.String(255), nullable=False); role=db.Column(db.String(20), default='user')
    def set_password(self, p): self.password_hash = generate_password_hash(p)
    def check_password(self, p): return check_password_hash(self.password_hash, p)

class Config3G(db.Model): __tablename__='config_3g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(255), index=True); site_code=db.Column(db.String(255)); cell_name=db.Column(db.String(255)); csht_code=db.Column(db.String(255)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); antena=db.Column(db.String(255)); azimuth=db.Column(db.Integer); total_tilt=db.Column(db.Float); equipment=db.Column(db.String(255)); frequency=db.Column(db.String(255)); psc=db.Column(db.String(255)); dl_uarfcn=db.Column(db.String(255)); bsc_lac=db.Column(db.String(255)); ci=db.Column(db.String(255)); anten_height=db.Column(db.Float); m_t=db.Column(db.Float); e_t=db.Column(db.Float)
class Cell3G(db.Model): __tablename__='cell_3g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(255), index=True); hang_sx=db.Column(db.String(255)); swap=db.Column(db.String(255)); start_day=db.Column(db.String(255)); ghi_chu=db.Column(db.Text)
class RF3G(db.Model): __tablename__='rf_3g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(255), index=True); site_code=db.Column(db.String(255)); cell_name=db.Column(db.String(255)); csht_code=db.Column(db.String(255)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); antena=db.Column(db.String(255)); azimuth=db.Column(db.Integer); total_tilt=db.Column(db.Float); equipment=db.Column(db.String(255)); frequency=db.Column(db.String(255)); psc=db.Column(db.String(255)); dl_uarfcn=db.Column(db.String(255)); bsc_lac=db.Column(db.String(255)); ci=db.Column(db.String(255)); anten_height=db.Column(db.Float); m_t=db.Column(db.Float); e_t=db.Column(db.Float); hang_sx=db.Column(db.String(255)); swap=db.Column(db.String(255)); start_day=db.Column(db.String(255)); ghi_chu=db.Column(db.Text)
class RF4G(db.Model): __tablename__='rf_4g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(255), index=True); site_code=db.Column(db.String(255)); cell_name=db.Column(db.String(255)); csht_code=db.Column(db.String(255)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); antena=db.Column(db.String(255)); azimuth=db.Column(db.Integer); total_tilt=db.Column(db.Float); equipment=db.Column(db.String(255)); frequency=db.Column(db.String(255)); dl_uarfcn=db.Column(db.String(255)); pci=db.Column(db.String(255)); tac=db.Column(db.String(255)); enodeb_id=db.Column(db.String(255)); lcrid=db.Column(db.String(255)); anten_height=db.Column(db.Float); m_t=db.Column(db.Float); e_t=db.Column(db.Float); mimo=db.Column(db.String(255)); hang_sx=db.Column(db.String(255)); swap=db.Column(db.String(255)); start_day=db.Column(db.String(255)); ghi_chu=db.Column(db.Text)
class RF5G(db.Model): __tablename__='rf_5g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(255), index=True); site_code=db.Column(db.String(255)); site_name=db.Column(db.String(255)); csht_code=db.Column(db.String(255)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); antena=db.Column(db.String(255)); azimuth=db.Column(db.Integer); total_tilt=db.Column(db.Float); equipment=db.Column(db.String(255)); frequency=db.Column(db.String(255)); nrarfcn=db.Column(db.String(255)); pci=db.Column(db.String(255)); tac=db.Column(db.String(255)); gnodeb_id=db.Column(db.String(255)); lcrid=db.Column(db.String(255)); anten_height=db.Column(db.Float); m_t=db.Column(db.Float); e_t=db.Column(db.Float); mimo=db.Column(db.String(255)); hang_sx=db.Column(db.String(255)); dong_bo=db.Column(db.String(255)); start_day=db.Column(db.String(255)); ghi_chu=db.Column(db.Text)
class POI4G(db.Model): __tablename__='poi_4g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(50)); site_code=db.Column(db.String(50)); poi_name=db.Column(db.String(200), index=True)
class POI5G(db.Model): __tablename__='poi_5g'; id=db.Column(db.Integer, primary_key=True); cell_code=db.Column(db.String(50)); site_code=db.Column(db.String(50)); poi_name=db.Column(db.String(200), index=True)
class KPI3G(db.Model): __tablename__='kpi_3g'; id=db.Column(db.Integer, primary_key=True); ten_cell=db.Column(db.String(100), index=True); thoi_gian=db.Column(db.String(50)); traffic=db.Column(db.Float); pstraffic=db.Column(db.Float); cssr=db.Column(db.Float); dcr=db.Column(db.Float); ps_cssr=db.Column(db.Float); ps_dcr=db.Column(db.Float); hsdpa_throughput=db.Column(db.Float); hsupa_throughput=db.Column(db.Float); cs_so_att=db.Column(db.Float); ps_so_att=db.Column(db.Float); csconges=db.Column(db.Float); psconges=db.Column(db.Float)
class KPI4G(db.Model): __tablename__='kpi_4g'; id=db.Column(db.Integer, primary_key=True); ten_cell=db.Column(db.String(100), index=True); thoi_gian=db.Column(db.String(50)); traffic=db.Column(db.Float); traffic_vol_dl=db.Column(db.Float); traffic_vol_ul=db.Column(db.Float); cell_dl_avg_thputs=db.Column(db.Float); cell_ul_avg_thput=db.Column(db.Float); user_dl_avg_thput=db.Column(db.Float); user_ul_avg_thput=db.Column(db.Float); erab_ssrate_all=db.Column(db.Float); service_drop_all=db.Column(db.Float); unvailable=db.Column(db.Float); res_blk_dl=db.Column(db.Float); cqi_4g=db.Column(db.Float)
class KPI5G(db.Model): __tablename__='kpi_5g'; id=db.Column(db.Integer, primary_key=True); ten_cell=db.Column(db.String(100), index=True); thoi_gian=db.Column(db.String(50)); traffic=db.Column(db.Float); dl_traffic_volume_gb=db.Column(db.Float); ul_traffic_volume_gb=db.Column(db.Float); cell_downlink_average_throughput=db.Column(db.Float); cell_uplink_average_throughput=db.Column(db.Float); user_dl_avg_throughput=db.Column(db.Float); cqi_5g=db.Column(db.Float); cell_avaibility_rate=db.Column(db.Float); sgnb_addition_success_rate=db.Column(db.Float); sgnb_abnormal_release_rate=db.Column(db.Float)
class QoE4G(db.Model): __tablename__='qoe_4g'; id=db.Column(db.Integer, primary_key=True); cell_name=db.Column(db.String(100), index=True); week_name=db.Column(db.String(100)); qoe_score=db.Column(db.Float); qoe_percent=db.Column(db.Float); details=db.Column(db.Text)
class QoS4G(db.Model): __tablename__='qos_4g'; id=db.Column(db.Integer, primary_key=True); cell_name=db.Column(db.String(100), index=True); week_name=db.Column(db.String(100)); qos_score=db.Column(db.Float); qos_percent=db.Column(db.Float); details=db.Column(db.Text)
class ITSLog(db.Model): __tablename__='its_log'; id=db.Column(db.Integer, primary_key=True); timestamp=db.Column(db.String(50)); latitude=db.Column(db.Float); longitude=db.Column(db.Float); networktech=db.Column(db.String(20)); level=db.Column(db.Float); qual=db.Column(db.Float); cellid=db.Column(db.String(50))

@login_manager.user_loader
def load_user(user_id): return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin'); u.set_password('admin123'); db.session.add(u); db.session.commit()
init_database()

# ==============================================================================
# 4. GIAO DIỆN HTML/CSS 
# ==============================================================================
BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>KPI Monitor System</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
    <link href='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.css' rel='stylesheet' />
    <script src='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.js'></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --primary-color: #0078d4; --sidebar-bg: rgba(240, 240, 245, 0.95); }
        body { background: #f3f4f6; font-family: 'Segoe UI', sans-serif; overflow-x: hidden; }
        .sidebar { height: 100vh; width: 260px; position: fixed; top: 0; left: 0; background: var(--sidebar-bg); z-index: 1000; transition: all 0.3s; padding-top: 1rem; overflow-y: auto; padding-bottom: 60px; }
        .sidebar-header { padding: 1.5rem; color: var(--primary-color); font-weight: 600; font-size: 1.5rem; text-align: center; }
        .sidebar-menu { padding: 0; list-style: none; margin: 1rem 0; }
        .sidebar-menu a { display: flex; align-items: center; padding: 14px 25px; color: #555; text-decoration: none; font-weight: 500; margin: 4px 12px; border-radius: 8px; transition: all 0.2s ease; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background: #fff; color: var(--primary-color); box-shadow: 0 2px 8px rgba(0,0,0,0.05); }
        .sidebar-menu i { margin-right: 15px; width: 24px; text-align: center; font-size: 1.1rem; }
        .main-content { margin-left: 260px; padding: 30px; min-height: 100vh; transition: all 0.3s; }
        .card { border: none; border-radius: 12px; background: rgba(255, 255, 255, 0.9); box-shadow: 0 4px 12px rgba(0,0,0,0.05); margin-bottom: 1.5rem; }
        .table { font-size: 0.9rem; } .table thead th { background: rgba(248,249,250,0.8); color: #555; text-transform: uppercase; }
        @media (max-width: 768px) { .sidebar { margin-left: -260px; } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; padding: 15px; } }
        #sidebar-overlay { display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.4); z-index: 999; }
        #sidebar-overlay.active { display: block; }
    </style>
</head>
<body>
    <div id="sidebar-overlay" onclick="toggleSidebar()"></div>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired"></i> NetOps</div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/gis" class="{{ 'active' if active_page == 'gis' else '' }}"><i class="fa-solid fa-map-location-dot"></i> Bản đồ GIS</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="/qoe-qos" class="{{ 'active' if active_page == 'qoe_qos' else '' }}"><i class="fa-solid fa-star-half-stroke"></i> QoE QoS</a></li>
            <li><a href="/optimize" class="{{ 'active' if active_page == 'optimize' else '' }}"><i class="fa-solid fa-wand-magic-sparkles"></i> Tối ưu QoE/QoS</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-pin"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li><a href="#toolsMenu" data-bs-toggle="collapse" class="{{ 'active' if active_page in ['azimuth', 'script'] else '' }}"><i class="fa-solid fa-toolbox"></i> Tools <i class="fa-solid fa-chevron-down ms-auto" style="font-size:0.8rem"></i></a>
                <ul class="collapse list-unstyled {{ 'show' if active_page in ['azimuth', 'script'] else '' }}" id="toolsMenu">
                    <li><a href="/azimuth" class="{{ 'active' if active_page == 'azimuth' else '' }}" style="margin-left:20px"><i class="fa-solid fa-compass"></i> Azimuth</a></li>
                    <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}" style="margin-left:20px"><i class="fa-solid fa-code"></i> Script</a></li>
                </ul>
            </li>
            {% if current_user.role == 'admin' %}
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Data Import</a></li>
            <li class="mt-3 mb-1 text-muted px-4 text-uppercase" style="font-size: 0.75rem;">System</li>
            <li><a href="/users"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="/backup-restore"><i class="fa-solid fa-database"></i> Backup/Restore</a></li>
            {% endif %}
            <li><a href="/profile"><i class="fa-solid fa-user-shield"></i> Profile</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Logout</a></li>
        </ul>
    </div>
    <div class="main-content">
        <button class="btn btn-light shadow-sm d-md-none mb-3 border fw-bold" onclick="toggleSidebar()"><i class="fa-solid fa-bars me-1"></i> Menu</button>
        <div class="container-fluid p-0">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}{% for category, message in messages %}<div class="alert alert-{{ category }} alert-dismissible fade show shadow-sm" role="alert">{{ message }}<button type="button" class="btn-close" data-bs-dismiss="alert"></button></div>{% endfor %}{% endif %}
            {% endwith %}
            {% block content %}{% endblock %}
        </div>
    </div>
    <div class="modal fade" id="chartDetailModal" tabindex="-1"><div class="modal-dialog modal-xl modal-dialog-centered"><div class="modal-content border-0 shadow-lg"><div class="modal-header border-0 pb-0"><h5 class="modal-title text-primary fw-bold" id="modalTitle"></h5><button type="button" class="btn-close" data-bs-dismiss="modal"></button></div><div class="modal-body p-4"><div class="chart-container" style="position:relative; height:65vh; width:100%"><canvas id="modalChart"></canvas></div></div></div></div></div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        function toggleSidebar(){ document.getElementById('sidebar').classList.toggle('active'); document.getElementById('sidebar-overlay').classList.toggle('active'); }
        let modalChartInstance=null;
        function showDetailModal(cName,date,val,mLabel,allDs,allLbls){ document.getElementById('modalTitle').innerText='Chi tiết '+mLabel; const ctx=document.getElementById('modalChart').getContext('2d'); if(modalChartInstance)modalChartInstance.destroy(); modalChartInstance=new Chart(ctx,{type:'line',data:{labels:allLbls,datasets:allDs},options:{responsive:true,maintainAspectRatio:false,interaction:{mode:'nearest',intersect:false}}}); new bootstrap.Modal(document.getElementById('chartDetailModal')).show(); }
        function toggleCheckboxes(src){ let cbs=document.getElementsByName('tables'); for(let i=0;i<cbs.length;i++) cbs[i].checked=src.checked; }
    </script>
</body>
</html>
"""

LOGIN_PAGE = """<!DOCTYPE html><html lang="vi"><head><meta charset="UTF-8"><title>Đăng nhập</title><link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet"><style>body{background:linear-gradient(135deg,#f0f2f5 0%,#d9e2ec 100%);height:100vh;display:flex;align-items:center;justify-content:center;}.login-card{width:100%;max-width:400px;background:rgba(255,255,255,0.9);padding:40px;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,0.08);}.btn-primary{background-color:#0078d4;width:100%;}</style></head><body><div class="login-card"><h3 class="text-center mb-4 text-primary fw-bold">NetOps Login</h3><form method="POST"><input type="text" name="username" class="form-control mb-3" placeholder="Username" required><input type="password" name="password" class="form-control mb-4" placeholder="Password" required><button type="submit" class="btn btn-primary w-100">Sign In</button></form></div></body></html>"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card mb-4 border-0 shadow-sm"><div class="card-header bg-white"><span class="fs-5 fw-bold">{{ title }}</span></div><div class="card-body">
    
    {% if active_page == 'dashboard' %}
        {% if dashboard_data and dashboard_data.labels %}
            <div class="row g-4"><div class="col-md-6"><div class="card shadow-sm h-100"><div class="card-body"><h6 class="fw-bold text-primary mb-3">Tổng Traffic 4G (GB)</h6><div style="height:30vh;"><canvas id="chartTraffic"></canvas></div></div></div></div><div class="col-md-6"><div class="card shadow-sm h-100"><div class="card-body"><h6 class="fw-bold text-success mb-3">Avg User DL Thput (Mbps)</h6><div style="height:30vh;"><canvas id="chartThput"></canvas></div></div></div></div><div class="col-md-6"><div class="card shadow-sm h-100"><div class="card-body"><h6 class="fw-bold text-warning mb-3">Avg PRB DL (%)</h6><div style="height:30vh;"><canvas id="chartPrb"></canvas></div></div></div></div><div class="col-md-6"><div class="card shadow-sm h-100"><div class="card-body"><h6 class="fw-bold text-info mb-3">Avg CQI 4G</h6><div style="height:30vh;"><canvas id="chartCqi"></canvas></div></div></div></div></div>
            <script>document.addEventListener('DOMContentLoaded', function() { const labels = {{ dashboard_data.labels | tojson }}; function buildChart(id, lbl, col, bg, dat) { new Chart(document.getElementById(id), {type:'line', data:{labels:labels, datasets:[{label:lbl, data:dat, borderColor:col, backgroundColor:bg, fill:true, tension:0.3}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}}}); } buildChart('chartTraffic', 'Traffic', '#0078d4', 'rgba(0,120,212,0.1)', {{ dashboard_data.traffic | tojson }}); buildChart('chartThput', 'Thput', '#107c10', 'rgba(16,124,16,0.1)', {{ dashboard_data.thput | tojson }}); buildChart('chartPrb', 'PRB', '#ffaa44', 'rgba(255,170,68,0.1)', {{ dashboard_data.prb | tojson }}); buildChart('chartCqi', 'CQI', '#00bcf2', 'rgba(0,188,242,0.1)', {{ dashboard_data.cqi | tojson }}); });</script>
        {% else %}<div class="alert alert-info">Chưa có dữ liệu KPI 4G.</div>{% endif %}
    
    {% elif active_page == 'azimuth' %}
        <div id="azimuthMap" style="height:70vh; width:100%; border-radius:8px; z-index:1;"></div>
        <div id="azForm" style="position:absolute; top:80px; right:30px; background:rgba(255,255,255,0.9); padding:15px; border-radius:8px; z-index:1000; width:300px; box-shadow:0 0 10px rgba(0,0,0,0.2);"><b>Tọa độ Gốc</b><br><input type="text" id="latO" class="form-control form-control-sm mb-1" placeholder="Lat"><input type="text" id="lngO" class="form-control form-control-sm mb-2" placeholder="Lng"><button class="btn btn-sm btn-secondary w-100 mb-3" onclick="getGPS()">Lấy GPS</button><b>Thêm Điểm (Đích)</b><br><input type="text" id="ptName" class="form-control form-control-sm mb-1" placeholder="Tên Trạm"><input type="number" id="ptAz" class="form-control form-control-sm mb-1" placeholder="Góc (độ)"><input type="number" id="ptDist" class="form-control form-control-sm mb-2" placeholder="Khoảng cách (m)"><button class="btn btn-sm btn-primary w-100 mb-1" onclick="drawAz()">Vẽ</button><button class="btn btn-sm btn-danger w-100" onclick="azMap.eachLayer(l=>{if(l!=googleStreets)azMap.removeLayer(l)}); markerO=null;">Xóa hết</button></div>
        <script>
            var azMap = L.map('azimuthMap').setView([16.0, 106.0], 5); var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {maxZoom:22, subdomains:['mt0','mt1','mt2','mt3']}).addTo(azMap); var markerO;
            azMap.on('click', e=>{ if(!markerO){ document.getElementById('latO').value=e.latlng.lat; document.getElementById('lngO').value=e.latlng.lng; markerO=L.marker(e.latlng).addTo(azMap); }});
            function getGPS(){ navigator.geolocation.getCurrentPosition(p=>{ document.getElementById('latO').value=p.coords.latitude; document.getElementById('lngO').value=p.coords.longitude; azMap.flyTo([p.coords.latitude, p.coords.longitude], 17); markerO=L.marker([p.coords.latitude, p.coords.longitude]).addTo(azMap); }); }
            function drawAz(){ var l1=parseFloat(document.getElementById('latO').value), ln1=parseFloat(document.getElementById('lngO').value), az=parseFloat(document.getElementById('ptAz').value), d=parseFloat(document.getElementById('ptDist').value), nm=document.getElementById('ptName').value; var R=6371e3, bR=az*Math.PI/180, l1R=l1*Math.PI/180, ln1R=ln1*Math.PI/180; var l2R=Math.asin(Math.sin(l1R)*Math.cos(d/R)+Math.cos(l1R)*Math.sin(d/R)*Math.cos(bR)); var ln2R=ln1R+Math.atan2(Math.sin(bR)*Math.sin(d/R)*Math.cos(l1R),Math.cos(d/R)-Math.sin(l1R)*Math.sin(l2R)); var p2=[l2R*180/Math.PI, ln2R*180/Math.PI]; L.marker(p2).bindTooltip(nm).addTo(azMap); L.polyline([[l1,ln1], p2], {color:'red'}).addTo(azMap); }
        </script>
    
    {% elif active_page == 'optimize' %}
        <form method="GET" action="/optimize" class="row g-3 mb-4 bg-light p-3 rounded shadow-sm"><div class="col-md-8"><select name="week_name" class="form-select">{% for w in all_weeks %}<option value="{{ w }}" {% if w == latest_week %}selected{% endif %}>{{ w }}</option>{% endfor %}</select></div><div class="col-md-4 d-flex gap-2"><button type="submit" name="action" value="filter" class="btn btn-danger w-100">Lọc</button><button type="submit" name="action" value="export" class="btn btn-success w-100">Export</button></div></form>
        <div class="table-responsive"><table class="table table-hover table-bordered text-center align-middle"><thead class="table-light"><tr><th rowspan="2">Cell Name</th><th colspan="2">1. Macro</th><th colspan="4">2. Micro</th><th rowspan="2">3. Chẩn đoán</th><th rowspan="2">4. Giải pháp</th></tr><tr><th>QoE</th><th>QoS</th><th>PRB</th><th>Thput</th><th>CQI</th><th>Drop</th></tr></thead><tbody>{% for row in optimized_data %}<tr><td class="fw-bold text-primary">{{ row.cell_name }}</td><td>{{ row.qoe_score }}</td><td>{{ row.qos_score }}</td><td>{{ row.prb }}</td><td>{{ row.thput }}</td><td>{{ row.cqi }}</td><td>{{ row.drop }}</td><td class="text-danger text-start"><ul>{% for i in row.issues %}<li>{{ i }}</li>{% endfor %}</ul></td><td class="text-success text-start"><ul>{% for a in row.actions %}<li>{{ a }}</li>{% endfor %}</ul></td></tr>{% else %}<tr><td colspan="9">Không có dữ liệu vi phạm.</td></tr>{% endfor %}</tbody></table></div>
    
    {% elif active_page == 'gis' %}
        <form method="POST" action="/gis" enctype="multipart/form-data" class="row g-3 mb-3 bg-light p-3 rounded shadow-sm"><div class="col-md-2"><select name="tech" class="form-select"><option value="3g" {% if selected_tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech=='5g' %}selected{% endif %}>5G</option></select></div><div class="col-md-2"><input type="text" name="site_code" class="form-control" placeholder="Site Code" value="{{ site_code_input }}"></div><div class="col-md-3"><input type="text" name="cell_name" class="form-control" placeholder="Cell Name" value="{{ cell_name_input }}"></div><div class="col-md-3"><input type="file" name="its_file" class="form-control" accept=".txt,.csv" multiple></div><div class="col-md-2 d-flex gap-2"><button type="submit" name="action" value="search" class="btn btn-primary w-100">Tìm</button><button type="submit" name="action" value="show_log" class="btn btn-warning w-100">Log</button></div></form>
        <div id="gisMap" style="height: 65vh; width: 100%; border-radius: 8px; z-index:1;"></div>
        <script>document.addEventListener('DOMContentLoaded', function() { var gisData = {{ gis_data|tojson|safe if gis_data else '[]' }}; var map = L.map('gisMap').setView([19.8, 105.7], 9); L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {maxZoom:22, subdomains:['mt0','mt1','mt2','mt3']}).addTo(map); gisData.forEach(c => { if(c.lat&&c.lon) L.circleMarker([c.lat, c.lon], {radius:5}).bindPopup('<b>'+c.cell_name+'</b>').addTo(map); }); if(gisData.length>0 && gisData[0].lat) map.setView([gisData[0].lat, gisData[0].lon], 15); });</script>
    
    {% elif active_page == 'kpi' %}
        <form method="GET" action="/kpi" class="row g-3 mb-4 bg-light p-3 rounded shadow-sm"><div class="col-md-2"><select name="tech" class="form-select"><option value="3g" {% if selected_tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech=='5g' %}selected{% endif %}>5G</option></select></div><div class="col-md-4"><input type="text" name="poi_name" list="poi_list_kpi" class="form-control" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div><div class="col-md-4"><input type="text" name="cell_name" class="form-control" placeholder="Cell Name..." value="{{ cell_name_input }}"></div><div class="col-md-2"><button type="submit" class="btn btn-primary w-100">Xem</button></div></form>
        {% for cid, cd in charts.items() %}<div class="card mb-4"><div class="card-body"><h6 class="fw-bold">{{ cd.title }}</h6><div style="height:45vh;"><canvas id="{{ cid }}"></canvas></div></div></div><script>new Chart(document.getElementById('{{ cid }}'), {type:'line', data:{{ cd|tojson }}, options:{responsive:true, maintainAspectRatio:false}});</script>{% endfor %}
    
    {% elif active_page == 'qoe_qos' %}
        <form method="GET" action="/qoe-qos" class="row g-3 mb-4 bg-light p-3 rounded shadow-sm"><div class="col-md-8"><input type="text" name="cell_name" class="form-control" placeholder="Cell Name 4G..." value="{{ cell_name_input }}" required></div><div class="col-md-4"><button type="submit" class="btn btn-primary w-100">Xem</button></div></form>
        <div class="row">{% for cid, cd in charts.items() %}<div class="col-md-6 mb-4"><div class="card"><div class="card-body"><h6 class="fw-bold">{{ cd.title }}</h6><div style="height:35vh;"><canvas id="{{ cid }}"></canvas></div></div></div></div><script>new Chart(document.getElementById('{{ cid }}'), {type:'line', data:{{ cd|tojson }}, options:{responsive:true, maintainAspectRatio:false}});</script>{% endfor %}</div>
    
    {% elif active_page == 'poi' %}
        <form method="GET" action="/poi" class="row g-3 mb-4 bg-light p-3 rounded shadow-sm"><div class="col-md-8"><input type="text" name="poi_name" list="poi_list" class="form-control" placeholder="Tên POI..." value="{{ selected_poi }}"><datalist id="poi_list">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div><div class="col-md-4"><button type="submit" class="btn btn-primary w-100">Xem Báo Cáo</button></div></form>
        <div class="row">{% for cid, cd in poi_charts.items() %}<div class="col-md-6 mb-4"><div class="card"><div class="card-body"><h6 class="fw-bold">{{ cd.title }}</h6><div style="height:35vh;"><canvas id="{{ cid }}"></canvas></div></div></div></div><script>new Chart(document.getElementById('{{ cid }}'), {type:'line', data:{{ cd|tojson }}, options:{responsive:true, maintainAspectRatio:false}});</script>{% endfor %}</div>
    
    {% elif active_page == 'worst_cell' %}
        <form method="GET" action="/worst-cell" class="row g-3 mb-4 bg-light p-3 rounded shadow-sm"><div class="col-auto"><select name="duration" class="form-select"><option value="1">1 Ngày</option><option value="3">3 Ngày</option><option value="7">7 Ngày</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-danger">Lọc</button><button type="submit" name="action" value="export" class="btn btn-success ms-2">Export</button></div></form>
        <div class="table-responsive"><table class="table table-hover table-bordered text-center"><thead class="table-light"><tr><th>Cell Name</th><th>Avg Thput</th><th>Avg PRB</th><th>Avg CQI</th><th>Avg Drop Rate</th></tr></thead><tbody>{% for r in worst_cells %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td>{{ r.avg_thput }}</td><td>{{ r.avg_res_blk }}</td><td>{{ r.avg_cqi }}</td><td>{{ r.avg_drop }}</td></tr>{% endfor %}</tbody></table></div>
    
    {% elif active_page == 'traffic_down' %}
        <form method="GET" action="/traffic-down" class="row g-3 mb-4 bg-light p-3 rounded shadow-sm"><div class="col-auto"><select name="tech" class="form-select"><option value="3g">3G</option><option value="4g">4G</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-primary">Thực hiện</button><button type="submit" name="action" value="export_zero" class="btn btn-success ms-2">Zero</button><button type="submit" name="action" value="export_degraded" class="btn btn-warning ms-2">Degraded</button></div></form>
        <div class="row"><div class="col-md-6"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-danger text-white fw-bold">Zero Traffic</div><div class="card-body p-0"><table class="table table-striped mb-0"><tr><th>Cell</th><th>Today</th><th>Avg 7D</th></tr>{% for r in zero_traffic %}<tr><td>{{ r.cell_name }}</td><td>{{ r.traffic_today }}</td><td>{{ r.avg_last_7 }}</td></tr>{% endfor %}</table></div></div></div><div class="col-md-6"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">Degraded</div><div class="card-body p-0"><table class="table table-striped mb-0"><tr><th>Cell</th><th>Today</th><th>Last Wk</th><th>Degrade</th></tr>{% for r in degraded %}<tr><td>{{ r.cell_name }}</td><td>{{ r.traffic_today }}</td><td>{{ r.traffic_last_week }}</td><td>-{{ r.degrade_percent }}%</td></tr>{% endfor %}</table></div></div></div></div>
    
    {% elif active_page == 'conges_3g' %}
        <form method="GET" action="/conges-3g" class="mb-4"><button type="submit" name="action" value="execute" class="btn btn-primary">Thực hiện</button><button type="submit" name="action" value="export" class="btn btn-success ms-2">Export</button></form>
        <div class="table-responsive"><table class="table table-bordered text-center"><thead class="table-light"><tr><th>Cell Name</th><th>CS Traffic</th><th>CS Conges</th><th>PS Traffic</th><th>PS Conges</th></tr></thead><tbody>{% for r in conges_data %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td>{{ r.avg_cs_traffic }}</td><td>{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_traffic }}</td><td>{{ r.avg_ps_conges }}</td></tr>{% endfor %}</tbody></table></div>
    
    {% elif active_page == 'rf' %}
        <div class="d-flex justify-content-between mb-3"><div class="btn-group"><a href="/rf?tech=3g" class="btn {{ 'btn-primary' if current_tech=='3g' else 'btn-outline-primary' }}">3G</a><a href="/rf?tech=4g" class="btn {{ 'btn-primary' if current_tech=='4g' else 'btn-outline-primary' }}">4G</a><a href="/rf?tech=5g" class="btn {{ 'btn-primary' if current_tech=='5g' else 'btn-outline-primary' }}">5G</a></div><form method="GET" action="/rf" class="d-flex"><input type="hidden" name="tech" value="{{ current_tech }}"><input type="text" name="cell_search" class="form-control me-2" placeholder="Tìm trạm..." value="{{ search_query }}"><button type="submit" class="btn btn-primary">Tìm</button></form><form method="GET" action="/rf"><input type="hidden" name="tech" value="{{ current_tech }}"><button type="submit" name="action" value="export" class="btn btn-success">Export</button></form></div>
        <div class="table-responsive" style="max-height:65vh;"><table class="table table-hover table-sm text-nowrap"><thead class="table-light position-sticky top-0"><tr><th>Action</th>{% for c in rf_columns %}<th>{{ c }}</th>{% endfor %}</tr></thead><tbody>{% for r in rf_data %}<tr><td><a href="/rf/detail/{{ current_tech }}/{{ r.id }}" class="btn btn-sm btn-info py-0">Xem</a></td>{% for c in rf_columns %}<td>{{ r[c] }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div>
    
    {% elif active_page == 'import' %}
        <div class="row"><div class="col-md-8"><div class="card"><div class="card-body"><ul class="nav nav-tabs mb-3"><li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabRF">RF</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabPOI">POI</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabKPI">KPI</button></li><li class="nav-item"><button class="nav-link text-primary fw-bold" data-bs-toggle="tab" data-bs-target="#tabQoE">QoE</button></li><li class="nav-item"><button class="nav-link text-danger fw-bold" data-bs-toggle="tab" data-bs-target="#tabReset">Reset</button></li></ul>
        <div class="tab-content">
            <div class="tab-pane fade show active" id="tabRF"><form action="/import" method="POST" enctype="multipart/form-data"><select name="type" class="form-select mb-3"><option value="3g">RF 3G (Gộp tự động Config & CELL)</option><option value="4g">RF 4G</option><option value="5g">RF 5G</option></select><input type="file" name="file" class="form-control mb-3" multiple required><button class="btn btn-primary w-100">Upload RF</button></form></div>
            <div class="tab-pane fade" id="tabPOI"><form action="/import" method="POST" enctype="multipart/form-data"><select name="type" class="form-select mb-3"><option value="poi4g">POI 4G</option><option value="poi5g">POI 5G</option></select><input type="file" name="file" class="form-control mb-3" multiple required><button class="btn btn-primary w-100">Upload POI</button></form></div>
            <div class="tab-pane fade" id="tabKPI"><form action="/import" method="POST" enctype="multipart/form-data"><select name="type" class="form-select mb-3"><option value="kpi3g">KPI 3G</option><option value="kpi4g">KPI 4G</option><option value="kpi5g">KPI 5G</option></select><input type="file" name="file" class="form-control mb-3" multiple required><button class="btn btn-primary w-100">Upload KPI</button></form></div>
            <div class="tab-pane fade" id="tabQoE"><form action="/import" method="POST" enctype="multipart/form-data"><select name="type" id="importQoEQoSType" class="form-select mb-3"><option value="qoe4g">QoE 4G</option><option value="qos4g">QoS 4G</option></select><input type="text" name="week_name" id="importWeekName" class="form-control mb-3" value="{{ next_qoe }}" required><input type="file" name="file" class="form-control mb-3" multiple required><button class="btn btn-primary w-100">Upload Data</button></form></div>
            <div class="tab-pane fade" id="tabReset"><form action="/reset-data" method="POST"><input type="hidden" name="target" value="rf"><button class="btn btn-danger w-100 mb-2" onclick="return confirm('Xóa hết cấu hình RF?')">Reset Toàn Bộ RF</button></form></div>
        </div></div></div></div>
        <div class="col-md-4"><div class="card h-100"><div class="card-header bg-success text-white">Lịch sử KPI</div><div class="card-body p-0 overflow-auto" style="max-height:400px"><table class="table table-sm text-center"><tr><th>3G</th><th>4G</th><th>5G</th></tr>{% for r3, r4, r5 in kpi_rows %}<tr><td>{{ r3 or '-' }}</td><td>{{ r4 or '-' }}</td><td>{{ r5 or '-' }}</td></tr>{% endfor %}</table></div></div></div></div>
        <script>document.getElementById('importQoEQoSType').addEventListener('change', function(){document.getElementById('importWeekName').value=this.value==='qoe4g'?'{{ next_qoe }}':'{{ next_qos }}';});</script>
    
    {% elif active_page == 'script' %}
        <div class="card"><div class="card-header">Script Gen</div><div class="card-body">
        <ul class="nav nav-tabs mb-3"><li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab3g900">3G 900</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab4g">4G</button></li></ul>
        <div class="tab-content">
            <div class="tab-pane fade show active" id="tab3g900"><form method="POST" action="/script"><input type="hidden" name="tech" value="3g900"><table class="table" id="rruTable_3g900"><tr><th>Name</th><th>SRN</th><th>Action</th></tr><tr><td><input name="rn[]" value="RRU1"></td><td><input name="srn[]" value="70"></td><td><button type="button" onclick="this.closest('tr').remove()">X</button></td><input type="hidden" name="hsn[]" value="2"><input type="hidden" name="hpn[]" value="0"><input type="hidden" name="rcn[]" value="0"><input type="hidden" name="sectorid[]" value="0"><input type="hidden" name="rxnum[]" value="2"><input type="hidden" name="txnum[]" value="1"></tr></table><button type="button" class="btn btn-success" onclick="addRow('3g900')">+ Add</button> <button class="btn btn-primary">Generate</button></form></div>
            <div class="tab-pane fade" id="tab4g"><form method="POST" action="/script"><input type="hidden" name="tech" value="4g"><table class="table" id="rruTable_4g"><tr><th>Name</th><th>SRN</th><th>Action</th></tr><tr><td><input name="rn[]" value="RRU1"></td><td><input name="srn[]" value="60"></td><td><button type="button" onclick="this.closest('tr').remove()">X</button></td><input type="hidden" name="hsn[]" value="3"><input type="hidden" name="hpn[]" value="0"><input type="hidden" name="rcn[]" value="0"><input type="hidden" name="sectorid[]" value="0"><input type="hidden" name="rxnum[]" value="4"><input type="hidden" name="txnum[]" value="4"></tr></table><button type="button" class="btn btn-success" onclick="addRow('4g')">+ Add</button> <button class="btn btn-primary">Generate</button></form></div>
        </div>
        <textarea class="form-control mt-3" rows="10">{{ script_result }}</textarea></div></div>
    {% endif %}
</div></div>
{% endblock %}
"""

app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': """{% extends "base" %}{% block content %}<div class="row"><div class="col-md-6"><div class="card"><div class="card-header bg-primary text-white">Backup Database</div><div class="card-body"><form action="/backup" method="POST"><input class="form-check-input" type="checkbox" id="selectAll" onclick="toggleCheckboxes(this)"> Select All<hr><input type="checkbox" name="tables" value="users.csv"> Users<br><input type="checkbox" name="tables" value="config_3g.csv"> Config 3G<br><input type="checkbox" name="tables" value="cell_3g.csv"> CELL 3G<br><input type="checkbox" name="tables" value="rf3g.csv"> RF 3G<br><input type="checkbox" name="tables" value="rf4g.csv"> RF 4G<br><input type="checkbox" name="tables" value="rf5g.csv"> RF 5G<br><input type="checkbox" name="tables" value="poi4g.csv"> POI 4G<br><input type="checkbox" name="tables" value="poi5g.csv"> POI 5G<br><input type="checkbox" name="tables" value="kpi3g.csv"> KPI 3G<br><input type="checkbox" name="tables" value="kpi4g.csv"> KPI 4G<br><input type="checkbox" name="tables" value="kpi5g.csv"> KPI 5G<br><input type="checkbox" name="tables" value="qoe_4g.csv"> QoE 4G<br><input type="checkbox" name="tables" value="qos_4g.csv"> QoS 4G<br><button type="submit" class="btn btn-primary w-100 mt-3">Download</button></form></div></div></div><div class="col-md-6"><div class="card"><div class="card-header bg-warning">Restore Database</div><div class="card-body"><form action="/restore" method="POST" enctype="multipart/form-data"><input class="form-control mb-3" type="file" name="file" accept=".zip" required><button type="submit" class="btn btn-warning w-100" onclick="return confirm('Cảnh báo ghi đè toàn bộ dữ liệu. Tiếp tục?')">Restore</button></form></div></div></div></div>{% endblock %}""",
    'users': """{% extends "base" %}{% block content %}<div class="row"><div class="col-md-4"><div class="card"><div class="card-header bg-white fw-bold">Add User</div><div class="card-body"><form method="POST" action="/users/add"><input name="username" class="form-control mb-2" placeholder="Username" required><input name="password" type="password" class="form-control mb-2" placeholder="Password" required><select name="role" class="form-select mb-3"><option value="user">User</option><option value="admin">Admin</option></select><button class="btn btn-success w-100">Create</button></form></div></div></div><div class="col-md-8"><div class="card"><div class="card-header bg-white fw-bold">Users</div><div class="table-responsive"><table class="table mb-0"><thead class="table-light"><tr><th>ID</th><th>User</th><th>Role</th><th>Action</th></tr></thead><tbody>{% for u in users %}<tr><td>{{ u.id }}</td><td class="fw-bold">{{ u.username }}</td><td><span class="badge bg-secondary">{{ u.role }}</span></td><td>{% if u.username!='admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-danger">Del</a>{% endif %}</td></tr>{% endfor %}</tbody></table></div></div></div></div>{% endblock %}""",
    'profile': """{% extends "base" %}{% block content %}<div class="row justify-content-center"><div class="col-md-6"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Change Password</div><div class="card-body"><form method="POST" action="/change-password"><input type="password" name="current_password" class="form-control mb-3" placeholder="Current Password" required><input type="password" name="new_password" class="form-control mb-3" placeholder="New Password" required><button class="btn btn-primary w-100 shadow-sm">Save Changes</button></form></div></div></div></div>{% endblock %}""",
    'rf_form': """{% extends "base" %}{% block content %}<div class="card"><div class="card-header bg-white fw-bold">{{ title }}</div><div class="card-body"><form method="POST"><div class="row">{% for col in columns %}<div class="col-md-4 mb-3"><label class="small text-muted">{{ col }}</label><input type="text" name="{{ col }}" class="form-control" value="{{ obj[col] if obj and col in obj else '' }}"></div>{% endfor %}</div><button type="submit" class="btn btn-primary">Save</button></form></div></div>{% endblock %}""",
    'rf_detail': """{% extends "base" %}{% block content %}<div class="card"><div class="card-header bg-white fw-bold d-flex justify-content-between"><span>Detail</span><a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm">Quay lại</a></div><div class="card-body p-0"><table class="table table-bordered mb-0">{% for k,v in obj.items() %}<tr><th class="w-25 text-end">{{ k }}</th><td class="fw-bold">{{ v }}</td></tr>{% endfor %}</table></div></div>{% endblock %}"""
})
def render_page(tpl, **kwargs): return render_template_string(tpl if tpl.startswith('{%') else app.jinja_loader.get_source(app.jinja_env, tpl)[0], **kwargs)

# ==============================================================================
# 5. API IMPORT DỮ LIỆU & GỘP FILE 3G
# ==============================================================================
@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.form.get('type')
        
        # 1. NHÁNH IMPORT QoE/QoS
        if itype in ['qoe4g', 'qos4g']:
            week_name = request.form.get('week_name', 'Tuần')
            TargetModel = QoE4G if itype == 'qoe4g' else QoS4G
            try: db.session.query(TargetModel).filter_by(week_name=week_name).delete(); db.session.commit()
            except: db.session.rollback()

            for file in files:
                try:
                    df = pd.read_excel(file, header=None) if file.filename.endswith('.xlsx') else pd.read_csv(file, header=None)
                    h_idx, c_idx = -1, -1
                    for i, row in df.iterrows():
                        for j, val in enumerate(row):
                            if str(val).lower().strip() in ['cell name', 'tên cell']: h_idx, c_idx = i, j; break
                        if h_idx != -1: break
                        
                    if h_idx != -1 and c_idx != -1:
                        headers = [str(df.iloc[h_idx, j]).strip() for j in range(len(df.columns))]
                        records = []
                        for i in range(h_idx + 1, len(df)):
                            row_data = df.iloc[i]
                            c_name = str(row_data[c_idx]).strip()
                            if not c_name or c_name.lower() in ['nan', 'none']: continue
                            try: val1 = float(str(row_data[c_idx + 2]).replace(',', '.'))
                            except: val1 = 0.0
                            try: val2 = float(str(row_data[c_idx + 3]).replace(',', '.'))
                            except: val2 = 0.0
                            if math.isnan(val1): val1 = 0.0; 
                            if math.isnan(val2): val2 = 0.0
                            percent, score = max(val1, val2), min(val1, val2)
                            details = json.dumps({headers[j]: str(row_data[j]).strip() for j in range(len(headers)) if pd.notna(row_data[j])}, ensure_ascii=False)
                            records.append({'cell_name': c_name, 'week_name': week_name, 'qoe_score' if itype == 'qoe4g' else 'qos_score': score, 'qoe_percent' if itype == 'qoe4g' else 'qos_percent': percent, 'details': details})
                        if records:
                            db.session.bulk_insert_mappings(TargetModel, records); db.session.commit()
                            flash(f'Import thành công {len(records)} dòng QoE/QoS.', 'success')
                except Exception as e: flash(f'Lỗi: {e}', 'danger')
                
        # 2. NHÁNH IMPORT RF/KPI VÀ THUẬT TOÁN GỘP FILE 3G
        else:
            cfg = {'3g': [Config3G, Cell3G], '4g': RF4G, '5g': RF5G, 'kpi3g': KPI3G, 'kpi4g': KPI4G, 'kpi5g': KPI5G, 'poi4g': POI4G, 'poi5g': POI5G}
            if itype == '3g': files = sorted(files, key=lambda f: 0 if 'config' in getattr(f, 'filename', '').lower() else 1)
                
            for file in files:
                try:
                    f_name_lower = file.filename.lower()
                    if f_name_lower.endswith('.csv'): preview_df = pd.read_csv(file, header=None, nrows=20, encoding='utf-8-sig', on_bad_lines='skip')
                    else: preview_df = pd.read_excel(file, header=None, nrows=20)
                        
                    h_idx = 0
                    for i in range(len(preview_df)):
                        row_vals = [str(x).lower().strip() for x in preview_df.iloc[i].values if pd.notna(x)]
                        if any(kw in row_vals for kw in ['mã node', 'cell name', 'tên cell', 'site name', 'mã cell', 'tên trên hệ thống']):
                            if not any('lọc kpi' in val for val in row_vals): h_idx = i; break
                    
                    current_itype = itype
                    is_update_only = False
                    if itype == '3g':
                        raw_headers_str = " ".join([str(x).lower().strip() for x in preview_df.iloc[h_idx].values if pd.notna(x)])
                        if 'hoàn cảnh ra đời' in raw_headers_str or 'antenna tên hãng sx' in raw_headers_str or 'tên trên hệ thống' in raw_headers_str:
                            current_itype = 'cell3g'; is_update_only = True
                        else: current_itype = 'config3g'
                            
                    Model = cfg.get(current_itype) if itype == '3g' else cfg.get(itype)
                    if not Model: continue
                    valid_cols = [c.key for c in Model.__table__.columns if c.key != 'id']
                    
                    file.seek(0)
                    CHUNK_SIZE = 2500
                    if f_name_lower.endswith('.csv'): chunks = pd.read_csv(file, header=h_idx, encoding='utf-8-sig', on_bad_lines='skip', low_memory=False, chunksize=CHUNK_SIZE)
                    else:
                        full_df = pd.read_excel(BytesIO(file.read()), header=h_idx)
                        chunks = [full_df[i:i + CHUNK_SIZE] for i in range(0, full_df.shape[0], CHUNK_SIZE)]
                        del full_df; gc.collect()

                    total_inserted = 0
                    for df in chunks:
                        raw_headers = list(df.columns)
                        df.columns = [clean_header(c, current_itype, raw_headers) for c in df.columns]
                        records_to_process = []
                        cell_codes_in_chunk = set()
                        
                        for row in df.to_dict('records'):
                            clean_row = {}
                            for k, v in row.items():
                                if k in valid_cols and pd.notna(v):
                                    col_type = Model.__table__.columns[k].type
                                    val = str(v).strip()
                                    if val.lower() in ['nan', 'none', 'null', 'na', 'n/a', 'no', '']: clean_row[k] = None
                                    elif isinstance(col_type, db.Float):
                                        try: clean_row[k] = float(val.replace(',', '.'))
                                        except ValueError: clean_row[k] = None
                                    elif isinstance(col_type, db.Integer):
                                        try: clean_row[k] = int(float(val.replace(',', '.')))
                                        except ValueError: clean_row[k] = None
                                    else:
                                        if val.endswith('.0'):
                                            try: clean_row[k] = str(int(float(val)))
                                            except: clean_row[k] = val
                                        else: clean_row[k] = val[:250] if len(val)>250 else val
                            
                            # TỰ ĐỘNG ÉP IN HOA KHÓA CHÍNH
                            if 'cell_code' in clean_row and clean_row['cell_code']:
                                clean_row['cell_code'] = str(clean_row['cell_code']).strip().upper()
                                
                            if clean_row:
                                records_to_process.append(clean_row)
                                if 'cell_code' in clean_row and clean_row['cell_code']:
                                    cell_codes_in_chunk.add(str(clean_row['cell_code']).strip())

                        is_rf_model = current_itype in ['config3g', 'cell3g', '4g', '5g']
                        
                        if is_rf_model and records_to_process:
                            existing_rf_db = db.session.query(Model).filter(Model.cell_code.in_(list(cell_codes_in_chunk))).all()
                            existing_rf_map = {str(r.cell_code).upper(): r for r in existing_rf_db if r.cell_code}
                            
                            for cr in records_to_process:
                                cc = str(cr.get('cell_code', '')).strip().upper()
                                if cc in existing_rf_map:
                                    obj = existing_rf_map[cc]
                                    for k, v in cr.items():
                                        if v is not None: setattr(obj, k, v)
                                else:
                                    if not is_update_only:
                                        new_obj = Model(**cr); existing_rf_map[cc] = new_obj; db.session.add(new_obj)
                                    
                            db.session.commit(); total_inserted += len(records_to_process)
                            
                        elif not is_rf_model and records_to_process:
                            db.session.bulk_insert_mappings(Model, records_to_process); db.session.commit()
                            total_inserted += len(records_to_process)
                            
                        del records_to_process; del cell_codes_in_chunk; gc.collect()

                    flash(f'Import hoàn tất file {file.filename} ({total_inserted} dòng)', 'success')
                except Exception as e: db.session.rollback(); flash(f'Lỗi file {file.filename}: {e}', 'danger')
                    
            # TỰ ĐỘNG GỘP BẢNG CONFIG VÀ CELL VÀO RF3G CHÍNH
            if itype == '3g':
                try:
                    db.session.query(RF3G).delete(); db.session.commit()
                    configs = {str(c.cell_code).upper(): c for c in Config3G.query.all() if c.cell_code}
                    cells = {str(c.cell_code).upper(): c for c in Cell3G.query.all() if c.cell_code}
                    
                    rf3g_inserts = []
                    for cc, cfg_row in configs.items():
                        cell_row = cells.get(cc)
                        rf3g_inserts.append({
                            'cell_code': cc, 'site_code': cfg_row.site_code, 'cell_name': cfg_row.cell_name, 'csht_code': cfg_row.csht_code, 'latitude': cfg_row.latitude, 'longitude': cfg_row.longitude, 'antena': cfg_row.antena, 'azimuth': cfg_row.azimuth, 'total_tilt': cfg_row.total_tilt, 'equipment': cfg_row.equipment, 'frequency': cfg_row.frequency, 'psc': cfg_row.psc, 'dl_uarfcn': cfg_row.dl_uarfcn, 'bsc_lac': cfg_row.bsc_lac, 'ci': cfg_row.ci, 'anten_height': cfg_row.anten_height, 'm_t': cfg_row.m_t, 'e_t': cfg_row.e_t, 'hang_sx': cell_row.hang_sx if cell_row else None, 'swap': cell_row.swap if cell_row else None, 'start_day': cell_row.start_day if cell_row else None, 'ghi_chu': cell_row.ghi_chu if cell_row else None
                        })
                    
                    if rf3g_inserts:
                        db.session.bulk_insert_mappings(RF3G, rf3g_inserts); db.session.commit()
                        flash(f'Đã tự động tổng hợp {len(rf3g_inserts)} trạm hoàn chỉnh vào Danh bạ RF 3G!', 'info')
                except Exception as e: db.session.rollback(); flash(f'Lỗi Gộp RF 3G: {e}', 'danger')

        return redirect(url_for('import_data'))

    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    today = datetime.now()
    start_w = today - timedelta(days=today.weekday())
    next_w_str = f"Tuần {today.isocalendar()[1]:02d} ({start_w.strftime('%d/%m')}-{(start_w + timedelta(days=6)).strftime('%d/%m')})"
    return render_page(CONTENT_TEMPLATE, title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), next_qoe=next_w_str, next_qos=next_w_str)

# ==============================================================================
# 6. API TÌM KIẾM, TỐI ƯU (BẢO TOÀN LOGIC QUERY CHỐNG TRỐNG DỮ LIỆU)
# ==============================================================================
@app.route('/optimize')
@login_required
def optimize():
    action = request.args.get('action')
    qoe_weeks = [r[0] for r in db.session.query(QoE4G.week_name).distinct().all()]
    qos_weeks = [r[0] for r in db.session.query(QoS4G.week_name).distinct().all()]
    all_weeks = sorted(list(set([w for w in qoe_weeks + qos_weeks if w])), reverse=True)
    selected_week = request.args.get('week_name') or (all_weeks[0] if all_weeks else None)
    
    bad_cells_dict = {}
    if selected_week:
        l900_cells = {c[0] for c in db.session.query(RF4G.cell_code).filter(RF4G.frequency.ilike('%L900%')).all()}
        qoe_bad = QoE4G.query.filter((QoE4G.week_name == selected_week) & ((QoE4G.qoe_score <= 2) | (QoE4G.qoe_percent < 80))).all()
        qos_bad = QoS4G.query.filter((QoS4G.week_name == selected_week) & ((QoS4G.qos_score <= 3) | (QoS4G.qos_percent < 90))).all()
        
        def is_trash(c_name):
            c_str = str(c_name).strip().upper()
            if not c_str or c_str in ['NAN', 'NONE', 'NULL'] or len(c_str) < 5 or c_str.replace('.', '', 1).isdigit() or c_str in l900_cells or c_str.startswith('VNP-4G') or c_str.startswith('MBF_TH'): return True
            return False

        for r in qoe_bad:
            if is_trash(r.cell_name): continue
            bad_cells_dict[r.cell_name] = {'qoe_score': r.qoe_score, 'qoe_percent': r.qoe_percent, 'qos_score': '-', 'qos_percent': '-'}
        for r in qos_bad:
            if is_trash(r.cell_name): continue
            if r.cell_name not in bad_cells_dict: bad_cells_dict[r.cell_name] = {'qoe_score': '-', 'qoe_percent': '-', 'qos_score': r.qos_score, 'qos_percent': r.qos_percent}
            else: bad_cells_dict[r.cell_name]['qos_score'] = r.qos_score; bad_cells_dict[r.cell_name]['qos_percent'] = r.qos_percent
        
        if bad_cells_dict:
            cell_names = list(bad_cells_dict.keys())
            latest_dates = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).limit(3).all()]
            if latest_dates:
                kpi_records = db.session.query(KPI4G.ten_cell, func.avg(KPI4G.res_blk_dl).label('avg_prb'), func.avg(KPI4G.user_dl_avg_thput).label('avg_thput'), func.avg(KPI4G.cqi_4g).label('avg_cqi'), func.avg(KPI4G.service_drop_all).label('avg_drop')).filter(KPI4G.ten_cell.in_(cell_names), KPI4G.thoi_gian.in_(latest_dates)).group_by(KPI4G.ten_cell).all()
                for r in kpi_records:
                    c = r.ten_cell
                    if c in bad_cells_dict:
                        prb, thput, cqi, drop = r.avg_prb or 0, r.avg_thput or 0, r.avg_cqi or 0, r.avg_drop or 0
                        issues, actions = [], []
                        if prb > 20 and thput < 10: issues.append("Nghẽn"); actions.append("Cân bằng tải/Thêm Carrier")
                        if cqi < 93: issues.append("Vô tuyến kém/Nhiễu"); actions.append("Chỉnh Tx Power/Tilt/Azimuth")
                        if drop > 0.3 and prb <= 20: issues.append("Lỗi Thiết bị/Truyền dẫn"); actions.append("NOC reset Card/Đo kiểm")
                        if not issues: issues.append("Chưa rõ"); actions.append("Theo dõi sâu")
                        bad_cells_dict[c].update({'prb': round(prb, 2), 'thput': round(thput, 2), 'cqi': round(cqi, 2), 'drop': round(drop, 2), 'issues': issues, 'actions': actions})
                    
    optimized_data = []
    for cell, data in bad_cells_dict.items():
        data['cell_name'] = cell
        if 'issues' not in data: data.update({'prb': '-', 'thput': '-', 'cqi': '-', 'drop': '-', 'issues': ['Thiếu KPI'], 'actions': ['Import KPI']})
        optimized_data.append(data)
        
    if action == 'export':
        df = pd.DataFrame([{'Cell Name': d.get('cell_name'), 'QoE Score': d.get('qoe_score'), 'QoE %': d.get('qoe_percent'), 'QoS Score': d.get('qos_score'), 'QoS %': d.get('qos_percent'), 'PRB (%)': d.get('prb'), 'Thput (Mbps)': d.get('thput'), 'CQI (%)': d.get('cqi'), 'Drop (%)': d.get('drop'), 'Chẩn đoán': " | ".join(d.get('issues',[])), 'Giải pháp': " | ".join(d.get('actions',[]))} for d in optimized_data])
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
        output.seek(0)
        return send_file(output, download_name=f'ToiUu_{selected_week}.xlsx', as_attachment=True)
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Tối ưu QoE/QoS", active_page='optimize', optimized_data=optimized_data, latest_week=selected_week, all_weeks=all_weeks)

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis():
    action_type = request.form.get('action', 'search') if request.method == 'POST' else 'search'
    tech = request.form.get('tech', '4g') if request.method == 'POST' else request.args.get('tech', '4g')
    site_code_input = request.form.get('site_code', '').strip() if request.method == 'POST' else request.args.get('site_code', '').strip()
    cell_name_input = request.form.get('cell_name', '').strip() if request.method == 'POST' else request.args.get('cell_name', '').strip()
    
    show_its, its_data, matched_sites, gis_data, cols = False, [], set(), [], []

    def clean_val(v):
        if pd.isna(v) or v is None or str(v).strip() in ['-', '', 'nan', 'none']: return None
        try: return str(int(float(v))) if float(v).is_integer() else str(float(v))
        except: return str(v).strip().upper()

    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db_mapping = {}
    
    if Model and action_type == 'show_log':
        if tech == '4g':
            for sc, en, lc in db.session.query(Model.site_code, Model.enodeb_id, Model.lcrid).all():
                if sc and clean_val(en) and clean_val(lc): db_mapping[f"{clean_val(en)}_{clean_val(lc)}"] = sc
        elif tech == '3g':
            for sc, ci in db.session.query(Model.site_code, Model.ci).all():
                if sc and clean_val(ci): db_mapping[clean_val(ci)] = sc
        elif tech == '5g':
            for sc, gn, lc in db.session.query(Model.site_code, Model.gnodeb_id, Model.lcrid).all():
                if sc and clean_val(gn) and clean_val(lc): db_mapping[f"{clean_val(gn)}_{clean_val(lc)}"] = sc
    
    if request.method == 'POST' and 'its_file' in request.files:
        for file in request.files.getlist('its_file'):
            if file and file.filename:
                show_its = True
                try:
                    lines = file.read().decode('utf-8-sig', errors='ignore').splitlines()
                    if len(lines) > 1:
                        headers = [h.strip().lower() for h in lines[0].split('|' if '|' in lines[0] else ',')]
                        lat_i, lon_i, node_i, cell_i, lvl_i, tech_i, qual_i = (headers.index(k) if k in headers else -1 for k in ['latitude', 'longitude', 'node', 'cellid', 'level', 'networktech', 'qual'])
                        if lat_i != -1 and lon_i != -1:
                            for line in lines[1:]:
                                p = line.split('|' if '|' in line else ',')
                                if len(p) > max(lat_i, lon_i):
                                    try:
                                        lat, lon = float(p[lat_i]), float(p[lon_i])
                                        n, c = clean_val(p[node_i]) if node_i!=-1 else None, clean_val(p[cell_i]) if cell_i!=-1 else None
                                        if action_type == 'show_log':
                                            key = f"{n}_{c}" if tech in ['4g', '5g'] else c
                                            if key in db_mapping: matched_sites.add(db_mapping[key])
                                        its_data.append({'lat': lat, 'lon': lon, 'level': float(p[lvl_i]) if lvl_i!=-1 else 0, 'qual': p[qual_i] if qual_i!=-1 else '', 'tech': (p[tech_i] if tech_i!=-1 else tech).upper(), 'cellid': c or '', 'node': n or ''})
                                    except: pass
                except: pass
        if len(its_data) > 10000: its_data = random.sample(its_data, 10000)

    if Model:
        query = db.session.query(Model)
        if action_type == 'show_log' and show_its:
            query = query.filter(Model.site_code.in_(list(matched_sites)[:500])) if matched_sites else query.filter(text("1=0"))
        else:
            if site_code_input: query = query.filter(Model.site_code.ilike(f"%{site_code_input}%"))
            if cell_name_input: query = query.filter(or_(Model.cell_code.ilike(f"%{cell_name_input}%"), Model.cell_name.ilike(f"%{cell_name_input}%")))
            query = query.limit(2000)

        cols = RF_COLS_ORDER.get(tech, [c.key for c in Model.__table__.columns if c.key != 'id'])
        for r in query.all():
            try:
                lat, lon = float(r.latitude), float(r.longitude)
                if 8 <= lat <= 24 and 102 <= lon <= 110:
                    gis_data.append({'cell_name': getattr(r, 'cell_name', str(r.cell_code)), 'site_code': r.site_code, 'lat': lat, 'lon': lon, 'azi': int(getattr(r, 'azimuth', 0) or 0), 'tech': tech, 'info': {c: getattr(r, c) or '' for c in cols}})
            except: pass
    return render_page(CONTENT_TEMPLATE, title="Bản đồ GIS", active_page='gis', selected_tech=tech, site_code_input=site_code_input, cell_name_input=cell_name_input, gis_data=gis_data, its_data=its_data, show_its=show_its, action_type=action_type, rf_cols=cols)

@app.route('/kpi')
@login_required
def kpi():
    selected_tech = request.args.get('tech', '4g')
    cell_name_input = request.args.get('cell_name', '').strip()
    poi_input = request.args.get('poi_name', '').strip()
    charts, target_cells = {}, []

    KPI_Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)
    RF_Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(selected_tech)
    POI_Model = {'4g': POI4G, '5g': POI5G}.get(selected_tech)

    if poi_input and POI_Model: target_cells = [r.cell_code for r in POI_Model.query.filter(POI_Model.poi_name == poi_input).all()]
    elif cell_name_input:
        if RF_Model: target_cells.extend([r.cell_code for r in RF_Model.query.filter(or_(RF_Model.site_code.ilike(f"%{cell_name_input}%"), RF_Model.cell_code.ilike(f"%{cell_name_input}%"))).all()])
        if KPI_Model: target_cells.extend([r[0] for r in KPI_Model.query.filter(KPI_Model.ten_cell.ilike(f"%{cell_name_input}%")).with_entities(KPI_Model.ten_cell).distinct().all()])
        if not target_cells: target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]
            
    target_cells = list(set([str(c).strip() for c in target_cells if c]))

    if target_cells and KPI_Model:
        data = KPI_Model.query.filter(KPI_Model.ten_cell.in_(target_cells)).all()
        if data:
            all_labels = sorted(list(set([x.thoi_gian for x in data])), key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
            data_by_cell = defaultdict(list)
            for x in data: data_by_cell[str(x.ten_cell).strip().upper()].append(x)

            metrics = {'3g': [('pstraffic', 'PS TRAFFIC (GB)'), ('traffic', 'CS TRAFFIC (Erl)'), ('psconges', 'PS CONG (%)'), ('csconges', 'CS CONG (%)')], '4g': [('traffic', 'TRAFFIC (GB)'), ('user_dl_avg_thput', 'THPUT (Mbps)'), ('res_blk_dl', 'PRB (%)'), ('cqi_4g', 'CQI 4G'), ('service_drop_all', 'DROP (%)')], '5g': [('traffic', 'TRAFFIC (GB)'), ('user_dl_avg_throughput', 'THPUT (Mbps)'), ('cqi_5g', 'CQI 5G')]}.get(selected_tech, [])
            colors = generate_colors(max(len(target_cells), 10))
            for key, lbl in metrics:
                ds = []
                for i, ccode in enumerate(target_cells):
                    cmap = {item.thoi_gian: (getattr(item, key, 0) or 0) for item in data_by_cell.get(ccode.upper(), [])}
                    ds.append({'label': ccode, 'data': [cmap.get(l, None) for l in all_labels], 'borderColor': colors[i%len(colors)], 'fill': False, 'spanGaps': True})
                charts[f"chart_{key}"] = {'title': lbl, 'labels': all_labels, 'datasets': ds}

    poi_list = []
    try: poi_list = sorted(list(set([r[0] for r in db.session.query(POI4G.poi_name).distinct()] + [r[0] for r in db.session.query(POI5G.poi_name).distinct()])))
    except: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi', selected_tech=selected_tech, cell_name_input=cell_name_input, selected_poi=poi_input, poi_list=poi_list, charts=charts)

@app.route('/qoe-qos')
@login_required
def qoe_qos():
    cell_name_input = request.args.get('cell_name', '').strip()
    charts, qoe_details, qos_details, qoe_headers, qos_headers = {}, [], [], [], []
    has_data = False
    
    if cell_name_input:
        qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoE4G.id.asc()).all()
        qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoS4G.id.asc()).all()
        
        if qoe_records or qos_records:
            has_data = True
            week_map = {}
            for r in qoe_records + qos_records:
                if not r.week_name: continue
                m = re.search(r'tuan\s*(\d+)', remove_accents(str(r.week_name)).lower())
                if m:
                    wn = int(m.group(1))
                    if wn not in week_map or len(r.week_name) > len(week_map[wn]): week_map[wn] = r.week_name
            for r in qoe_records + qos_records:
                if r.week_name:
                    m = re.search(r'tuan\s*(\d+)', remove_accents(str(r.week_name)).lower())
                    if m: r.week_name = week_map[int(m.group(1))]

            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))
            if qoe_records:
                qoe_score_map = {r.week_name: r.qoe_score or 0 for r in qoe_records}
                qoe_percent_map = {r.week_name: r.qoe_percent or 0 for r in qoe_records}
                charts['qoe_score_chart'] = {'title': 'Điểm QoE', 'labels': all_weeks, 'datasets': [{'label': 'Điểm', 'data': [qoe_score_map.get(w, None) for w in all_weeks], 'borderColor': '#0078d4'}]}
                charts['qoe_percent_chart'] = {'title': 'Tỷ lệ QoE (%)', 'labels': all_weeks, 'datasets': [{'label': '% QoE', 'data': [qoe_percent_map.get(w, None) for w in all_weeks], 'borderColor': '#107c10'}]}
                seen_w = set()
                for r in reversed(qoe_records):
                    if r.week_name not in seen_w and r.details:
                        seen_w.add(r.week_name)
                        try:
                            d = json.loads(r.details)
                            if not qoe_headers: qoe_headers = list(d.keys())
                            qoe_details.append({'week': r.week_name, 'data': d})
                        except: pass
                qoe_details.reverse()
            if qos_records:
                qos_score_map = {r.week_name: r.qos_score or 0 for r in qos_records}
                qos_percent_map = {r.week_name: r.qos_percent or 0 for r in qos_records}
                charts['qos_score_chart'] = {'title': 'Điểm QoS', 'labels': all_weeks, 'datasets': [{'label': 'Điểm', 'data': [qos_score_map.get(w, None) for w in all_weeks], 'borderColor': '#ffaa44'}]}
                charts['qos_percent_chart'] = {'title': 'Tỷ lệ QoS (%)', 'labels': all_weeks, 'datasets': [{'label': '% QoS', 'data': [qos_percent_map.get(w, None) for w in all_weeks], 'borderColor': '#e3008c'}]}
                seen_w = set()
                for r in reversed(qos_records):
                    if r.week_name not in seen_w and r.details:
                        seen_w.add(r.week_name)
                        try:
                            d = json.loads(r.details)
                            if not qos_headers: qos_headers = list(d.keys())
                            qos_details.append({'week': r.week_name, 'data': d})
                        except: pass
                qos_details.reverse()
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="QoE & QoS Analytics", active_page='qoe_qos', cell_name_input=cell_name_input, charts=charts, has_data=has_data, qoe_details=qoe_details, qos_details=qos_details, qoe_headers=qoe_headers, qos_headers=qos_headers)

@app.route('/poi')
@login_required
def poi():
    pname = request.args.get('poi_name', '').strip()
    charts, pois = {}, []
    try: pois = sorted(list(set([r[0] for r in db.session.query(POI4G.poi_name).distinct()] + [r[0] for r in db.session.query(POI5G.poi_name).distinct()])))
    except: pass
    
    if pname:
        c4 = [r[0] for r in db.session.query(POI4G.cell_code).filter_by(poi_name=pname).all()]
        c5 = [r[0] for r in db.session.query(POI5G.cell_code).filter_by(poi_name=pname).all()]
        if c4:
            k4 = db.session.query(KPI4G.thoi_gian, KPI4G.traffic, KPI4G.user_dl_avg_thput).filter(KPI4G.ten_cell.in_(c4)).all()
            if k4:
                dates4 = sorted(list(set(x.thoi_gian for x in k4)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                agg_traf, agg_thput = defaultdict(float), defaultdict(list)
                for r in k4:
                    agg_traf[r.thoi_gian] += (r.traffic or 0)
                    if r.user_dl_avg_thput is not None: agg_thput[r.thoi_gian].append(r.user_dl_avg_thput)
                charts['4g_traf'] = {'title': 'Total 4G Traffic (GB)', 'labels': dates4, 'datasets': [{'label': 'Traffic', 'data': [agg_traf[d] for d in dates4], 'borderColor': 'blue'}]}
                charts['4g_thp'] = {'title': 'Avg 4G Thput (Mbps)', 'labels': dates4, 'datasets': [{'label': 'Thput', 'data': [(sum(agg_thput[d])/len(agg_thput[d])) if agg_thput[d] else 0 for d in dates4], 'borderColor': 'green'}]}
        if c5:
            k5 = db.session.query(KPI5G.thoi_gian, KPI5G.traffic, KPI5G.user_dl_avg_throughput).filter(KPI5G.ten_cell.in_(c5)).all()
            if k5:
                dates5 = sorted(list(set(x.thoi_gian for x in k5)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                agg_traf5, agg_thput5 = defaultdict(float), defaultdict(list)
                for r in k5:
                    agg_traf5[r.thoi_gian] += (r.traffic or 0)
                    if r.user_dl_avg_throughput is not None: agg_thput5[r.thoi_gian].append(r.user_dl_avg_throughput)
                charts['5g_traf'] = {'title': 'Total 5G Traffic (GB)', 'labels': dates5, 'datasets': [{'label': 'Traffic', 'data': [agg_traf5[d] for d in dates5], 'borderColor': 'orange'}]}
                charts['5g_thp'] = {'title': 'Avg 5G Thput (Mbps)', 'labels': dates5, 'datasets': [{'label': 'Thput', 'data': [(sum(agg_thput5[d])/len(agg_thput5[d])) if agg_thput5[d] else 0 for d in dates5], 'borderColor': 'purple'}]}
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="POI Report", active_page='poi', poi_list=pois, selected_poi=pname, poi_charts=charts)

@app.route('/worst-cell')
@login_required
def worst_cell():
    duration = int(request.args.get('duration', 1))
    action = request.args.get('action')
    dates = sorted([datetime.strptime(d[0], '%d/%m/%Y') for d in db.session.query(KPI4G.thoi_gian).distinct().all() if d[0]], reverse=True)
    target_dates = [d.strftime('%d/%m/%Y') for d in dates[:duration]]
    results = []
    
    if target_dates:
        l900_cells = {c[0] for c in db.session.query(RF4G.cell_code).filter(RF4G.frequency.ilike('%L900%')).all()}
        active_cells = {c[0] for c in db.session.query(KPI4G.ten_cell).filter(KPI4G.thoi_gian == target_dates[0]).all()}
        records = db.session.query(KPI4G.ten_cell, KPI4G.user_dl_avg_thput, KPI4G.res_blk_dl, KPI4G.cqi_4g, KPI4G.service_drop_all).filter(
            KPI4G.thoi_gian.in_(target_dates), ~KPI4G.ten_cell.startswith('MBF_TH'), ~KPI4G.ten_cell.startswith('VNP-4G'),
            ((KPI4G.user_dl_avg_thput < 7000) | (KPI4G.res_blk_dl > 20) | (KPI4G.cqi_4g < 93) | (KPI4G.service_drop_all > 0.3))
        ).all()
        groups = defaultdict(list)
        for r in records: 
            if r.ten_cell in active_cells and r.ten_cell not in l900_cells: groups[r.ten_cell].append(r)
        for cell, rows in groups.items():
            if len(rows) == duration:
                results.append({'cell_name': cell, 'avg_thput': round(sum(r.user_dl_avg_thput or 0 for r in rows)/duration, 2), 'avg_res_blk': round(sum(r.res_blk_dl or 0 for r in rows)/duration, 2), 'avg_cqi': round(sum(r.cqi_4g or 0 for r in rows)/duration, 2), 'avg_drop': round(sum(r.service_drop_all or 0 for r in rows)/duration, 2)})
    gc.collect()
    if action == 'export':
        df = pd.DataFrame(results)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Worst Cells')
        output.seek(0)
        return send_file(output, download_name=f'WorstCell_{duration}days.xlsx', as_attachment=True)
    return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=results, dates=target_dates, duration=duration)

@app.route('/traffic-down')
@login_required
def traffic_down():
    tech = request.args.get('tech', '4g')
    action = request.args.get('action')
    zero_traffic, degraded, degraded_pois, analysis_date = [], [], [], "N/A"
    
    if action in ['execute', 'export_zero', 'export_degraded', 'export_poi_degraded']:
        Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
        POI_Model = {'4g': POI4G, '5g': POI5G}.get(tech)
        if Model:
            dates = sorted([datetime.strptime(d[0], '%d/%m/%Y') for d in db.session.query(Model.thoi_gian).distinct().all() if d[0]], reverse=True)
            if dates:
                latest = dates[0]
                analysis_date = latest.strftime('%d/%m/%Y')
                needed_str = [d.strftime('%d/%m/%Y') for d in [latest] + [latest - timedelta(days=i) for i in range(1, 8)]]
                records = db.session.query(Model.ten_cell, Model.thoi_gian, Model.traffic).filter(Model.thoi_gian.in_(needed_str)).all()
                data_map = defaultdict(dict)
                for r in records:
                    if r.ten_cell.startswith('MBF') or r.ten_cell.startswith('VNP'): continue
                    try: data_map[r.ten_cell][datetime.strptime(r.thoi_gian, '%d/%m/%Y')] = r.traffic or 0
                    except: pass
                last_week = latest - timedelta(days=7)
                for cell, d_map in data_map.items():
                    t0 = d_map.get(latest, 0); t_last = d_map.get(last_week, 0)
                    if t0 < 0.1:
                        avg7 = sum(d_map.get(latest - timedelta(days=i), 0) for i in range(1,8)) / 7
                        if avg7 > 2: zero_traffic.append({'cell_name': cell, 'traffic_today': round(t0,3), 'avg_last_7': round(avg7,3)})
                    if t_last > 1 and t0 < 0.7 * t_last: degraded.append({'cell_name': cell, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})
                
                if POI_Model:
                    poi_map = {r.cell_code: r.poi_name for r in db.session.query(POI_Model).all()}
                    poi_traffic = defaultdict(lambda: {'today': 0, 'last_week': 0})
                    for cell, d_map in data_map.items():
                        if cell in poi_map:
                            p_name = poi_map[cell]
                            poi_traffic[p_name]['today'] += d_map.get(latest, 0)
                            poi_traffic[p_name]['last_week'] += d_map.get(last_week, 0)
                    for pname, traf in poi_traffic.items():
                        t0 = traf['today']; t_last = traf['last_week']
                        if t_last > 5 and t0 < 0.7 * t_last: degraded_pois.append({'poi_name': pname, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})
        gc.collect()
        if action == 'export_zero':
            df = pd.DataFrame(zero_traffic)
            output = BytesIO(); df.to_excel(output, engine='openpyxl', index=False); output.seek(0)
            return send_file(output, download_name=f'ZeroTraffic_{tech}.xlsx', as_attachment=True)
        elif action == 'export_degraded':
            df = pd.DataFrame(degraded)
            output = BytesIO(); df.to_excel(output, engine='openpyxl', index=False); output.seek(0)
            return send_file(output, download_name=f'DegradedTraffic_{tech}.xlsx', as_attachment=True)
        elif action == 'export_poi_degraded':
            df = pd.DataFrame(degraded_pois)
            output = BytesIO(); df.to_excel(output, engine='openpyxl', index=False); output.seek(0)
            return send_file(output, download_name=f'POIDegraded_{tech}.xlsx', as_attachment=True)
    return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', zero_traffic=zero_traffic, degraded=degraded, degraded_pois=degraded_pois, tech=tech, analysis_date=analysis_date)

@app.route('/conges-3g')
@login_required
def conges_3g():
    conges_data, target_dates = [], []
    action = request.args.get('action')
    if action in ['execute', 'export']:
        try:
            dates = sorted([datetime.strptime(d[0], '%d/%m/%Y') for d in db.session.query(KPI3G.thoi_gian).distinct().all() if d[0]], reverse=True)
            if len(dates) >= 3:
                target_dates = [d.strftime('%d/%m/%Y') for d in dates[:3]]
                records = db.session.query(KPI3G.ten_cell, KPI3G.traffic, KPI3G.csconges, KPI3G.pstraffic, KPI3G.psconges).filter(
                    KPI3G.thoi_gian.in_(target_dates), ((KPI3G.csconges > 2) & (KPI3G.cs_so_att > 100)) | ((KPI3G.psconges > 2) & (KPI3G.ps_so_att > 500))
                ).all()
                groups = defaultdict(list)
                for r in records: groups[r.ten_cell].append(r)
                for cell, rows in groups.items():
                    if len(rows) == 3:
                        conges_data.append({'cell_name': cell, 'avg_cs_traffic': round(sum(r.traffic or 0 for r in rows)/3, 2), 'avg_cs_conges': round(sum(r.csconges or 0 for r in rows)/3, 2), 'avg_ps_traffic': round(sum(r.pstraffic or 0 for r in rows)/3, 2), 'avg_ps_conges': round(sum(r.psconges or 0 for r in rows)/3, 2)})
        except: pass
    gc.collect()
    if action == 'export':
        df = pd.DataFrame(conges_data)
        output = BytesIO(); df.to_excel(output, engine='openpyxl', index=False, sheet_name='Congestion 3G'); output.seek(0)
        return send_file(output, download_name='Congestion3G.xlsx', as_attachment=True)
    return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=conges_data, dates=target_dates)

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '4g')
    search_query = request.args.get('cell_search', '').strip()
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    cols = RF_COLS_ORDER.get(tech, [c.key for c in Model.__table__.columns if c.key != 'id']) if Model else []
    
    if Model:
        query = Model.query
        if search_query: query = query.filter(or_(Model.cell_code.ilike(f"%{search_query}%"), Model.site_code.ilike(f"%{search_query}%")))
        data = [{c: getattr(r, c) for c in cols} | {'id': r.id} for r in query.limit(100).all()]
    else: data = []
    
    return render_page(CONTENT_TEMPLATE, title="RF Database", active_page='rf', current_tech=tech, rf_columns=cols, rf_data=data, search_query=search_query)

# ==============================================================================
# CÁC API KHÁC (USER, BACKUP, SCRIPT...)
# ==============================================================================
@app.route('/script', methods=['GET', 'POST'])
@login_required
def script():
    script_result = ""
    if request.method == 'POST':
        tech = request.form.get('tech')
        rns, srns, hsns, hpns, rcns = request.form.getlist('rn[]'), request.form.getlist('srn[]'), request.form.getlist('hsn[]'), request.form.getlist('hpn[]'), request.form.getlist('rcn[]')
        lines = []
        for i in range(len(rns)): lines.append(f"ADD RRUCHAIN: RCN={rcns[i]}, TT=CHAIN, BM=COLD, AT=LOCALPORT, HSRN=0, HSN={hsns[i]}, HPN={hpns[i]}, CR=AUTO, USERDEFRATENEGOSW=OFF;")
        script_result = "\n".join(lines)
    return render_page(CONTENT_TEMPLATE, title="Generate Script", active_page='script', script_result=script_result)

@app.route('/backup', methods=['POST'])
@login_required
def backup_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    selected_tables = request.form.getlist('tables')
    stream = BytesIO()
    models_map = {'users.csv': User, 'config_3g.csv': Config3G, 'cell_3g.csv': Cell3G, 'rf3g.csv': RF3G, 'rf4g.csv': RF4G, 'rf5g.csv': RF5G, 'poi4g.csv': POI4G, 'poi5g.csv': POI5G, 'kpi3g.csv': KPI3G, 'kpi4g.csv': KPI4G, 'kpi5g.csv': KPI5G, 'qoe_4g.csv': QoE4G, 'qos_4g.csv': QoS4G}
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname in selected_tables:
            if fname in models_map:
                Model = models_map[fname]
                cols = [c.key for c in Model.__table__.columns]
                data = db.session.query(Model).all()
                df = pd.DataFrame([{c: getattr(row, c) for c in cols} for row in data]) if data else pd.DataFrame(columns=cols)
                zf.writestr(fname, df.to_csv(index=False, encoding='utf-8-sig'))
    stream.seek(0); gc.collect()
    return send_file(stream, as_attachment=True, download_name=f'backup_{datetime.now().strftime("%Y%m%d")}.zip')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
