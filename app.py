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
# 2. CÁC HÀM TIỆN ÍCH (BỘ LỌC ĐỘC QUYỀN 3G & TỰ ĐỘNG UPPERCASE)
# ==============================================================================
def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1, s0 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ', u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
    return ''.join(s0[s1.index(c)] if c in s1 else c for c in input_str)

def clean_header(col_name, itype=None, raw_headers=None):
    c = str(col_name).strip().lower()
    raw_headers_str = " | ".join([str(x).lower().strip() for x in raw_headers]) if raw_headers else ""
    
    if itype == 'config3g':
        mapping_config = {'mã csht': 'csht_code', 'cell name (alias)': 'cell_name', 'mã cell': 'cell_code', 'mã trạm': 'site_code', 'latitude': 'latitude', 'longitude': 'longitude', 'longtitude': 'longitude', 'thiết bị': 'equipment', 'tên thiết bị': 'equipment', 'băng tần': 'frequency', 'dlpsc': 'psc', 'dl_psc': 'psc', 'dl_uarfcn': 'dl_uarfcn', 'lac': 'bsc_lac', 'bsc_lac': 'bsc_lac', 'ci': 'ci', 'antennahigh': 'anten_height', 'antenna high': 'anten_height', 'azimuth': 'azimuth', 'mechanicaltilt': 'm_t', 'mechanical tilt': 'm_t', 'electricaltilt': 'e_t', 'electrical tilt': 'e_t', 'totaltilt': 'total_tilt', 'total tilt': 'total_tilt', 'antennatype': 'antena', 'model ăn ten': 'antena'}
        return mapping_config.get(c, f"ignore_config3g_{re.sub(r'[^a-z0-9]', '_', remove_accents(c))}")
        
    if itype == 'cell3g':
        mapping_cell = {'tên trên hệ thống': 'cell_code', 'mã cell': 'cell_code', 'antenna tên hãng sx': 'hang_sx', 'hãng sx': 'hang_sx', 'antenna dùng chung': 'swap', 'swap': 'swap', 'ngày hoạt động': 'start_day', 'hoàn cảnh ra đời': 'ghi_chu', 'ghi chú': 'ghi_chu'}
        return mapping_cell.get(c, f"ignore_cell3g_{re.sub(r'[^a-z0-9]', '_', remove_accents(c))}")

    if 'mã node cha' in c: return 'site_code'
    if 'mã node' in c: return 'cell_code'
    if 'tên trên hệ thống' in c: return 'cell_name'
    if 'mã csht của trạm' in c or 'mã csht' in c: return 'csht_code'
    if 'mã csht của cell' in c: return 'csht_cell_ignore'
    if 'longtitude' in c or 'longitude' in c: return 'longitude'
    if 'latitude' in c: return 'latitude'
    if 'model ăn ten' in c or 'antenna model' in c or 'antennatype' in c: return 'antena'
    if 'total tilt' in c or 'totaltilt' in c: return 'total_tilt'
    if 'mechaincal tilt' in c or 'mechanical tilt' in c or 'mechainical tilt' in c or 'mechanicaltilt' in c: return 'm_t'
    if 'electrical tilt' in c or 'electricaltilt' in c: return 'e_t'
    if 'tên thiết bị' in c or 'thiết bị' in c: return 'equipment'
    if 'băng tần' in c: return 'frequency'
    if 'enodeb id' in c: return 'enodeb_id'
    if 'gnodeb id' in c: return 'gnodeb_id'
    if 'nrci' == c or 'lcrid' == c: return 'lcrid'
    if 'antenna high' in c or 'antennahigh' in c: return 'anten_height'
    if 'hãng sx' in c or 'tên hãng sx' in c: return 'hang_sx'
    if 'ngày hoạt động' in c: return 'start_day'
    if 'ghi chú' in c or 'hoàn cảnh ra đời' in c: return 'ghi_chu'
    if 'lac' == c or 'bsc_lac' in c: return 'bsc_lac'
    if 'dl_psc' in c or 'psc' == c or 'dlpsc' in c: return 'psc'
    if 'cell name' in c or 'tên cell' in c: return 'ten_cell'
    if 'thời gian' in c: return 'thoi_gian'
    if 'total data traffic volume' in c: return 'traffic'
    if 'user downlink average throughput' in c: return 'user_dl_avg_thput'
    if 'resource block untilizing rate downlink' in c: return 'res_blk_dl'
    if 'service drop (all service)' in c: return 'service_drop_all'
    if 'mimo' == c: return 'mimo'
    if 'nrarfcndl' in c: return 'nrarfcn'
    if 'pci' == c: return 'pci'
    if 'tac' == c: return 'tac'
    if 'ci' == c: return 'ci'
    if 'đồng bộ' in c: return 'dong_bo'
    if 'site name' in c: return 'site_name'
    if 'antenna dùng chung' in c: return 'swap'
    if 'dl_uarfcn' in c: return 'dl_uarfcn'
    
    clean = re.sub(r'[^a-z0-9]', '_', remove_accents(c))
    return re.sub(r'_+', '_', clean).strip('_')

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
    id = db.Column(db.Integer, primary_key=True); username = db.Column(db.String(50), unique=True, nullable=False); password_hash = db.Column(db.String(255), nullable=False); role = db.Column(db.String(20), default='user')
    def set_password(self, password): self.password_hash = generate_password_hash(password)
    def check_password(self, password): return check_password_hash(self.password_hash, password)

class Config3G(db.Model):
    __tablename__ = 'config_3g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(255), index=True); site_code = db.Column(db.String(255)); cell_name = db.Column(db.String(255)); csht_code = db.Column(db.String(255)); latitude = db.Column(db.Float); longitude = db.Column(db.Float); antena = db.Column(db.String(255)); azimuth = db.Column(db.Integer); total_tilt = db.Column(db.Float); equipment = db.Column(db.String(255)); frequency = db.Column(db.String(255)); psc = db.Column(db.String(255)); dl_uarfcn = db.Column(db.String(255)); bsc_lac = db.Column(db.String(255)); ci = db.Column(db.String(255)); anten_height = db.Column(db.Float); m_t = db.Column(db.Float); e_t = db.Column(db.Float)

class Cell3G(db.Model):
    __tablename__ = 'cell_3g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(255), index=True); hang_sx = db.Column(db.String(255)); swap = db.Column(db.String(255)); start_day = db.Column(db.String(255)); ghi_chu = db.Column(db.Text)

class RF3G(db.Model):
    __tablename__ = 'rf_3g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(255), index=True); site_code = db.Column(db.String(255)); cell_name = db.Column(db.String(255)); csht_code = db.Column(db.String(255)); latitude = db.Column(db.Float); longitude = db.Column(db.Float); antena = db.Column(db.String(255)); azimuth = db.Column(db.Integer); total_tilt = db.Column(db.Float); equipment = db.Column(db.String(255)); frequency = db.Column(db.String(255)); psc = db.Column(db.String(255)); dl_uarfcn = db.Column(db.String(255)); bsc_lac = db.Column(db.String(255)); ci = db.Column(db.String(255)); anten_height = db.Column(db.Float); m_t = db.Column(db.Float); e_t = db.Column(db.Float); hang_sx = db.Column(db.String(255)); swap = db.Column(db.String(255)); start_day = db.Column(db.String(255)); ghi_chu = db.Column(db.Text)

class RF4G(db.Model):
    __tablename__ = 'rf_4g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(255), index=True); site_code = db.Column(db.String(255)); cell_name = db.Column(db.String(255)); csht_code = db.Column(db.String(255)); latitude = db.Column(db.Float); longitude = db.Column(db.Float); antena = db.Column(db.String(255)); azimuth = db.Column(db.Integer); total_tilt = db.Column(db.Float); equipment = db.Column(db.String(255)); frequency = db.Column(db.String(255)); dl_uarfcn = db.Column(db.String(255)); pci = db.Column(db.String(255)); tac = db.Column(db.String(255)); enodeb_id = db.Column(db.String(255)); lcrid = db.Column(db.String(255)); anten_height = db.Column(db.Float); m_t = db.Column(db.Float); e_t = db.Column(db.Float); mimo = db.Column(db.String(255)); hang_sx = db.Column(db.String(255)); swap = db.Column(db.String(255)); start_day = db.Column(db.String(255)); ghi_chu = db.Column(db.Text)

class RF5G(db.Model):
    __tablename__ = 'rf_5g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(255), index=True); site_code = db.Column(db.String(255)); site_name = db.Column(db.String(255)); csht_code = db.Column(db.String(255)); latitude = db.Column(db.Float); longitude = db.Column(db.Float); antena = db.Column(db.String(255)); azimuth = db.Column(db.Integer); total_tilt = db.Column(db.Float); equipment = db.Column(db.String(255)); frequency = db.Column(db.String(255)); nrarfcn = db.Column(db.String(255)); pci = db.Column(db.String(255)); tac = db.Column(db.String(255)); gnodeb_id = db.Column(db.String(255)); lcrid = db.Column(db.String(255)); anten_height = db.Column(db.Float); m_t = db.Column(db.Float); e_t = db.Column(db.Float); mimo = db.Column(db.String(255)); hang_sx = db.Column(db.String(255)); dong_bo = db.Column(db.String(255)); start_day = db.Column(db.String(255)); ghi_chu = db.Column(db.Text)

class POI4G(db.Model):
    __tablename__ = 'poi_4g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(50)); site_code = db.Column(db.String(50)); poi_name = db.Column(db.String(200), index=True)

class POI5G(db.Model):
    __tablename__ = 'poi_5g'; id = db.Column(db.Integer, primary_key=True); cell_code = db.Column(db.String(50)); site_code = db.Column(db.String(50)); poi_name = db.Column(db.String(200), index=True)

class KPI3G(db.Model):
    __tablename__ = 'kpi_3g'; id = db.Column(db.Integer, primary_key=True); ten_cell = db.Column(db.String(100), index=True); thoi_gian = db.Column(db.String(50)); traffic = db.Column(db.Float); pstraffic = db.Column(db.Float); cssr = db.Column(db.Float); dcr = db.Column(db.Float); ps_cssr = db.Column(db.Float); ps_dcr = db.Column(db.Float); hsdpa_throughput = db.Column(db.Float); hsupa_throughput = db.Column(db.Float); cs_so_att = db.Column(db.Float); ps_so_att = db.Column(db.Float); csconges = db.Column(db.Float); psconges = db.Column(db.Float)

class KPI4G(db.Model):
    __tablename__ = 'kpi_4g'; id = db.Column(db.Integer, primary_key=True); ten_cell = db.Column(db.String(100), index=True); thoi_gian = db.Column(db.String(50)); traffic = db.Column(db.Float); traffic_vol_dl = db.Column(db.Float); traffic_vol_ul = db.Column(db.Float); cell_dl_avg_thputs = db.Column(db.Float); cell_ul_avg_thput = db.Column(db.Float); user_dl_avg_thput = db.Column(db.Float); user_ul_avg_thput = db.Column(db.Float); erab_ssrate_all = db.Column(db.Float); service_drop_all = db.Column(db.Float); unvailable = db.Column(db.Float); res_blk_dl = db.Column(db.Float); cqi_4g = db.Column(db.Float)

class KPI5G(db.Model):
    __tablename__ = 'kpi_5g'; id = db.Column(db.Integer, primary_key=True); ten_cell = db.Column(db.String(100), index=True); thoi_gian = db.Column(db.String(50)); traffic = db.Column(db.Float); dl_traffic_volume_gb = db.Column(db.Float); ul_traffic_volume_gb = db.Column(db.Float); cell_downlink_average_throughput = db.Column(db.Float); cell_uplink_average_throughput = db.Column(db.Float); user_dl_avg_throughput = db.Column(db.Float); cqi_5g = db.Column(db.Float); cell_avaibility_rate = db.Column(db.Float); sgnb_addition_success_rate = db.Column(db.Float); sgnb_abnormal_release_rate = db.Column(db.Float)

class QoE4G(db.Model):
    __tablename__ = 'qoe_4g'; id = db.Column(db.Integer, primary_key=True); cell_name = db.Column(db.String(100), index=True); week_name = db.Column(db.String(100)); qoe_score = db.Column(db.Float); qoe_percent = db.Column(db.Float); details = db.Column(db.Text)

class QoS4G(db.Model):
    __tablename__ = 'qos_4g'; id = db.Column(db.Integer, primary_key=True); cell_name = db.Column(db.String(100), index=True); week_name = db.Column(db.String(100)); qos_score = db.Column(db.Float); qos_percent = db.Column(db.Float); details = db.Column(db.Text)

@login_manager.user_loader
def load_user(user_id): return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin'); u.set_password('admin123'); db.session.add(u); db.session.commit()
init_database()

# ==============================================================================
# 4. GIAO DIỆN HTML/CSS (Đã phục hồi cấu trúc chuẩn chống lỗi JS)
# ==============================================================================
BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor System</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
    <link href='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.css' rel='stylesheet' />
    <script src='https://cdnjs.cloudflare.com/ajax/libs/leaflet.fullscreen/3.0.0/Control.FullScreen.min.js'></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { --acrylic-bg: rgba(255, 255, 255, 0.6); --acrylic-blur: blur(20px); --sidebar-bg: rgba(240, 240, 245, 0.85); --primary-color: #0078d4; --text-color: #212529; --shadow-soft: 0 4px 12px rgba(0, 0, 0, 0.05); --shadow-hover: 0 8px 16px rgba(0, 0, 0, 0.1); --border-radius: 12px; }
        body { background: linear-gradient(135deg, #f3f4f6 0%, #eef2f3 100%); font-family: 'Segoe UI', sans-serif; color: var(--text-color); overflow-x: hidden; }
        .sidebar { height: 100vh; width: 260px; position: fixed; top: 0; left: 0; background: var(--sidebar-bg); backdrop-filter: var(--acrylic-blur); border-right: 1px solid rgba(255,255,255,0.5); z-index: 1000; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); padding-top: 1rem; overflow-y: auto; padding-bottom: 60px; }
        .sidebar-header { padding: 1.5rem; color: var(--primary-color); font-weight: 600; font-size: 1.5rem; text-align: center; letter-spacing: 0.5px; }
        .sidebar-menu { padding: 0; list-style: none; margin: 1rem 0; }
        .sidebar-menu a { display: flex; align-items: center; padding: 14px 25px; color: #555; text-decoration: none; font-weight: 500; border-left: 4px solid transparent; transition: all 0.2s ease; margin: 4px 12px; border-radius: 8px; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background-color: rgba(255, 255, 255, 0.8); color: var(--primary-color); box-shadow: 0 2px 8px rgba(0,0,0,0.05); }
        .sidebar-menu a.active { border-left-color: var(--primary-color); background-color: rgba(255, 255, 255, 0.95); }
        .sidebar-menu i { margin-right: 15px; width: 24px; text-align: center; font-size: 1.1rem; }
        .main-content { margin-left: 260px; padding: 30px; min-height: 100vh; transition: all 0.3s ease; }
        .card { border: none; border-radius: var(--border-radius); background: rgba(255, 255, 255, 0.85); backdrop-filter: blur(10px); box-shadow: var(--shadow-soft); transition: box-shadow 0.3s ease; margin-bottom: 1.5rem; }
        .card:hover { box-shadow: var(--shadow-hover); }
        .card-header { background-color: rgba(255, 255, 255, 0.9); border-bottom: 1px solid rgba(0,0,0,0.05); padding: 1.25rem 1.5rem; font-weight: 600; color: #333; font-size: 1.1rem; }
        .card-body { padding: 1.5rem; }
        .btn-primary { background-color: var(--primary-color); border: none; box-shadow: 0 2px 6px rgba(0, 120, 212, 0.3); border-radius: 6px; padding: 0.5rem 1.25rem; font-weight: 500; transition: all 0.2s; }
        .btn-primary:hover { background-color: #0063b1; box-shadow: 0 4px 12px rgba(0, 120, 212, 0.4); transform: translateY(-1px); }
        .btn-warning { background-color: #ffaa44; border: none; box-shadow: 0 2px 6px rgba(255, 170, 68, 0.3); border-radius: 6px; padding: 0.5rem 1.25rem; font-weight: 500; color: #fff; transition: all 0.2s; }
        .btn-warning:hover { background-color: #e69532; color: #fff; box-shadow: 0 4px 12px rgba(255, 170, 68, 0.4); transform: translateY(-1px); }
        .table { background: transparent; }
        .table thead th { background-color: rgba(248, 249, 250, 0.8); border-bottom: 2px solid #e9ecef; color: #555; font-weight: 600; font-size: 0.9rem; text-transform: uppercase; }
        .table-hover tbody tr:hover { background-color: rgba(0, 120, 212, 0.05); }
        .chart-container canvas { cursor: zoom-in; }
        @media (max-width: 768px) { .sidebar { margin-left: -260px; } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; padding: 15px; } }
        .legend { line-height: 18px; color: #333; }
        .legend i { width: 14px; height: 14px; float: left; margin-right: 8px; opacity: 0.8; border: 1px solid #999; }
        .table-responsive::-webkit-scrollbar { height: 8px; width: 8px; }
        .table-responsive::-webkit-scrollbar-track { background: #f1f1f1; border-radius: 4px; }
        .table-responsive::-webkit-scrollbar-thumb { background: #c1c1c1; border-radius: 4px; }
        .table-responsive::-webkit-scrollbar-thumb:hover { background: #a8a8a8; }
        #sidebar-overlay { display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.4); z-index: 999; backdrop-filter: blur(2px); transition: all 0.3s ease; }
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
            <li><a href="/qoe-qos" class="{{ 'active' if active_page == 'qoe_qos' else '' }}"><i class="fa-solid fa-star-half-stroke"></i> QoE QoS Analytics</a></li>
            <li><a href="/optimize" class="{{ 'active' if active_page == 'optimize' else '' }}"><i class="fa-solid fa-wand-magic-sparkles"></i> Tối ưu QoE/QoS</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-pin"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li>
                <a href="#toolsMenu" data-bs-toggle="collapse" class="{{ 'active' if active_page in ['azimuth', 'script'] else '' }}">
                    <i class="fa-solid fa-toolbox"></i> Tools
                    <i class="fa-solid fa-chevron-down ms-auto" style="width: auto; font-size: 0.8rem;"></i>
                </a>
                <ul class="collapse list-unstyled {{ 'show' if active_page in ['azimuth', 'script'] else '' }}" id="toolsMenu">
                    <li><a href="/azimuth" class="{{ 'active' if active_page == 'azimuth' else '' }}" style="margin-left: 20px; font-size: 0.95rem;"><i class="fa-solid fa-compass"></i> Azimuth</a></li>
                    <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}" style="margin-left: 20px; font-size: 0.95rem;"><i class="fa-solid fa-code"></i> Script</a></li>
                </ul>
            </li>
            {% if current_user.role == 'admin' %}
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-cloud-arrow-up"></i> Data Import</a></li>
            <li class="mt-4 mb-2 text-muted px-4 text-uppercase" style="font-size: 0.75rem; letter-spacing: 1px;">System</li>
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> User Mgmt</a></li>
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
            {% endif %}
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Profile</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Logout</a></li>
        </ul>
    </div>

    <div class="main-content">
        <button class="btn btn-light shadow-sm d-md-none mb-3 border fw-bold" onclick="toggleSidebar()">
            <i class="fa-solid fa-bars me-1"></i> Menu
        </button>

        <div class="container-fluid p-0">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }} alert-dismissible fade show shadow-sm border-0 mb-4" role="alert">
                            {{ message }}
                            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                        </div>
                    {% endfor %}
                {% endif %}
            {% endwith %}

            <!-- Dynamic Content -->
            {% block content %}{% endblock %}
        </div>
    </div>
    
    <div class="modal fade" id="chartDetailModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-xl modal-dialog-centered">
            <div class="modal-content border-0 shadow-lg" style="background: rgba(255,255,255,0.95); backdrop-filter: blur(15px);">
                <div class="modal-header border-0 pb-0">
                    <h5 class="modal-title text-primary fw-bold" id="modalTitle"></h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>
                <div class="modal-body p-4">
                    <div class="chart-container" style="position: relative; height:65vh; width:100%">
                        <canvas id="modalChart"></canvas>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        function toggleSidebar() {
            document.getElementById('sidebar').classList.toggle('active');
            document.getElementById('sidebar-overlay').classList.toggle('active');
        }

        let modalChartInstance = null;
        function showDetailModal(cellName, date, value, metricLabel, allDatasets, allLabels) {
            document.getElementById('modalTitle').innerText = 'Chi tiết ' + metricLabel;
            const ctx = document.getElementById('modalChart').getContext('2d');
            if (modalChartInstance) modalChartInstance.destroy();
            modalChartInstance = new Chart(ctx, {
                type: 'line',
                data: { labels: allLabels, datasets: allDatasets },
                options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'nearest', intersect: false }, plugins: { legend: { display: true } }, spanGaps: true, elements: { line: { tension: 0.3 } } }
            });
            new bootstrap.Modal(document.getElementById('chartDetailModal')).show();
        }
        
        function toggleCheckboxes(source) {
            let checkboxes = document.getElementsByName('tables');
            for(var i=0; i<checkboxes.length; i++) checkboxes[i].checked = source.checked;
        }
        
        function addRow(tech) {
            var table = document.getElementById("rruTable_" + tech).getElementsByTagName('tbody')[0];
            var newRow = table.insertRow(table.rows.length);
            var idx = table.rows.length;
            var defaultSRN, defaultSlot;
            if (tech == '3g900') { defaultSRN = 70 + idx - 1; defaultSlot = 2; }
            else if (tech == '3g2100') { defaultSRN = 80 + idx - 1; defaultSlot = 3; }
            else { defaultSRN = 60 + idx - 1; defaultSlot = 3; }

            newRow.insertCell(0).innerHTML = `<input type="text" name="rn[]" class="form-control" value="RRU${idx}">`;
            newRow.insertCell(1).innerHTML = `<input type="number" name="srn[]" class="form-control" value="${defaultSRN}">`;
            newRow.insertCell(2).innerHTML = `<input type="number" name="hsn[]" class="form-control" value="${defaultSlot}">`;
            newRow.insertCell(3).innerHTML = `<input type="number" name="hpn[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(4).innerHTML = `<input type="number" name="rcn[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(5).innerHTML = `<input type="number" name="sectorid[]" class="form-control" value="${idx-1}">`;
            newRow.insertCell(6).innerHTML = `<input type="number" name="rxnum[]" class="form-control" value="${tech=='4g'?4:2}">`;
            newRow.insertCell(7).innerHTML = `<input type="number" name="txnum[]" class="form-control" value="${tech=='4g'?4:1}">`;
            newRow.insertCell(8).innerHTML = `<button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button>`;
        }
    </script>
</body>
</html>
"""

LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Đăng nhập | NetOps</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: linear-gradient(135deg, #f0f2f5 0%, #d9e2ec 100%); height: 100vh; display: flex; align-items: center; justify-content: center; font-family: 'Segoe UI', sans-serif; }
        .login-card { width: 100%; max-width: 400px; background: rgba(255, 255, 255, 0.9); backdrop-filter: blur(20px); padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.08); border: 1px solid rgba(255,255,255,0.5); }
        .btn-primary { background-color: #0078d4; border: none; padding: 10px; font-weight: 600; border-radius: 8px; transition: all 0.3s; width: 100%; }
        .btn-primary:hover { background-color: #0063b1; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0, 120, 212, 0.3); }
        .form-control { border-radius: 8px; padding: 12px; margin-bottom: 1rem; border: 1px solid #e0e0e0; }
        .form-control:focus { box-shadow: 0 0 0 3px rgba(0, 120, 212, 0.15); border-color: #0078d4; }
    </style>
</head>
<body>
    <div class="login-card">
        <h3 class="text-center mb-4 text-primary fw-bold">Welcome Back</h3>
        <p class="text-center text-muted mb-4">Sign in to access KPI Monitor</p>
        <form method="POST">
            <label class="form-label fw-bold text-secondary small">USERNAME</label>
            <input type="text" name="username" class="form-control" placeholder="Enter username" required>
            <label class="form-label fw-bold text-secondary small">PASSWORD</label>
            <input type="password" name="password" class="form-control" placeholder="Enter password" required>
            <button type="submit" class="btn btn-primary">Sign In</button>
        </form>
    </div>
</body>
</html>
"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm mb-4">
    <div class="card-header d-flex justify-content-between align-items-center bg-white border-bottom">
        <span class="fs-5 fw-bold text-secondary">{{ title }}</span>
        <span class="badge bg-soft-primary text-primary px-3 py-2 rounded-pill border">{{ current_user.role | upper }}</span>
    </div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <h5 class="fw-bold text-secondary mb-4"><i class="fa-solid fa-chart-pie me-2"></i>Tổng quan Chất lượng Mạng 4G Toàn Hệ thống</h5>
            {% if dashboard_data and dashboard_data.labels %}
                <div class="row g-4">
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-primary mb-3">Tổng Traffic 4G (GB)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartTraffic"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-success mb-3">Trung bình User DL Thput (Mbps)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartThput"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-warning mb-3">Trung bình Tài nguyên PRB DL (%)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartPrb"></canvas></div></div></div></div>
                    <div class="col-md-6"><div class="card border border-light shadow-sm h-100"><div class="card-body p-3"><h6 class="fw-bold text-info mb-3">Trung bình Chất lượng Vô tuyến (CQI 4G)</h6><div class="chart-container" style="position: relative; height:30vh; width:100%"><canvas id="chartCqi"></canvas></div></div></div></div>
                </div>
                <script>
                    document.addEventListener('DOMContentLoaded', function() {
                        const labels = {{ dashboard_data.labels | tojson }};
                        function createDashChart(id, label, color, bgColor, dataArr, titleStr) {
                            const ds = [{ label: label, data: dataArr, borderColor: color, backgroundColor: bgColor, fill: true, tension: 0.3, borderWidth: 2 }];
                            new Chart(document.getElementById(id).getContext('2d'), { type: 'line', data: { labels: labels, datasets: ds }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, interaction: { mode: 'nearest', axis: 'x', intersect: false }, onClick: (e, el) => { if (el.length > 0) showDetailModal(ds[0].label, labels[el[0].index], ds[0].data[el[0].index], titleStr, ds, labels); } } });
                        }
                        createDashChart('chartTraffic', 'Traffic (GB)', '#0078d4', 'rgba(0,120,212,0.1)', {{ dashboard_data.traffic | tojson }}, 'Tổng Traffic 4G');
                        createDashChart('chartThput', 'Avg Thput (Mbps)', '#107c10', 'rgba(16,124,16,0.1)', {{ dashboard_data.thput | tojson }}, 'Trung bình Thput');
                        createDashChart('chartPrb', 'Avg PRB (%)', '#ffaa44', 'rgba(255,170,68,0.1)', {{ dashboard_data.prb | tojson }}, 'Trung bình PRB');
                        createDashChart('chartCqi', 'Avg CQI', '#00bcf2', 'rgba(0,188,242,0.1)', {{ dashboard_data.cqi | tojson }}, 'Trung bình CQI');
                    });
                </script>
            {% else %}
                <div class="alert alert-info border-0 shadow-sm"><i class="fa-solid fa-circle-info me-2"></i>Chưa có dữ liệu KPI 4G để hiển thị biểu đồ.</div>
            {% endif %}

        {% elif active_page == 'azimuth' %}
            <div class="card border border-light shadow-sm">
                <div class="card-body p-1">
                    <div id="azimuthMap" style="height: 75vh; width: 100%; border-radius: 8px; z-index: 1;"></div>
                </div>
            </div>

            <!-- Panel công cụ (Sẽ được gắn vào trong Bản đồ như một Control Nổi) -->
            <div id="azimuthFormContainer" class="shadow-lg" style="background: rgba(255, 255, 255, 0.95); padding: 15px; border-radius: 8px; width: 320px; max-width: 90vw; max-height: 65vh; overflow-y: auto; border: 1px solid #dee2e6;">
                <h6 class="fw-bold text-primary mb-2"><i class="fa-solid fa-compass me-2"></i>Tọa độ Điểm O (Gốc)</h6>
                <div class="mb-2 text-muted" style="font-size: 0.75rem;"><i class="fa-solid fa-info-circle me-1"></i><i>Mẹo: Click lên Bản đồ để chọn nhanh Điểm O</i></div>
                <div class="mb-2">
                    <label class="form-label small fw-bold mb-1">Vĩ độ (Latitude)</label>
                    <input type="text" id="latO" class="form-control form-control-sm" placeholder="VD: 21.028511" required>
                </div>
                <div class="mb-3">
                    <label class="form-label small fw-bold mb-1">Kinh độ (Longitude)</label>
                    <input type="text" id="lngO" class="form-control form-control-sm" placeholder="VD: 105.804817" required>
                </div>
                <button type="button" class="btn btn-outline-secondary btn-sm w-100 mb-3 fw-bold" onclick="getGPS()"><i class="fa-solid fa-location-crosshairs me-1"></i>Lấy GPS của tôi</button>

                <hr class="my-3">

                <h6 class="fw-bold text-success mb-2"><i class="fa-solid fa-pencil me-2"></i>Thêm Điểm Kết Nối</h6>
                <form id="azimuthForm">
                    <div class="mb-2">
                        <label class="form-label small fw-bold mb-1">Tên điểm tới</label>
                        <input type="text" id="ptName" class="form-control form-control-sm" placeholder="VD: Trạm A" required>
                    </div>
                    <div class="mb-2">
                        <label class="form-label small fw-bold mb-1">Góc Azimuth (Độ)</label>
                        <input type="number" id="ptAzimuth" class="form-control form-control-sm" min="0" max="360" step="any" placeholder="0 - 360" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label small fw-bold mb-1">Khoảng cách (Mét)</label>
                        <input type="number" id="ptDistance" class="form-control form-control-sm" min="0" step="any" placeholder="Nhập số mét..." required>
                    </div>
                    <button type="submit" class="btn btn-primary btn-sm w-100 shadow-sm fw-bold mb-2"><i class="fa-solid fa-plus me-1"></i>Vẽ đường nối</button>
                    <button type="button" class="btn btn-danger btn-sm w-100 shadow-sm fw-bold" onclick="clearDrawnPoints()"><i class="fa-solid fa-trash-can me-1"></i>Xóa các đường đã vẽ</button>
                </form>
            </div>

            <script>
                var azMap, markerO;
                var drawnItems = L.layerGroup();
                var drawnPointsData = []; 

                document.addEventListener('DOMContentLoaded', function() {
                    azMap = L.map('azimuthMap', {
                        center: [16.0, 106.0], 
                        zoom: 5,
                        zoomControl: true,
                        fullscreenControl: true,
                        fullscreenControlOptions: { position: 'topleft' }
                    });

                    var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    }).addTo(azMap);
                    
                    var googleHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    });

                    L.control.layers({"Bản đồ (Google)": googleStreets, "Vệ tinh (Google)": googleHybrid}, null, {position: 'topright'}).addTo(azMap);
                    
                    drawnItems.addTo(azMap);

                    var formControl = L.control({position: 'topright'});
                    formControl.onAdd = function (map) {
                        var wrapper = L.DomUtil.create('div', 'leaflet-control');
                        var toggleBtn = L.DomUtil.create('button', 'btn btn-primary btn-sm shadow-lg mb-2 w-100 fw-bold', wrapper);
                        toggleBtn.innerHTML = '<i class="fa-solid fa-sliders me-1"></i>Công Cụ Vẽ';
                        toggleBtn.style.border = '2px solid white';
                        toggleBtn.style.borderRadius = '8px';

                        var formDiv = document.getElementById('azimuthFormContainer');
                        wrapper.appendChild(formDiv);
                        
                        if (window.innerWidth <= 768) {
                            formDiv.style.display = 'none';
                        }

                        L.DomEvent.disableClickPropagation(wrapper);
                        L.DomEvent.disableScrollPropagation(wrapper);

                        L.DomEvent.on(toggleBtn, 'click', function(e) {
                            L.DomEvent.preventDefault();
                            L.DomEvent.stopPropagation();
                            if (formDiv.style.display === 'none') {
                                formDiv.style.display = 'block';
                            } else {
                                formDiv.style.display = 'none';
                            }
                        });

                        return wrapper;
                    };
                    formControl.addTo(azMap);

                    azMap.on('click', function(e) {
                        if (!markerO) { 
                            document.getElementById('latO').value = e.latlng.lat.toFixed(6);
                            document.getElementById('lngO').value = e.latlng.lng.toFixed(6);
                            drawOrigin();
                        }
                    });

                    document.getElementById('azimuthForm').addEventListener('submit', function(e) {
                        e.preventDefault();
                        drawAzimuth();
                    });
                });

                function flyToOrigin(lat, lng) {
                    azMap.flyTo([lat, lng], 17, { animate: true, duration: 1.5 });
                }

                function getGPS() {
                    if (navigator.geolocation) {
                        navigator.geolocation.getCurrentPosition(function(position) {
                            document.getElementById('latO').value = position.coords.latitude.toFixed(6);
                            document.getElementById('lngO').value = position.coords.longitude.toFixed(6);
                            flyToOrigin(position.coords.latitude, position.coords.longitude);
                            drawOrigin();
                        }, function(error) {
                            alert("Lỗi không lấy được GPS: " + error.message);
                        });
                    } else {
                        alert("Trình duyệt của bạn không hỗ trợ Geolocation.");
                    }
                }

                function calculateDistanceAndBearing(lat1, lon1, lat2, lon2) {
                    const R = 6371e3;
                    const f1 = lat1 * Math.PI/180;
                    const f2 = lat2 * Math.PI/180;
                    const df = (lat2-lat1) * Math.PI/180;
                    const dl = (lon2-lon1) * Math.PI/180;

                    const a = Math.sin(df/2) * Math.sin(df/2) + Math.cos(f1) * Math.cos(f2) * Math.sin(dl/2) * Math.sin(dl/2);
                    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
                    const dist = R * c;

                    const y = Math.sin(dl) * Math.cos(f2);
                    const x = Math.cos(f1)*Math.sin(f2) - Math.sin(f1)*Math.cos(f2)*Math.cos(dl);
                    let brng = Math.atan2(y, x) * 180 / Math.PI;
                    brng = (brng + 360) % 360;

                    return { distance: dist, bearing: brng };
                }

                function updateAllLines() {
                    var latO = parseFloat(document.getElementById('latO').value);
                    var lngO = parseFloat(document.getElementById('lngO').value);
                    if(isNaN(latO) || isNaN(lngO)) return;

                    drawnPointsData.forEach(function(obj) {
                        var posB = obj.marker.getLatLng();
                        var calc = calculateDistanceAndBearing(latO, lngO, posB.lat, posB.lng);
                        
                        obj.line.setLatLngs([[latO, lngO], posB]);
                        var popupContent = "<div class='text-center'><b>" + obj.name + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + calc.bearing.toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + calc.distance.toFixed(2) + " m</b></div>";
                        if(obj.marker.getPopup()) obj.marker.getPopup().setContent(popupContent);
                    });
                }

                function drawOrigin() {
                    var latO = document.getElementById('latO').value;
                    var lngO = document.getElementById('lngO').value;
                    if (!latO || !lngO) return;
                    
                    if (markerO) { azMap.removeLayer(markerO); }
                    
                    var iconO = L.divIcon({className: 'custom-div-icon', html: "<div style='background-color:#c0392b;width:18px;height:18px;border-radius:50%;border:3px solid white;box-shadow:0 0 8px rgba(0,0,0,0.8);'></div>", iconSize: [18, 18], iconAnchor: [9, 9]});
                    
                    markerO = L.marker([latO, lngO], {icon: iconO, draggable: true})
                        .bindTooltip("<b class='text-danger'>Điểm O</b>", {permanent: true, direction: 'left', className: 'bg-white border-danger rounded shadow-sm px-1 py-0'})
                        .addTo(azMap);
                        
                    markerO.on('drag', function(e) {
                        var newPos = e.target.getLatLng();
                        document.getElementById('latO').value = newPos.lat.toFixed(6);
                        document.getElementById('lngO').value = newPos.lng.toFixed(6);
                        updateAllLines();
                    });

                    updateAllLines();
                }

                document.getElementById('latO').addEventListener('change', function() {
                    drawOrigin();
                    var lat = document.getElementById('latO').value;
                    var lng = document.getElementById('lngO').value;
                    if(lat && lng) flyToOrigin(lat, lng);
                });
                
                document.getElementById('lngO').addEventListener('change', function() {
                    drawOrigin();
                    var lat = document.getElementById('latO').value;
                    var lng = document.getElementById('lngO').value;
                    if(lat && lng) flyToOrigin(lat, lng);
                });

                function calculateDestinationPoint(lat1, lon1, brng, dist) {
                    const R = 6371e3;
                    const d = parseFloat(dist);
                    const brngRad = parseFloat(brng) * Math.PI / 180;
                    const lat1Rad = parseFloat(lat1) * Math.PI / 180;
                    const lon1Rad = parseFloat(lon1) * Math.PI / 180;

                    const lat2Rad = Math.asin(Math.sin(lat1Rad) * Math.cos(d/R) + Math.cos(lat1Rad) * Math.sin(d/R) * Math.cos(brngRad));
                    const lon2Rad = lon1Rad + Math.atan2(Math.sin(brngRad) * Math.sin(d/R) * Math.cos(lat1Rad), Math.cos(d/R) - Math.sin(lat1Rad) * Math.sin(lat2Rad));

                    return [lat2Rad * 180 / Math.PI, lon2Rad * 180 / Math.PI];
                }

                function drawAzimuth() {
                    var latO = document.getElementById('latO').value;
                    var lngO = document.getElementById('lngO').value;
                    var ptName = document.getElementById('ptName').value;
                    var az = document.getElementById('ptAzimuth').value;
                    var dist = document.getElementById('ptDistance').value;

                    if (!latO || !lngO || !ptName || !az || !dist) {
                        alert("Vui lòng nhập đủ Điểm O, Tên điểm, Góc và Khoảng cách!");
                        return;
                    }

                    drawOrigin();
                    var pointB = calculateDestinationPoint(latO, lngO, az, dist);

                    var iconB = L.divIcon({className: 'custom-div-icon', html: "<div style='background-color:#2980b9;width:14px;height:14px;border-radius:50%;border:2px solid white;box-shadow:0 0 6px rgba(0,0,0,0.6);'></div>", iconSize: [14, 14], iconAnchor: [7, 7]});
                    
                    var popupContent = "<div class='text-center'><b>" + ptName + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + parseFloat(az).toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + parseFloat(dist).toFixed(2) + " m</b></div>";
                    
                    var markerB = L.marker(pointB, {icon: iconB, draggable: true})
                        .bindTooltip("<b>" + ptName + "</b>", {permanent: true, direction: 'right', className: 'text-primary border-primary rounded shadow-sm px-1 py-0'})
                        .bindPopup(popupContent, {autoPan: false})
                        .addTo(drawnItems);

                    markerB.on('dragstart', function(e) {
                        this.openPopup();
                    });

                    var polyline = L.polyline([[latO, lngO], pointB], {
                        color: '#000000',
                        weight: 4,
                        opacity: 1.0
                    }).addTo(drawnItems);
                    
                    var drawnObj = { marker: markerB, line: polyline, name: ptName };
                    drawnPointsData.push(drawnObj);
                    
                    markerB.on('drag', function(e) {
                        var newPos = e.target.getLatLng();
                        var curLatO = parseFloat(document.getElementById('latO').value);
                        var curLngO = parseFloat(document.getElementById('lngO').value);
                        
                        var calc = calculateDistanceAndBearing(curLatO, curLngO, newPos.lat, newPos.lng);
                        drawnObj.line.setLatLngs([[curLatO, curLngO], newPos]);
                        
                        var newPopup = "<div class='text-center'><b>" + ptName + "</b><hr class='my-1'>Góc Azimuth: <b class='text-danger'>" + calc.bearing.toFixed(2) + "°</b><br>Khoảng cách: <b class='text-primary'>" + calc.distance.toFixed(2) + " m</b></div>";
                        drawnObj.marker.getPopup().setContent(newPopup);
                    });

                    var group = new L.featureGroup([markerO, drawnItems]);
                    azMap.fitBounds(group.getBounds(), {padding: [50, 50]});
                    
                    document.getElementById('ptName').value = '';
                    document.getElementById('ptAzimuth').value = '';
                    document.getElementById('ptDistance').value = '';
                    document.getElementById('ptName').focus();
                }
                
                function clearDrawnPoints() {
                    drawnItems.clearLayers();
                    drawnPointsData = [];
                    if(markerO) azMap.setView(markerO.getLatLng(), 15);
                }
            </script>
            
        {% elif active_page == 'optimize' %}
            <div class="alert alert-info border-0 shadow-sm mb-4">
                <h5 class="fw-bold text-primary mb-3"><i class="fa-solid fa-book-open-reader me-2"></i>Quy trình Tối ưu 5 Bước</h5>
                <ol class="mb-0 text-dark">
                    <li class="mb-1"><strong>Thu thập Vĩ mô:</strong> Tự động lọc Top Cell tệ từ báo cáo QoE/QoS Tuần.</li>
                    <li class="mb-1"><strong>Chẩn đoán Vi mô:</strong> Tự động ghép nối KPI Ngày để tìm nguyên nhân gốc rễ (Nghẽn, Nhiễu, Lỗi Phần cứng).</li>
                    <li class="mb-1"><strong>Giải pháp Tối ưu:</strong> Đề xuất hành động cho RNO, NOC, UCTT xử lý.</li>
                    <li class="mb-1"><strong>Giám sát Tức thời:</strong> Xem nhanh biểu đồ KPI sau khi thực hiện tác động.</li>
                    <li><strong>Đóng vòng Tối ưu:</strong> Theo dõi sự cải thiện điểm QoE/QoS ở tuần tiếp theo.</li>
                </ol>
            </div>
            
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/optimize" class="row g-3 align-items-center bg-white p-3 rounded-3 border shadow-sm">
                        <div class="col-md-8">
                            <label class="form-label fw-bold small text-muted">CHỌN TUẦN PHÂN TÍCH (BƯỚC 1)</label>
                            <select name="week_name" class="form-select border-0 shadow-sm bg-light">
                                {% for w in all_weeks %}
                                <option value="{{ w }}" {% if w == latest_week %}selected{% endif %}>{{ w }}</option>
                                {% endfor %}
                            </select>
                        </div>
                        <div class="col-md-4 align-self-end d-flex gap-2">
                            <button type="submit" name="action" value="filter" class="btn btn-danger w-100 shadow-sm"><i class="fa-solid fa-filter me-1"></i>Lọc</button>
                            <button type="submit" name="action" value="export" class="btn btn-success w-100 shadow-sm"><i class="fa-solid fa-file-excel me-1"></i>Export</button>
                        </div>
                    </form>
                </div>
            </div>
            
            <div class="d-flex justify-content-between align-items-center mb-3 mt-4">
                <h6 class="fw-bold text-danger mb-0"><i class="fa-solid fa-list-check me-2"></i>Danh sách Trạm Cần Xử lý ({{ latest_week or 'Chưa có dữ liệu' }})</h6>
            </div>
            
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;">
                 <table class="table table-hover table-bordered mb-0 align-middle" style="font-size: 0.85rem;">
                     <thead class="table-light text-center position-sticky top-0" style="z-index: 10;">
                         <tr>
                             <th rowspan="2" class="align-middle">Cell Name</th>
                             <th colspan="2">1. Báo cáo Tuần (Macro)</th>
                             <th colspan="4">2. KPI Ngày Gần Nhất (Micro)</th>
                             <th rowspan="2" class="align-middle">3. Chẩn đoán (Bệnh)</th>
                             <th rowspan="2" class="align-middle">4. Giải pháp (Action)</th>
                             <th rowspan="2" class="align-middle">5. Giám sát</th>
                         </tr>
                         <tr>
                             <th>Điểm QoE</th><th>Điểm QoS</th>
                             <th>PRB (%)</th><th>Thput (Mbps)</th><th>CQI (%)</th><th>Drop (%)</th>
                         </tr>
                     </thead>
                     <tbody>
                         {% for row in optimized_data %}
                         <tr>
                             <td class="fw-bold text-primary text-nowrap">{{ row.cell_name }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.qoe_score != '-' and row.qoe_score <= 2 }}">{{ row.qoe_score }}{% if row.qoe_percent != '-' %}<br><small class="text-muted">({{ row.qoe_percent }}%)</small>{% endif %}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.qos_score != '-' and row.qos_score <= 3 }}">{{ row.qos_score }}{% if row.qos_percent != '-' %}<br><small class="text-muted">({{ row.qos_percent }}%)</small>{% endif %}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.prb != '-' and row.prb > 20 }}">{{ row.prb }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.thput != '-' and row.thput < 10 }}">{{ row.thput }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.cqi != '-' and row.cqi < 93 }}">{{ row.cqi }}</td>
                             <td class="text-center {{ 'text-danger fw-bold' if row.drop != '-' and row.drop > 0.3 }}">{{ row.drop }}</td>
                             <td>
                                 <ul class="mb-0 ps-3 text-danger fw-bold" style="min-width: 150px;">
                                     {% for issue in row.issues %}<li>{{ issue }}</li>{% endfor %}
                                 </ul>
                             </td>
                             <td>
                                 <ul class="mb-0 ps-3 text-success" style="min-width: 180px;">
                                     {% for action in row.actions %}<li>{{ action }}</li>{% endfor %}
                                 </ul>
                             </td>
                             <td class="text-center p-2" style="min-width: 100px;">
                                 <a href="/kpi?tech=4g&cell_name={{ row.cell_name }}" class="btn btn-sm btn-outline-primary mb-1 w-100 py-0 shadow-sm" style="font-size: 0.75rem;"><i class="fa-solid fa-chart-line me-1"></i>Xem KPI</a>
                                 <a href="/qoe-qos?cell_name={{ row.cell_name }}" class="btn btn-sm btn-outline-warning w-100 py-0 text-dark shadow-sm" style="font-size: 0.75rem;"><i class="fa-solid fa-star-half-stroke me-1"></i>Xem QoE</a>
                             </td>
                         </tr>
                         {% else %}
                         <tr><td colspan="10" class="text-center py-5 text-muted"><i class="fa-solid fa-face-smile fa-3x mb-3 text-success d-block"></i>Tuyệt vời! Không phát hiện Cell nào vi phạm ngưỡng tệ trong tuần gần nhất.</td></tr>
                         {% endfor %}
                     </tbody>
                 </table>
            </div>
        
        {% elif active_page == 'gis' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="POST" action="/gis" enctype="multipart/form-data" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option></select></div>
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">SITE CODE</label><input type="text" name="site_code" class="form-control border-0 shadow-sm" placeholder="VD: THA001" value="{{ site_code_input }}"></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted">CELL NAME</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="VD: THA001_1" value="{{ cell_name_input }}"></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted text-warning"><i class="fa-solid fa-file-lines me-1"></i>LOG ITS (.TXT/.CSV)</label><input type="file" name="its_file" class="form-control border-0 shadow-sm" accept=".txt,.csv" multiple></div>
                        <div class="col-md-2 d-flex flex-column gap-2 mt-4 pt-1"><button type="submit" name="action" value="search" class="btn btn-primary btn-sm w-100 shadow-sm fw-bold"><i class="fa-solid fa-search me-1"></i>Tìm kiếm</button><button type="submit" name="action" value="show_log" class="btn btn-warning btn-sm w-100 shadow-sm fw-bold text-white"><i class="fa-solid fa-route me-1"></i>Xem Log</button></div>
                    </form>
                </div>
            </div>
            <div class="card border border-light shadow-sm">
                <div class="card-body p-1 position-relative">
                    <div id="gisMap" style="height: 65vh; width: 100%; border-radius: 8px; z-index: 1;"></div>
                </div>
            </div>
            <script>
                document.addEventListener('DOMContentLoaded', function() {
                    var gisData = {{ gis_data | tojson | safe if gis_data else '[]' }};
                    var itsData = {{ its_data | tojson | safe if its_data else '[]' }};
                    var rfCols = {{ rf_cols | tojson | safe if rf_cols else '[]' }};
                    var actionType = "{{ action_type }}";
                    var searchSite = "{{ site_code_input }}";
                    var searchCell = "{{ cell_name_input }}";
                    var isShowIts = {{ 'true' if show_its else 'false' }};
                    var hasGisData = gisData.length > 0;
                    var hasItsData = isShowIts && itsData.length > 0;
                    
                    if(!document.getElementById('gisMap')) return;

                    var mapCenter = [19.807, 105.776];
                    var mapZoom = 9;

                    var map = L.map('gisMap', {
                        center: mapCenter,
                        zoom: mapZoom,
                        zoomControl: true,
                        fullscreenControl: true,
                        fullscreenControlOptions: { position: 'topleft' }
                    });

                    var gpsControlGis = L.control({position: 'topleft'});
                    gpsControlGis.onAdd = function(m) {
                        var container = L.DomUtil.create('div', 'leaflet-bar leaflet-control');
                        var btn = L.DomUtil.create('a', '', container);
                        btn.href = '#';
                        btn.title = 'Vị trí của tôi';
                        btn.style.display = 'flex';
                        btn.style.alignItems = 'center';
                        btn.style.justifyContent = 'center';
                        btn.style.width = '30px';
                        btn.style.height = '30px';
                        btn.style.backgroundColor = '#fff';
                        btn.style.color = '#333';
                        btn.style.textDecoration = 'none';
                        btn.innerHTML = '<i class="fa-solid fa-location-crosshairs"></i>';
                        
                        var userMarker = null;

                        L.DomEvent.disableClickPropagation(btn);
                        L.DomEvent.on(btn, 'click', function(e) {
                            L.DomEvent.preventDefault();
                            if (navigator.geolocation) {
                                btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
                                navigator.geolocation.getCurrentPosition(function(position) {
                                    var lat = position.coords.latitude;
                                    var lng = position.coords.longitude;
                                    m.flyTo([lat, lng], 16, { animate: true, duration: 1.5 });
                                    if (userMarker) m.removeLayer(userMarker);
                                    var iconGPS = L.divIcon({
                                        className: 'custom-gps-icon', 
                                        html: "<div style='background-color:#4285F4;width:16px;height:16px;border-radius:50%;border:2px solid white;box-shadow:0 0 8px rgba(0,0,0,0.6);'></div>", 
                                        iconSize: [16, 16], 
                                        iconAnchor: [8, 8]
                                    });
                                    userMarker = L.marker([lat, lng], {icon: iconGPS}).bindTooltip("<b>Vị trí của bạn</b>", {permanent: false, direction: 'top', className: 'text-primary shadow-sm border-0'}).addTo(m);
                                    btn.innerHTML = '<i class="fa-solid fa-location-crosshairs"></i>';
                                }, function(error) {
                                    alert("Lỗi không lấy được GPS: " + error.message);
                                    btn.innerHTML = '<i class="fa-solid fa-location-crosshairs"></i>';
                                });
                            } else {
                                alert("Trình duyệt của bạn không hỗ trợ định vị.");
                            }
                        });
                        return container;
                    };
                    gpsControlGis.addTo(map);

                    var googleStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    }).addTo(map);
                    
                    var googleHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
                        maxZoom: 22,
                        subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
                        detectRetina: true,
                        attribution: '© Google Maps'
                    });

                    L.control.layers({"Bản đồ (Google)": googleStreets, "Vệ tinh (Google)": googleHybrid}).addTo(map);

                    if (hasGisData || hasItsData) {
                        var settingsControl = L.control({position: 'topright'});
                        settingsControl.onAdd = function (map) {
                            var div = L.DomUtil.create('div', 'info settings-control shadow-sm');
                            div.style.backgroundColor = 'rgba(255, 255, 255, 0.95)';
                            div.style.padding = '12px';
                            div.style.borderRadius = '8px';
                            div.style.border = '1px solid #dee2e6';
                            L.DomEvent.disableClickPropagation(div);

                            var html = '<div class="d-flex flex-column gap-2">';
                            if (hasGisData) {
                                html += '<div class="d-flex align-items-center justify-content-between">';
                                html += '<label class="small fw-bold text-muted mb-0 me-2" style="white-space: nowrap;"><i class="fa-solid fa-wifi text-info me-1"></i>Bán kính quạt:</label>';
                                html += '<input type="range" class="form-range" id="sectorRadiusSlider" min="50" max="2000" step="50" value="350" style="width: 100px;">';
                                html += '<span id="sectorRadiusVal" class="small text-muted ms-2 fw-bold" style="min-width: 45px; text-align: right;">350m</span>';
                                html += '</div>';
                            }
                            if (hasItsData) {
                                html += '<div class="d-flex align-items-center justify-content-between">';
                                html += '<label class="small fw-bold text-muted mb-0 me-2" style="white-space: nowrap;"><i class="fa-solid fa-circle text-primary me-1"></i>Cỡ hạt Log:</label>';
                                html += '<input type="range" class="form-range" id="pointSizeSlider" min="1" max="10" value="3" style="width: 100px;">';
                                html += '</div>';
                                html += '<div class="d-flex align-items-center mt-1 pt-2 border-top">';
                                html += '<div class="form-check form-switch mb-0 w-100 d-flex align-items-center ps-0">';
                                html += '<label class="form-check-label small fw-bold text-muted me-3" for="showLinesToggle" style="cursor: pointer;"><i class="fa-solid fa-link text-success me-1"></i>Nối vệt tia</label>';
                                html += '<input class="form-check-input mt-0 ms-auto float-none" type="checkbox" id="showLinesToggle" checked style="cursor: pointer;">';
                                html += '</div></div>';
                            }
                            html += '</div>';
                            div.innerHTML = html;
                            return div;
                        };
                        settingsControl.addTo(map);
                    }

                    var bounds = [];
                    if (isShowIts && itsData.length > 0) {
                        itsData.forEach(function(pt) { bounds.push([pt.lat, pt.lon]); });
                    }
                    if (actionType === 'show_log' && gisData.length > 0) {
                        gisData.forEach(function(cell) { bounds.push([cell.lat, cell.lon]); });
                    }

                    if (actionType === 'search' && (searchSite || searchCell) && gisData.length > 0) {
                        var targetCell = gisData[0];
                        for (var i = 0; i < gisData.length; i++) {
                            var sCode = (gisData[i].site_code || "").toLowerCase();
                            var cName = (gisData[i].cell_name || "").toLowerCase();
                            var sInput = searchSite.toLowerCase();
                            var cInput = searchCell.toLowerCase();
                            if ((sInput && sCode.includes(sInput)) || (cInput && cName.includes(cInput))) { targetCell = gisData[i]; break; }
                        }
                        if (targetCell.lat && targetCell.lon) { map.setView([targetCell.lat, targetCell.lon], 15); } 
                        else { map.setView(mapCenter, mapZoom); }
                    } else if (bounds.length > 0) {
                        map.fitBounds(bounds, {padding: [30, 30], maxZoom: 16});
                    } else {
                        map.setView(mapCenter, mapZoom);
                    }

                    var siteLayerGroup = L.layerGroup().addTo(map);
                    var sectorLayerGroup = L.layerGroup().addTo(map);
                    var itsLayerGroup = L.layerGroup().addTo(map);
                    var cellLookup = {};
                    var renderedSites = {};
                    var techColors = {'3g': '#0078d4', '4g': '#107c10', '5g': '#ffaa44'};

                    function getSectorMidPoint(lat, lon, azimuth, distanceMeters) {
                        var latFactor = 111320;
                        var lonFactor = 111320 * Math.cos(lat * Math.PI / 180);
                        var radMap = (azimuth || 0) * Math.PI / 180;
                        var dx = (distanceMeters * Math.sin(radMap)) / lonFactor;
                        var dy = (distanceMeters * Math.cos(radMap)) / latFactor;
                        return [lat + dy, lon + dx];
                    }

                    function getSectorPolygon(lat, lon, azimuth, beamwidth, radiusMeters) {
                        var center = [lat, lon];
                        var points = [center];
                        var startAngle = azimuth - beamwidth / 2;
                        var endAngle = azimuth + beamwidth / 2;
                        var latFactor = 111320;
                        var lonFactor = 111320 * Math.cos(lat * Math.PI / 180);
                        for (var i = startAngle; i <= endAngle; i += 5) {
                            var radMap = i * Math.PI / 180;
                            var dx = (radiusMeters * Math.sin(radMap)) / lonFactor; 
                            var dy = (radiusMeters * Math.cos(radMap)) / latFactor; 
                            points.push([lat + dy, lon + dx]);
                        }
                        points.push(center);
                        return points;
                    }

                    gisData.forEach(function(cell) {
                        if(!cell.lat || !cell.lon) return;
                        if (!renderedSites[cell.site_code]) {
                            L.circleMarker([cell.lat, cell.lon], {radius: 5, color: '#333', weight: 1.5, fillColor: '#ffffff', fillOpacity: 1}).bindPopup("<b>Site Code:</b> " + cell.site_code).addTo(siteLayerGroup);
                            renderedSites[cell.site_code] = true;
                        }
                    });

                    function cVal(v) {
                        if (v === null || v === undefined) return "";
                        var s = String(v).trim();
                        if (s.endsWith(".0")) s = s.slice(0, -2);
                        return s;
                    }

                    function buildLookupAndDrawSectors() {
                        sectorLayerGroup.clearLayers();
                        cellLookup = {};
                        var radiusSlider = document.getElementById('sectorRadiusSlider');
                        var sectorRadius = radiusSlider ? parseInt(radiusSlider.value) : 350;
                        var valDisplay = document.getElementById('sectorRadiusVal');
                        if (valDisplay) valDisplay.innerText = sectorRadius + 'm';

                        var targetCellCoord = null;
                        var targetPolygon = null;

                        gisData.forEach(function(cell) {
                            if(!cell.lat || !cell.lon) return;
                            var isMatch = false;
                            if (actionType === 'search' && (searchSite || searchCell)) {
                                var sCode = (cell.site_code || "").toLowerCase();
                                var cName = (cell.cell_name || "").toLowerCase();
                                var sInput = searchSite.toLowerCase();
                                var cInput = searchCell.toLowerCase();
                                if ((sInput && sCode.includes(sInput)) || (cInput && cName.includes(cInput))) { 
                                    isMatch = true;
                                    if (!targetCellCoord) targetCellCoord = [cell.lat, cell.lon];
                                }
                            }

                            var tech = cell.tech;
                            var info = cell.info;
                            var key = "";

                            if (tech === '4g') {
                                var en = cVal(info.enodeb_id); var lc = cVal(info.lcrid);
                                if (en && lc) key = en + "_" + lc;
                            } else if (tech === '3g') {
                                var ci = cVal(info.ci);
                                if (ci) key = ci;
                            } else if (tech === '5g') {
                                 var gn = cVal(info.gnodeb_id); var lc5 = cVal(info.lcrid);
                                 if (gn && lc5) key = gn + "_" + lc5;
                            }

                            if (key) {
                                cellLookup[key] = getSectorMidPoint(cell.lat, cell.lon, cell.azi, sectorRadius * 0.65);
                            }

                            var color = techColors[cell.tech] || '#dc3545';
                            var polyPoints = getSectorPolygon(cell.lat, cell.lon, cell.azi, 60, sectorRadius);
                            var polygon = L.polygon(polyPoints, {
                                color: isMatch ? '#ff0000' : color, 
                                weight: isMatch ? 3 : 1, 
                                fillColor: isMatch ? '#ff0000' : color, 
                                fillOpacity: isMatch ? 0.7 : 0.35
                            }).addTo(sectorLayerGroup);

                            if (isMatch && !targetPolygon) targetPolygon = polygon;

                            var infoHtml = "<div style='max-height: 250px; overflow-y: auto; overflow-x: hidden;'><table class='table table-sm table-bordered mb-0' style='font-size: 0.8rem;'>";
                            var keysToLoop = (rfCols && rfCols.length > 0) ? rfCols : Object.keys(cell.info);
                            keysToLoop.forEach(function(k) {
                                var v = cell.info[k];
                                if (v !== undefined && v !== null && v !== '' && v !== 'None') {
                                    infoHtml += "<tr><th class='text-muted bg-light w-50'>" + k.toUpperCase() + "</th><td class='fw-bold'>" + v + "</td></tr>";
                                }
                            });
                            infoHtml += "</table></div>";

                            polygon.bindPopup(
                                "<div class='mb-2 pb-2 border-bottom'><b>Cell:</b> <span class='text-primary fs-6'>" + cell.cell_name + "</span><br>" +
                                "<b>Site:</b> " + cell.site_code + "<br>" +
                                "<b>Tọa độ:</b> " + cell.lat + ", " + cell.lon + "</div>" + infoHtml,
                                { minWidth: 300, maxWidth: 450 }
                            );
                        });
                        
                        if (actionType === 'search' && targetCellCoord) {
                            map.setView(targetCellCoord, 16);
                            if (targetPolygon) setTimeout(() => targetPolygon.openPopup(), 600);
                        }

                        drawITSData();
                    }

                    function getSignalColor(tech, level) {
                        var t = (tech || '').toUpperCase();
                        if (t.includes('4G') || t.includes('LTE')) {
                            if (level >= -75) return '#0000FF';
                            if (level >= -85) return '#00FF00';
                            if (level >= -95) return '#FFFF00';
                            if (level >= -105) return '#FFA500';
                            if (level >= -115) return '#FF0000';
                            return '#000000';
                        } else { 
                            if (level >= -65) return '#0000FF';
                            if (level >= -75) return '#00FF00';
                            if (level >= -85) return '#FFFF00';
                            if (level >= -95) return '#FFA500';
                            if (level >= -105) return '#FF0000';
                            return '#000000';
                        }
                    }

                    function drawITSData() {
                        if (!isShowIts || itsData.length === 0) return;
                        itsLayerGroup.clearLayers();

                        var pointSizeSlider = document.getElementById('pointSizeSlider');
                        var pointSize = pointSizeSlider ? parseInt(pointSizeSlider.value) : 3;
                        var showLinesToggle = document.getElementById('showLinesToggle');
                        var showLines = showLinesToggle ? showLinesToggle.checked : false;

                        itsData.forEach(function(pt) {
                            var ptColor = getSignalColor(pt.tech, pt.level);
                            var ptCoord = [pt.lat, pt.lon];
                            
                            L.circleMarker(ptCoord, {radius: pointSize, fillColor: ptColor, color: "#000", weight: 0.5, fillOpacity: 0.9})
                            .bindPopup("<div class='small'><b>Tech:</b> " + pt.tech + "<br><b>Level:</b> <span class='fw-bold' style='color:"+ptColor+"'>" + pt.level + " dBm</span><br><b>Qual:</b> " + (pt.qual||'-') + "<br><b>Node/CellID:</b> " + (pt.node||'-') + " / " + pt.cellid + "</div>")
                            .addTo(itsLayerGroup);

                            if (showLines) {
                                var ptTech = (pt.tech || '').toLowerCase();
                                var key = "";
                                if (ptTech.includes('4g') || ptTech.includes('lte')) {
                                    if (pt.node && pt.cellid) key = pt.node + "_" + pt.cellid;
                                } else if (ptTech.includes('3g') || ptTech.includes('wcdma') || ptTech.includes('hspa') || ptTech.includes('umts')) {
                                    if (pt.cellid) key = pt.cellid;
                                } else if (ptTech.includes('5g') || ptTech.includes('nr')) {
                                    if (pt.node && pt.cellid) key = pt.node + "_" + pt.cellid;
                                }

                                var targetCellCoord = cellLookup[key];
                                if (targetCellCoord) {
                                    L.polyline([ptCoord, targetCellCoord], {color: ptColor, weight: 1.5, opacity: 0.6, dashArray: '3, 4'}).addTo(itsLayerGroup);
                                }
                            }
                        });
                    }

                    buildLookupAndDrawSectors();

                    var radSliderEl = document.getElementById('sectorRadiusSlider');
                    if (radSliderEl) radSliderEl.addEventListener('input', buildLookupAndDrawSectors);

                    var sliderEl = document.getElementById('pointSizeSlider');
                    if (sliderEl) sliderEl.addEventListener('input', drawITSData);

                    var toggleEl = document.getElementById('showLinesToggle');
                    if (toggleEl) toggleEl.addEventListener('change', drawITSData);

                    if (isShowIts && itsData.length > 0) {
                        var legend = L.control({position: 'bottomright'});
                        legend.onAdd = function (map) {
                            var div = L.DomUtil.create('div', 'info legend shadow-sm');
                            div.style.backgroundColor = 'rgba(255, 255, 255, 0.95)';
                            div.style.padding = '10px 15px';
                            div.style.borderRadius = '8px';
                            div.style.fontSize = '0.85rem';
                            div.style.lineHeight = '1.8';
                            div.style.border = '1px solid #dee2e6';
                            div.style.minWidth = '160px';
                            
                            L.DomEvent.disableClickPropagation(div);
                            
                            var currentTech = "{{ selected_tech }}";
                            
                            var html = '<div id="legend-header" class="d-flex justify-content-between align-items-center" style="cursor:pointer; margin-bottom: 2px;">';
                            html += '<strong class="text-dark fs-6 mb-0"><i class="fa-solid fa-list-ul me-1"></i> Chú giải</strong>';
                            html += '<i class="fa-solid fa-chevron-down text-muted ms-3" id="legend-toggle-icon"></i>';
                            html += '</div>';
                            
                            html += '<div id="legend-body" style="display: block; margin-top: 8px;">';

                            if (currentTech === '3g') {
                                html += '<strong class="text-success d-block mb-1"><i class="fa-solid fa-signal me-1"></i> 3G RSCP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -65)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-75 đến -65)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-95 đến -85)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -105)</div>';
                            } else {
                                html += '<strong class="text-primary d-block mb-1"><i class="fa-solid fa-signal me-1"></i> 4G/5G RSRP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -75)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-95 đến -85)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-115 đến -105)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -115)</div>';
                            }
                            
                            html += '</div>';
                            div.innerHTML = html;
                            return div;
                        };
                        legend.addTo(map);

                        setTimeout(function() {
                            var header = document.getElementById('legend-header');
                            var body = document.getElementById('legend-body');
                            var icon = document.getElementById('legend-toggle-icon');
                            if (header && body && icon) {
                                header.addEventListener('click', function(e) {
                                    e.stopPropagation();
                                    if (body.style.display === 'none') {
                                        body.style.display = 'block';
                                        icon.classList.remove('fa-chevron-up');
                                        icon.classList.add('fa-chevron-down');
                                    } else {
                                        body.style.display = 'none';
                                        icon.classList.remove('fa-chevron-down');
                                        icon.classList.add('fa-chevron-up');
                                    }
                                });
                            }
                        }, 100);
                    }
                });
            </script>
        
        {% elif active_page == 'kpi' %}
            <form method="GET" action="/kpi" class="row g-3 mb-4 bg-light p-3"><div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select"><option value="3g" {% if selected_tech=='3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech=='4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech=='5g' %}selected{% endif %}>5G</option></select></div><div class="col-md-4"><label class="form-label fw-bold small text-muted">TÌM THEO POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div><div class="col-md-3"><label class="form-label fw-bold small text-muted">NHẬP CELL/SITE</label><input type="text" name="cell_name" class="form-control" placeholder="Site code, Cell list..." value="{{ cell_name_input }}"></div><div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100">Visualize</button></div></form>
            {% if charts %}
                {% for chart_id, chart_config in charts.items() %}
                <div class="card mb-4 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ chart_config.title }}</h6><div class="chart-container" style="position: relative; height:45vh; width:100%"><canvas id="{{ chart_id }}"></canvas></div></div></div>
                {% endfor %}
                <script>{% for chart_id, chart_data in charts.items() %}(function(){const ctx=document.getElementById('{{ chart_id }}').getContext('2d'); const cd={{ chart_data | tojson }}; new Chart(ctx,{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0){const i=el[0].index;const di=el[0].datasetIndex;showDetailModal(cd.datasets[di].label,cd.labels[i],cd.datasets[di].data[i],'{{ chart_data.title }}',cd.datasets,cd.labels);}},plugins:{legend:{position:'bottom'},tooltip:{mode:'index',intersect:false}}}});})();{% endfor %}</script>
            {% elif cell_name_input or selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm"><i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu phù hợp.</div>
            {% else %}
                <div class="text-center text-muted py-5 opacity-50"><i class="fa-solid fa-chart-line fa-4x mb-3"></i><p class="fs-5">Vui lòng chọn tiêu chí để xem báo cáo KPI.</p></div>
            {% endif %}
        
        {% elif active_page == 'qoe_qos' %}
            <form method="GET" action="/qoe-qos" class="row g-3 mb-4 bg-light p-3"><div class="col-md-8"><label class="form-label fw-bold small text-muted">NHẬP CELL NAME 4G</label><input type="text" name="cell_name" class="form-control" placeholder="Cell Name 4G..." value="{{ cell_name_input }}" required></div><div class="col-md-4 align-self-end d-flex gap-2"><button type="submit" class="btn btn-primary w-100">Xem</button>{% if has_data %}<a href="/kpi?tech=4g&cell_name={{ cell_name_input }}" class="btn btn-success w-100">Link tới KPI</a>{% endif %}</div></form>
            {% if charts %}
                <div class="row">
                    {% for chart_id, c in charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ c.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ chart_id }}"></canvas></div></div></div></div>
                    <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                    {% endfor %}
                </div>
                {% if qoe_details %}
                <div class="card mt-2 shadow-sm border-0 mb-4"><div class="card-header bg-white fw-bold text-primary">Dữ liệu gốc QoE Hàng tuần</div><div class="card-body p-0 table-responsive"><table class="table table-bordered table-striped table-hover mb-0 text-nowrap" style="font-size: 0.8rem;"><thead class="table-light"><tr><th>Tuần</th>{% for k in qoe_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qoe_details %}<tr><td class="fw-bold text-primary">{{ row.week }}</td>{% for k in qoe_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
                {% if qos_details %}
                <div class="card shadow-sm border-0 mb-4"><div class="card-header bg-white fw-bold text-success">Dữ liệu gốc QoS Hàng tuần</div><div class="card-body p-0 table-responsive"><table class="table table-bordered table-striped table-hover mb-0 text-nowrap" style="font-size: 0.8rem;"><thead class="table-light"><tr><th>Tuần</th>{% for k in qos_headers %}<th>{{ k }}</th>{% endfor %}</tr></thead><tbody>{% for row in qos_details %}<tr><td class="fw-bold text-success">{{ row.week }}</td>{% for k in qos_headers %}<td>{{ row.data.get(k, '-') }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div></div>
                {% endif %}
            {% elif cell_name_input %}
                <div class="alert alert-warning border-0 shadow-sm"><i class="fa-solid fa-circle-exclamation me-2"></i>Không tìm thấy dữ liệu QoE/QoS cho Cell: <strong>{{ cell_name_input }}</strong>.</div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-star-half-stroke fa-3x mb-3 opacity-50"></i><p>Nhập mã Cell 4G để xem biểu đồ xu hướng QoE, QoS.</p></div>
            {% endif %}
        
        {% elif active_page == 'poi' %}
            <form method="GET" action="/poi" class="row g-3 mb-4 bg-light p-3"><div class="col-md-8"><label class="form-label fw-bold small text-muted">CHỌN POI</label><input type="text" name="poi_name" list="poi_list" class="form-control" placeholder="Tên POI..." value="{{ selected_poi }}"><datalist id="poi_list">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div><div class="col-md-4 align-self-end"><button type="submit" class="btn btn-primary w-100">Xem Báo Cáo</button></div></form>
            {% if poi_charts %}
            <div class="row">{% for cid, cd in poi_charts.items() %}<div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ cd.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ cid }}"></canvas></div></div></div></div><script>(function(){ const cd={{ cd | tojson }}; new Chart(document.getElementById('{{ cid }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ cd.title }}',cd.datasets,cd.labels)}}});})();</script>{% endfor %}</div>
            {% elif selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm">Không có dữ liệu cho POI: <strong>{{ selected_poi }}</strong></div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-map-location-dot fa-3x mb-3"></i><p>Chọn địa điểm POI để xem báo cáo.</p></div>
            {% endif %}
        
        {% elif active_page == 'worst_cell' %}
            <form method="GET" action="/worst-cell" class="row g-3 mb-4 bg-light p-3"><div class="col-auto"><label class="col-form-label fw-bold text-muted">THỜI GIAN</label></div><div class="col-auto"><select name="duration" class="form-select"><option value="1">1 Ngày</option><option value="3">3 Ngày</option><option value="7">7 Ngày</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-danger">Lọc</button><button type="submit" name="action" value="export" class="btn btn-success ms-2">Export</button></div></form>
            {% if dates %}<div class="alert alert-info border-0 shadow-sm mb-4"><i class="fa-solid fa-calendar-days me-2"></i><strong>Xét duyệt:</strong> {% for d in dates %}<span class="badge bg-white text-info border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 70vh;"><table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light position-sticky top-0" style="z-index: 10;"><tr><th>Cell Name</th><th>Avg Thput</th><th>Avg PRB</th><th>Avg CQI</th><th>Avg Drop Rate</th><th>Hành động</th></tr></thead><tbody>{% for r in worst_cells %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_thput < 7000 }}">{{ r.avg_thput }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_res_blk > 20 }}">{{ r.avg_res_blk }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_cqi < 93 }}">{{ r.avg_cqi }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_drop > 0.3 }}">{{ r.avg_drop }}</td><td class="text-center"><a href="/kpi?tech=4g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted">Nhấn Lọc để xem dữ liệu</td></tr>{% endfor %}</tbody></table></div>
        
        {% elif active_page == 'traffic_down' %}
            <form method="GET" action="/traffic-down" class="row g-3 mb-4 bg-light p-3"><div class="col-auto"><label class="col-form-label fw-bold text-muted">CÔNG NGHỆ:</label></div><div class="col-auto"><select name="tech" class="form-select"><option value="3g" {% if tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if tech == '4g' %}selected{% endif %}>4G</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-primary">Thực hiện</button><button type="submit" name="action" value="export_zero" class="btn btn-success ms-2">Zero</button><button type="submit" name="action" value="export_degraded" class="btn btn-warning ms-2">Degraded</button></div><div class="col-auto ms-auto"><span class="badge bg-info text-dark">Ngày phân tích: {{ analysis_date }}</span></div></form>
            <div class="row g-4"><div class="col-md-6"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-danger text-white fw-bold">Zero Traffic</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Avg 7D</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in zero_traffic %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.avg_last_7 }}</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div><div class="col-md-6"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">Degraded</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Last Wk</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div></div>
        
        {% elif active_page == 'conges_3g' %}
            <form method="GET" action="/conges-3g" class="d-flex align-items-center mb-4"><div class="alert alert-info border-0 shadow-sm bg-soft-primary text-primary mb-0 flex-grow-1"><strong>Điều kiện:</strong> (CS_CONG > 2% & CS_ATT > 100) OR (PS_CONG > 2% & PS_ATT > 500) (3 ngày liên tiếp)</div><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm ms-3">Thực hiện</button><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2">Export</button></form>
            {% if dates %}<div class="mb-3 text-muted small">Xét duyệt: {% for d in dates %}<span class="badge bg-light text-dark border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border"><table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light"><tr><th>Cell Name</th><th>Avg CS Traffic</th><th>Avg CS Conges (%)</th><th>Avg PS Traffic</th><th>Avg PS Conges (%)</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in conges_data %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td>{{ r.avg_cs_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_cs_conges > 2 }}">{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_ps_conges > 2 }}">{{ r.avg_ps_conges }}</td><td class="text-center"><a href="/kpi?tech=3g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white shadow-sm">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted opacity-50">Nhấn nút "Thực hiện" để xem kết quả</td></tr>{% endfor %}</tbody></table></div>
        
        {% elif active_page == 'rf' %}
            <div class="d-flex flex-wrap justify-content-between align-items-center mb-4 bg-white p-3 rounded shadow-sm border gap-3"><div class="btn-group shadow-sm"><a href="/rf?tech=3g" class="btn {{ 'btn-primary' if current_tech=='3g' else 'btn-outline-primary' }}">3G</a><a href="/rf?tech=4g" class="btn {{ 'btn-primary' if current_tech=='4g' else 'btn-outline-primary' }}">4G</a><a href="/rf?tech=5g" class="btn {{ 'btn-primary' if current_tech=='5g' else 'btn-outline-primary' }}">5G</a></div><form method="GET" action="/rf" class="d-flex flex-grow-1 mx-lg-4"><input type="hidden" name="tech" value="{{ current_tech }}"><div class="input-group shadow-sm"><span class="input-group-text bg-light border-end-0"><i class="fa-solid fa-search text-muted"></i></span><input type="text" name="cell_search" class="form-control border-start-0 ps-0" placeholder="Nhập Cell Code hoặc Site Code để tìm nhanh..." value="{{ search_query }}"><button type="submit" class="btn btn-primary px-4 fw-bold">Tìm kiếm</button></div></form><div class="d-flex gap-2"><form method="GET" action="/rf" class="m-0"><input type="hidden" name="tech" value="{{ current_tech }}"><input type="hidden" name="cell_search" value="{{ search_query }}"><button type="submit" name="action" value="export" class="btn btn-success shadow-sm text-white fw-bold"><i class="fa-solid fa-file-excel me-2"></i>Export</button></form>{% if current_user.role == 'admin' %}<a href="/rf/add?tech={{ current_tech }}" class="btn btn-warning shadow-sm fw-bold"><i class="fa-solid fa-plus me-1"></i>New</a>{% endif %}</div></div>
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;"><table class="table table-hover mb-0" style="font-size: 0.85rem; white-space: nowrap;"><thead class="table-light position-sticky top-0" style="z-index: 10;"><tr><th class="text-center border-bottom bg-light" style="position: sticky; left: 0; z-index: 20;">Action</th>{% for col in rf_columns %}<th>{{ col | replace('site_name', 'cell_name') | replace('_', ' ') | upper }}</th>{% endfor %}</tr></thead><tbody>{% for row in rf_data %}<tr><td class="text-center bg-white border-end shadow-sm" style="position: sticky; left: 0; z-index: 5;"><a href="/rf/detail/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-primary py-0"><i class="fa-solid fa-eye"></i></a>{% if current_user.role == 'admin' %}<a href="/rf/edit/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-warning py-0"><i class="fa-solid fa-pen"></i></a><a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-danger py-0" onclick="return confirm('Xóa?')"><i class="fa-solid fa-trash"></i></a>{% endif %}</td>{% for col in rf_columns %}<td>{{ row[col] }}</td>{% endfor %}</tr>{% else %}<tr><td colspan="100%" class="text-center py-4 text-muted"><i class="fa-solid fa-magnifying-glass fa-2x mb-2 d-block opacity-50"></i>Không tìm thấy trạm nào.</td></tr>{% endfor %}</tbody></table></div>
        
        {% elif active_page == 'import' %}
            <div class="row"><div class="col-md-8"><div class="tab-content bg-white p-4 rounded-3 shadow-sm border"><h5 class="mb-3 text-primary"><i class="fa-solid fa-cloud-arrow-up me-2"></i>Data Import</h5><ul class="nav nav-tabs mb-4" id="importTabs" role="tablist"><li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabRF" type="button">Import RF</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabPOI" type="button">Import POI</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabKPI" type="button">Import KPI</button></li><li class="nav-item"><button class="nav-link text-primary fw-bold" data-bs-toggle="tab" data-bs-target="#tabQoE" type="button">Import QoE/QoS</button></li><li class="nav-item"><button class="nav-link text-danger fw-bold" data-bs-toggle="tab" data-bs-target="#tabReset" type="button">Reset Data</button></li></ul>
            <div class="tab-content">
                <div class="tab-pane fade show active" id="tabRF"><form action="/import" method="POST" enctype="multipart/form-data"><div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu RF</label><select name="type" class="form-select"><option value="3g">RF 3G (Gộp tự động Config & CELL)</option><option value="4g">RF 4G</option><option value="5g">RF 5G</option></select></div><div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div><button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload RF Data</button></form></div>
                <div class="tab-pane fade" id="tabPOI"><form action="/import" method="POST" enctype="multipart/form-data"><div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu POI</label><select name="type" class="form-select"><option value="poi4g">POI 4G</option><option value="poi5g">POI 5G</option></select></div><div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div><button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload POI Data</button></form></div>
                <div class="tab-pane fade" id="tabKPI"><form action="/import" method="POST" enctype="multipart/form-data"><div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu KPI</label><select name="type" class="form-select"><option value="kpi3g">KPI 3G</option><option value="kpi4g">KPI 4G</option><option value="kpi5g">KPI 5G</option></select></div><div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div><button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload KPI Data</button></form></div>
                <div class="tab-pane fade" id="tabQoE"><form action="/import" method="POST" enctype="multipart/form-data"><div class="mb-3"><label class="form-label fw-bold text-primary">Chọn Loại Dữ Liệu QoE/QoS</label><select name="type" id="importQoEQoSType" class="form-select border-primary"><option value="qoe4g">QoE 4G</option><option value="qos4g">QoS 4G</option></select></div><div class="mb-3"><label class="form-label fw-bold">Tên Tuần</label><input type="text" name="week_name" id="importWeekName" class="form-control" value="{{ next_qoe }}" required></div><div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div><button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload Data</button></form></div>
                <div class="tab-pane fade" id="tabReset"><div class="alert alert-warning border-0 shadow-sm mb-4"><i class="fa-solid fa-triangle-exclamation me-2"></i><strong>Cảnh báo:</strong> Hành động này sẽ xóa sạch dữ liệu khỏi cơ sở dữ liệu. Không thể hoàn tác!</div><div class="d-flex flex-column gap-3"><form action="/reset-data" method="POST" onsubmit="return confirm('CẢNH BÁO TỐI KHẨN: Bạn có CHẮC CHẮN muốn xóa sạch toàn bộ dữ liệu cấu hình RF của 3G, 4G, 5G?');"><input type="hidden" name="target" value="rf"><button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu RF</button></form><form action="/reset-data" method="POST" onsubmit="return confirm('CẢNH BÁO TỐI KHẨN: Bạn có CHẮC CHẮN muốn xóa sạch toàn bộ dữ liệu địa điểm POI của 4G, 5G?');"><input type="hidden" name="target" value="poi"><button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu POI</button></form></div></div>
            </div></div></div>
            <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-white fw-bold text-success border-bottom">Data History</div><div class="card-body p-0 overflow-auto" style="max-height: 400px;"><table class="table table-sm table-striped mb-0 text-center"><thead class="table-light sticky-top"><tr><th>3G</th><th>4G</th><th>5G</th></tr></thead><tbody>{% for r3, r4, r5 in kpi_rows %}<tr><td>{{ r3 or '-' }}</td><td>{{ r4 or '-' }}</td><td>{{ r5 or '-' }}</td></tr>{% endfor %}</tbody></table></div></div></div></div>
            <script>document.getElementById('importQoEQoSType').addEventListener('change', function(){document.getElementById('importWeekName').value=this.value==='qoe4g'?'{{ next_qoe }}':'{{ next_qos }}';});</script>
        
        {% elif active_page == 'script' %}
            <div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Generate Script</div><div class="card-body"><ul class="nav nav-tabs mb-3" id="scriptTabs" role="tablist"><li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab3g900" type="button">3G 900</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab4g" type="button">4G</button></li><li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab3g2100" type="button">3G 2100</button></li></ul><div class="tab-content"><div class="tab-pane fade show active" id="tab3g900"><form method="POST" action="/script"><input type="hidden" name="tech" value="3g900"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g900"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="70"></td><td><input type="number" name="hsn[]" class="form-control" value="2"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g900')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form></div><div class="tab-pane fade" id="tab4g"><form method="POST" action="/script"><input type="hidden" name="tech" value="4g"><div class="table-responsive"><table class="table table-bordered" id="rruTable_4g"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="60"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="4"></td><td><input type="number" name="txnum[]" class="form-control" value="4"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('4g')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form></div><div class="tab-pane fade" id="tab3g2100"><form method="POST" action="/script"><input type="hidden" name="tech" value="3g2100"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g2100"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="80"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g2100')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form></div></div>{% if script_result %}<div class="mt-4"><h5 class="fw-bold text-primary">Result:</h5><textarea class="form-control font-monospace bg-light border-0" rows="12" readonly>{{ script_result }}</textarea></div>{% endif %}</div></div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})

def render_page(tpl, **kwargs):
    if tpl == BACKUP_RESTORE_TEMPLATE: return render_template_string(tpl, **kwargs)
    return render_template_string(tpl, **kwargs)

# ==============================================================================
# 5. ROUTES CHỨC NĂNG CHÍNH
# ==============================================================================
def send_telegram_message(chat_id, text_content):
    if not TELEGRAM_BOT_TOKEN: return
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text_content, "parse_mode": "HTML"})

def send_telegram_photo(chat_id, photo_url, caption=""):
    if not TELEGRAM_BOT_TOKEN: return
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto", json={"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"})

def process_bot_command(text):
    text = str(text).strip().upper()
    parts = text.split()
    if not parts: return "🤖 Lỗi cú pháp!"
    cmd = parts[0]
    
    if cmd == 'HELP': return "Hướng dẫn: DASHBOARD, KPI [Mã], CTS [Mã], RF [Mã]"
    
    with app.app_context():
        if cmd == 'DASHBOARD':
            records = db.session.query(KPI4G.thoi_gian, func.sum(KPI4G.traffic).label('traffic'), func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput')).group_by(KPI4G.thoi_gian).order_by(KPI4G.thoi_gian.desc()).limit(7).all()
            if not records: return "❌ Chưa có dữ liệu hệ thống 4G."
            records.reverse()
            labels = [r[0] for r in records if r[0]]
            def create_dash_url(label, data, color, title):
                cfg = {"type": "line", "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "fill": False}]}, "options": {"title": {"display": True, "text": title}}}
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"
            charts = []
            metrics = [("Total Traffic (GB)", [round(r[1] or 0, 2) for r in records], "#0078d4", "Tổng Traffic 4G"), ("Avg Thput (Mbps)", [round(r[2] or 0, 2) for r in records], "#107c10", "Tốc độ DL")]
            for l, d, c, t in metrics: charts.append({"type": "photo", "url": create_dash_url(l, d, c, t), "caption": f"📈 {t}"})
            return charts

        if len(parts) < 2: return "🤖 Vui lòng nhập đúng mẫu. (VD: KPI THA001)"
        target = parts[-1] 
        
        if cmd == 'CTS':
            qoe = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).first()
            if not qoe: return f"❌ Không tìm thấy QoE cho {target}"
            return f"🌟 THÔNG SỐ QoE - {target}\nTuần: {qoe.week_name}\nĐiểm: {qoe.qoe_score} ⭐"

        tech = '4g'
        if len(parts) >= 3 and parts[1].lower() in ['3g', '4g', '5g']: tech = parts[1].lower()

        if cmd == 'KPI':
            Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
            record = Model.query.filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).first() if Model else None
            if record: return f"📊 KPI {tech.upper()} - {record.ten_cell}\nNgày: {record.thoi_gian}\nTraffic: {record.traffic} GB"
            return f"❌ Không có KPI cho {target}"
            
        elif cmd == 'RF':
            Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
            record = Model.query.filter(Model.cell_code.ilike(f"%{target}%")).first() if Model else None
            if record: return f"📡 RF {tech.upper()} - {record.cell_code}\nTọa độ: {record.latitude}, {record.longitude}\nAzimuth: {record.azimuth}"
            return f"❌ Không có RF cho {target}"
            
    return "🤖 Cú pháp không hỗ trợ."

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    data = request.json
    if data and 'message' in data:
        chat_id = data['message']['chat']['id']
        text = data['message'].get('text', '')
        if text:
            reply = process_bot_command(text)
            if isinstance(reply, list):
                for item in reply: send_telegram_photo(chat_id, item['url'], item.get('caption', ''))
            else: send_telegram_message(chat_id, str(reply))
    return jsonify({"status": "success"}), 200

@app.route('/telegram/set_webhook')
def set_telegram_webhook():
    if not TELEGRAM_BOT_TOKEN: return "Missing Bot Token", 400
    webhook_url = request.host_url.rstrip('/') + url_for('telegram_webhook')
    try: return jsonify(requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook?url={webhook_url}").json())
    except Exception as e: return str(e), 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        user = User.query.filter_by(username=request.form['username']).first()
        if user and user.check_password(request.form['password']): login_user(user); return redirect(url_for('index'))
        flash('Login failed', 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    dashboard_data = {'labels': [], 'traffic': [], 'thput': [], 'prb': [], 'cqi': []}
    try:
        records = db.session.query(KPI4G.thoi_gian, func.sum(KPI4G.traffic).label('traffic'), func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput'), func.avg(KPI4G.res_blk_dl).label('res_blk_dl'), func.avg(KPI4G.cqi_4g).label('cqi_4g')).group_by(KPI4G.thoi_gian).all()
        if records:
            agg = {r[0]: {'traf': r[1] or 0, 'thput': r[2] or 0, 'prb': r[3] or 0, 'cqi': r[4] or 0} for r in records if r[0]}
            sorted_dates = sorted(agg.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
            dashboard_data['labels'] = sorted_dates
            for d in sorted_dates:
                dashboard_data['traffic'].append(round(agg[d]['traf'], 2))
                dashboard_data['thput'].append(round(agg[d]['thput'], 2))
                dashboard_data['prb'].append(round(agg[d]['prb'], 2))
                dashboard_data['cqi'].append(round(agg[d]['cqi'], 2))
    except: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard', dashboard_data=dashboard_data)

@app.route('/azimuth')
@login_required
def azimuth(): return render_page(CONTENT_TEMPLATE, title="Azimuth", active_page='azimuth')

@app.route('/optimize')
@login_required
def optimize():
    all_w = sorted(list(set([r[0] for r in db.session.query(QoE4G.week_name).distinct()])), reverse=True)
    sel_w = request.args.get('week_name') or (all_w[0] if all_w else None)
    opt_data = []
    return render_page(CONTENT_TEMPLATE, title="Tối ưu QoE", active_page='optimize', optimized_data=opt_data, latest_week=sel_w, all_weeks=all_w)

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis():
    tech = request.form.get('tech', '4g') if request.method == 'POST' else request.args.get('tech', '4g')
    return render_page(CONTENT_TEMPLATE, title="Bản đồ GIS", active_page='gis', selected_tech=tech, gis_data=[], its_data=[], show_its=False, rf_cols=[])

@app.route('/kpi')
@login_required
def kpi(): return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi', selected_tech='4g', poi_list=[], charts={})

@app.route('/qoe-qos')
@login_required
def qoe_qos(): return render_page(CONTENT_TEMPLATE, title="QoE & QoS Analytics", active_page='qoe_qos', charts={})

@app.route('/poi')
@login_required
def poi(): return render_page(CONTENT_TEMPLATE, title="POI Report", active_page='poi', poi_list=[], poi_charts={})

@app.route('/worst-cell')
@login_required
def worst_cell(): return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell', worst_cells=[], dates=[])

@app.route('/traffic-down')
@login_required
def traffic_down(): return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', zero_traffic=[], degraded=[])

@app.route('/conges-3g')
@login_required
def conges_3g(): return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=[])

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '4g')
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    cols = RF_COLS_ORDER.get(tech, [c.key for c in Model.__table__.columns if c.key != 'id']) if Model else []
    data = [{c: getattr(r, c) for c in cols} | {'id': r.id} for r in (Model.query.limit(100).all() if Model else [])]
    return render_page(CONTENT_TEMPLATE, title="RF Database", active_page='rf', current_tech=tech, rf_columns=cols, rf_data=data)

@app.route('/rf/detail/<tech>/<int:id>')
@login_required
def rf_detail(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    cols = RF_COLS_ORDER.get(tech, [c.key for c in Model.__table__.columns if c.key != 'id'])
    clean_obj = {c: getattr(obj, c) for c in cols if hasattr(obj, c)}
    return render_page(RF_DETAIL_TEMPLATE, obj=clean_obj, tech=tech)

# ==============================================================================
# HÀM IMPORT - CHỨA THUẬT TOÁN GỘP FILE 3G VÀ TỰ ĐỘNG CHUẨN HÓA DỮ LIỆU
# ==============================================================================
@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    if request.method == 'POST':
        files = request.files.getlist('file')
        itype = request.form.get('type')
        
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
                            if math.isnan(val1): val1 = 0.0
                            if math.isnan(val2): val2 = 0.0
                            percent, score = max(val1, val2), min(val1, val2)
                            details = json.dumps({headers[j]: str(row_data[j]).strip() for j in range(len(headers)) if pd.notna(row_data[j])}, ensure_ascii=False)
                            records.append({'cell_name': c_name, 'week_name': week_name, 'qoe_score' if itype == 'qoe4g' else 'qos_score': score, 'qoe_percent' if itype == 'qoe4g' else 'qos_percent': percent, 'details': details})
                        if records:
                            db.session.bulk_insert_mappings(TargetModel, records)
                            db.session.commit()
                            flash(f'Import thành công {len(records)} dòng QoE/QoS.', 'success')
                except Exception as e: flash(f'Lỗi: {e}', 'danger')
                
        else:
            cfg = {'3g': [Config3G, Cell3G], '4g': RF4G, '5g': RF5G, 'kpi3g': KPI3G, 'kpi4g': KPI4G, 'kpi5g': KPI5G, 'poi4g': POI4G, 'poi5g': POI5G}
            
            # Ép thứ tự: Nếu có nhiều file 3G, file có chữ Config sẽ được nạp trước
            if itype == '3g':
                files = sorted(files, key=lambda f: 0 if 'config' in getattr(f, 'filename', '').lower() else 1)
                
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
                    
                    # NHẬN DIỆN THÔNG MINH FILE NÀO CỦA 3G ĐANG ĐƯỢC NẠP VÀO
                    current_itype = itype
                    is_update_only = False
                    if itype == '3g':
                        raw_headers_str = " ".join([str(x).lower().strip() for x in preview_df.iloc[h_idx].values if pd.notna(x)])
                        if 'hoàn cảnh ra đời' in raw_headers_str or 'antenna tên hãng sx' in raw_headers_str or 'tên trên hệ thống' in raw_headers_str:
                            current_itype = 'cell3g'
                            is_update_only = True
                        else:
                            current_itype = 'config3g'
                            
                    Model = cfg.get(current_itype) if itype == '3g' else cfg.get(itype)
                    if not Model: continue
                    valid_cols = [c.key for c in Model.__table__.columns if c.key != 'id']
                    
                    file.seek(0)
                    CHUNK_SIZE = 2500
                    if f_name_lower.endswith('.csv'):
                        chunks = pd.read_csv(file, header=h_idx, encoding='utf-8-sig', on_bad_lines='skip', low_memory=False, chunksize=CHUNK_SIZE)
                    else:
                        full_df = pd.read_excel(BytesIO(file.read()), header=h_idx)
                        chunks = [full_df[i:i + CHUNK_SIZE] for i in range(0, full_df.shape[0], CHUNK_SIZE)]
                        del full_df
                        gc.collect()

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
                                        else:
                                            clean_row[k] = val[:250] if len(val)>250 else val
                            
                            # ÉP CHUẨN IN HOA CHỐNG LỖI LỆCH KHÓA CHÍNH (Vd: 3G_BSN001 == 3g_bsn001)
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
                                    # CELL_3G CHỈ ĐƯỢC PHÉP CẬP NHẬT, KHÔNG ĐƯỢC ĐẺ THÊM RÁC
                                    if not is_update_only:
                                        new_obj = Model(**cr)
                                        existing_rf_map[cc] = new_obj
                                        db.session.add(new_obj)
                                    
                            db.session.commit()
                            total_inserted += len(records_to_process)
                            
                        elif not is_rf_model and records_to_process:
                            db.session.bulk_insert_mappings(Model, records_to_process)
                            db.session.commit()
                            total_inserted += len(records_to_process)
                            
                        del records_to_process
                        del cell_codes_in_chunk
                        gc.collect()

                    flash(f'Import hoàn tất file {file.filename} ({total_inserted} dòng)', 'success')
                except Exception as e: 
                    db.session.rollback()
                    flash(f'Lỗi file {file.filename}: {e}', 'danger')
                    
            # BƯỚC CUỐI CÙNG SAU KHI NẠP XONG 2 FILE 3G: TỰ ĐỘNG GỘP VÀO BẢNG RF3G CHÍNH
            if itype == '3g':
                try:
                    db.session.query(RF3G).delete()
                    db.session.commit()
                    
                    configs = {str(c.cell_code).upper(): c for c in Config3G.query.all() if c.cell_code}
                    cells = {str(c.cell_code).upper(): c for c in Cell3G.query.all() if c.cell_code}
                    
                    rf3g_inserts = []
                    for cc, cfg_row in configs.items():
                        cell_row = cells.get(cc)
                        rf3g_inserts.append({
                            'cell_code': cc, 'site_code': cfg_row.site_code, 'cell_name': cfg_row.cell_name,
                            'csht_code': cfg_row.csht_code, 'latitude': cfg_row.latitude, 'longitude': cfg_row.longitude,
                            'antena': cfg_row.antena, 'azimuth': cfg_row.azimuth, 'total_tilt': cfg_row.total_tilt,
                            'equipment': cfg_row.equipment, 'frequency': cfg_row.frequency, 'psc': cfg_row.psc,
                            'dl_uarfcn': cfg_row.dl_uarfcn, 'bsc_lac': cfg_row.bsc_lac, 'ci': cfg_row.ci,
                            'anten_height': cfg_row.anten_height, 'm_t': cfg_row.m_t, 'e_t': cfg_row.e_t,
                            'hang_sx': cell_row.hang_sx if cell_row else None,
                            'swap': cell_row.swap if cell_row else None,
                            'start_day': cell_row.start_day if cell_row else None,
                            'ghi_chu': cell_row.ghi_chu if cell_row else None
                        })
                    
                    if rf3g_inserts:
                        db.session.bulk_insert_mappings(RF3G, rf3g_inserts)
                        db.session.commit()
                        flash(f'Đã tự động tổng hợp {len(rf3g_inserts)} trạm hoàn chỉnh vào Danh bạ RF 3G!', 'info')
                except Exception as e:
                    db.session.rollback()
                    flash(f'Lỗi Gộp RF 3G: {e}', 'danger')

        return redirect(url_for('import_data'))

    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    today = datetime.now()
    start_w = today - timedelta(days=today.weekday())
    next_w_str = f"Tuần {today.isocalendar()[1]:02d} ({start_w.strftime('%d/%m')}-{(start_w + timedelta(days=6)).strftime('%d/%m')})"
    return render_page(CONTENT_TEMPLATE, title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), next_qoe=next_w_str, next_qos=next_w_str)

@app.route('/reset-data', methods=['POST'])
@login_required
def reset_data():
    if current_user.role != 'admin': return redirect(url_for('index'))
    target = request.form.get('target')
    try:
        if target == 'rf':
            db.session.query(RF3G).delete(); db.session.query(Config3G).delete(); db.session.query(Cell3G).delete(); db.session.query(RF4G).delete(); db.session.query(RF5G).delete(); db.session.commit()
            flash('Đã xóa sạch dữ liệu RF', 'success')
        elif target == 'poi':
            db.session.query(POI4G).delete(); db.session.query(POI5G).delete(); db.session.commit()
            flash('Đã xóa sạch POI', 'success')
    except Exception as e: db.session.rollback(); flash(f'Lỗi: {e}', 'danger')
    return redirect(url_for('import_data'))

@app.route('/script', methods=['GET', 'POST'])
@login_required
def script():
@app.route('/backup', methods=['POST'])
@login_required
def backup_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    selected_tables = request.form.getlist('tables')
    if not selected_tables:
        flash('No tables selected', 'warning'); return redirect(url_for('backup_restore'))
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

@app.route('/restore', methods=['POST'])
@login_required
def restore_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    file = request.files['file']
    if file:
        try:
            with zipfile.ZipFile(BytesIO(file.read())) as zf:
                models = {'users.csv': User, 'config_3g.csv': Config3G, 'cell_3g.csv': Cell3G, 'rf3g.csv': RF3G, 'rf4g.csv': RF4G, 'rf5g.csv': RF5G, 'poi4g.csv': POI4G, 'poi5g.csv': POI5G, 'kpi3g.csv': KPI3G, 'kpi4g.csv': KPI4G, 'kpi5g.csv': KPI5G, 'qoe_4g.csv': QoE4G, 'qos_4g.csv': QoS4G}
                for fname in zf.namelist():
                    if fname in models:
                        Model = models[fname]
                        with zf.open(fname) as f: df = pd.read_csv(f, encoding='utf-8-sig')
                        db.session.query(Model).delete()
                        records = df.to_dict('records')
                        clean_records = [{k: (v if not pd.isna(v) else None) for k, v in r.items() if k in [c.key for c in Model.__table__.columns]} for r in records]
                        if clean_records: db.session.bulk_insert_mappings(Model, clean_records)
            db.session.commit(); flash('Restore Success', 'success')
        except Exception as e: db.session.rollback(); flash(f'Error: {e}', 'danger')
    return redirect(url_for('backup_restore'))

@app.route('/backup-restore')
@login_required
def backup_restore(): return render_page(BACKUP_RESTORE_TEMPLATE, title="Backup", active_page='backup_restore')

@app.route('/users')
@login_required
def manage_users(): return render_page(USER_MANAGEMENT_TEMPLATE, users=User.query.all(), active_page='users')

@app.route('/users/add', methods=['POST'])
@login_required
def add_user(): u = User(username=request.form['username'], role=request.form['role']); u.set_password(request.form['password']); db.session.add(u); db.session.commit(); return redirect(url_for('manage_users'))

@app.route('/users/delete/<int:id>')
@login_required
def delete_user(id): db.session.delete(db.session.get(User, id)); db.session.commit(); return redirect(url_for('manage_users'))

@app.route('/profile')
@login_required
def profile(): return render_page(PROFILE_TEMPLATE, active_page='profile')

@app.route('/change-password', methods=['POST'])
@login_required
def change_password(): 
    if current_user.check_password(request.form['current_password']): current_user.set_password(request.form['new_password']); db.session.commit(); flash('Done', 'success')
    return redirect(url_for('profile'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
