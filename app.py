import os
import jinja2
import pandas as pd
import json
import gc
import re
import zipfile
import unicodedata
import random
import math
import requests
import urllib.parse
from io import BytesIO, StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text, func, inspect, or_, and_
from itertools import zip_longest
from collections import defaultdict

# ==============================================================================
# 1. APP CONFIGURATION & DATABASE SETUP
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
# 2. UTILS
# ==============================================================================

def remove_accents(input_str):
    if not isinstance(input_str, str): return str(input_str)
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨíŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴịỶảỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYaYy'
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
        'POI': 'poi_name', 'Cell_Code': 'cell_code', 'Site_Code': 'site_code',
        'CSHT_code': 'csht_code', 'Hãng_SX': 'hang_sx', 'Antena': 'antena',
        'Swap': 'swap', 'Start_day': 'start_day', 'Ghi_chú': 'ghi_chu',
        'Anten_height': 'anten_height', 'Azimuth': 'azimuth', 'M_T': 'm_t', 'E_T': 'e_t', 'Total_tilt': 'total_tilt',
        'PSC': 'psc', 'DL_UARFCN': 'dl_uarfcn', 'BSC_LAC': 'bsc_lac', 'CI': 'ci',
        'Latitude': 'latitude', 'Longitude': 'longitude', 'Equipment': 'equipment',
        'nrarfcn': 'nrarfcn', 'Lcrid': 'lcrid', 'Đồng_bộ': 'dong_bo',
        'CellID': 'cellid', 'NetworkTech': 'networktech'
    }
    col_upper = col_name.upper()
    for key, val in special_map.items():
        if key.upper() == col_upper: return val
    clean = re.sub(r'[^a-z0-9]', '_', remove_accents(col_name).lower())
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
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
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
    site_name = db.Column(db.String(100))
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
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))

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
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))

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
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_gnodeb = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))

class QoE4G(db.Model):
    __tablename__ = 'qoe_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_name = db.Column(db.String(100), index=True)
    week_name = db.Column(db.String(100))
    qoe_score = db.Column(db.Float)
    qoe_percent = db.Column(db.Float)
    details = db.Column(db.Text)

class QoS4G(db.Model):
    __tablename__ = 'qos_4g'
    id = db.Column(db.Integer, primary_key=True)
    cell_name = db.Column(db.String(100), index=True)
    week_name = db.Column(db.String(100))
    qos_score = db.Column(db.Float)
    qos_percent = db.Column(db.Float)
    details = db.Column(db.Text)

class ITSLog(db.Model):
    __tablename__ = 'its_log'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    networktech = db.Column(db.String(20))
    level = db.Column(db.Float)
    qual = db.Column(db.Float)
    cellid = db.Column(db.String(50))

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

def init_database():
    with app.app_context():
        db.create_all()
        if not User.query.filter_by(username='admin').first():
            u = User(username='admin', role='admin')
            u.set_password('admin123')
            db.session.add(u)
            db.session.commit()
init_database()

# ==============================================================================
# 4. TEMPLATES (DEFINED AT MODULE LEVEL - DO NOT INDENT)
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
        .card { border: none; border-radius: var(--border-radius); background: rgba(255, 255, 255, 0.85); backdrop-filter: blur(10px); box-shadow: var(--shadow-soft); transition: transform 0.3s ease, box-shadow 0.3s ease; margin-bottom: 1.5rem; overflow: hidden; }
        .card:hover { transform: translateY(-2px); box-shadow: var(--shadow-hover); }
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
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><i class="fa-solid fa-network-wired"></i> NetOps</div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/gis" class="{{ 'active' if active_page == 'gis' else '' }}"><i class="fa-solid fa-map-location-dot"></i> Bản đồ GIS</a></li>
            <li><a href="/azimuth" class="{{ 'active' if active_page == 'azimuth' else '' }}"><i class="fa-solid fa-compass"></i> Tính Azimuth</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI Analytics</a></li>
            <li><a href="/qoe-qos" class="{{ 'active' if active_page == 'qoe_qos' else '' }}"><i class="fa-solid fa-star-half-stroke"></i> QoE QoS Analytics</a></li>
            <li><a href="/optimize" class="{{ 'active' if active_page == 'optimize' else '' }}"><i class="fa-solid fa-wand-magic-sparkles"></i> Tối ưu QoE/QoS</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF Database</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-pin"></i> POI Report</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cells</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Congestion 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}"><i class="fa-solid fa-code"></i> Script</a></li>
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
        <button class="btn btn-light shadow-sm d-md-none mb-3 border" onclick="document.getElementById('sidebar').classList.toggle('active')">
            <i class="fa-solid fa-bars"></i> Menu
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
            <div id="azimuthFormContainer" class="shadow-lg" style="display: none; background: rgba(255, 255, 255, 0.95); padding: 15px; border-radius: 8px; width: 320px; max-height: 70vh; overflow-y: auto; border: 1px solid #dee2e6;">
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
                        zoomControl: false, // Tắt zoom mặc định (topleft) để nhường chỗ cho Form
                        fullscreenControl: true, // Bật Fullscreen chuẩn của thư viện
                        fullscreenControlOptions: { position: 'bottomright' } // Đặt cạnh nút Zoom
                    });
                    
                    // Thêm lại nút zoom ở góc dưới phải
                    L.control.zoom({ position: 'bottomright' }).addTo(azMap);

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

                    // --- CHUYỂN FORM VÀO TRONG BẢN ĐỒ LÀM WIDGET ---
                    var formControl = L.control({position: 'topleft'});
                    formControl.onAdd = function (map) {
                        var div = document.getElementById('azimuthFormContainer');
                        div.style.display = 'block'; 
                        L.DomEvent.disableClickPropagation(div);
                        L.DomEvent.disableScrollPropagation(div);
                        return div;
                    };
                    formControl.addTo(azMap);

                    // Sự kiện Click lên bản đồ để lấy toạ độ Điểm O
                    azMap.on('click', function(e) {
                        if (!markerO) { // CHỈ TẠO LẦN ĐẦU TIÊN
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
                        zoomControl: false,
                        fullscreenControl: true, // Bật nút Fullscreen mặc định
                        fullscreenControlOptions: { position: 'bottomright' }
                    });
                    
                    // Thêm nút zoom ở góc dưới phải
                    L.control.zoom({ position: 'bottomright' }).addTo(map);

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

                    // Add Custom Settings Control
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
                                html += '</div>';
                                html += '</div>';
                            }

                            html += '</div>';
                            div.innerHTML = html;
                            return div;
                        };
                        settingsControl.addTo(map);
                    }

                    // Bounds Management
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
                            for (const [k, v] of Object.entries(cell.info)) {
                                if (v !== null && v !== '' && v !== 'None') {
                                    infoHtml += "<tr><th class='text-muted bg-light w-50'>" + k.toUpperCase() + "</th><td class='fw-bold'>" + v + "</td></tr>";
                                }
                            }
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
                            if (level >= -75) return '#0000FF'; // Blue
                            if (level >= -85) return '#00FF00'; // Green
                            if (level >= -95) return '#FFFF00'; // Yellow
                            if (level >= -105) return '#FFA500'; // Orange
                            if (level >= -115) return '#FF0000'; // Red
                            return '#000000'; // Black
                        } else { 
                            if (level >= -65) return '#0000FF'; // Blue
                            if (level >= -75) return '#00FF00'; // Green
                            if (level >= -85) return '#FFFF00'; // Yellow
                            if (level >= -95) return '#FFA500'; // Orange
                            if (level >= -105) return '#FF0000'; // Red
                            return '#000000'; // Black
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

                    // Add Legend
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
                            
                            var html = '';
                            var has4G = itsData.some(pt => (pt.tech || '').toUpperCase().includes('4G') || (pt.tech || '').toUpperCase().includes('LTE'));
                            var has3G = itsData.some(pt => !(pt.tech || '').toUpperCase().includes('4G') && !(pt.tech || '').toUpperCase().includes('LTE'));

                            if (has4G) {
                                html += '<strong class="text-primary fs-6 d-block mb-2"><i class="fa-solid fa-signal me-1"></i> Chú giải 4G RSRP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -75)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-95 đến -85)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-115 đến -105)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -115)</div>';
                            }
                            if (has3G) {
                                if (has4G) html += '<hr class="my-2">';
                                html += '<strong class="text-success fs-6 d-block mb-2"><i class="fa-solid fa-signal me-1"></i> Chú giải 3G RSCP</strong>';
                                html += '<div><i style="background:#0000FF; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất tốt (≥ -65)</div>';
                                html += '<div><i style="background:#00FF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Tốt (-75 đến -65)</div>';
                                html += '<div><i style="background:#FFFF00; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Khá (-85 đến -75)</div>';
                                html += '<div><i style="background:#FFA500; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Kém (-95 đến -85)</div>';
                                html += '<div><i style="background:#FF0000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Rất kém (-105 đến -95)</div>';
                                html += '<div><i style="background:#000000; width:16px; height:16px; display:inline-block; margin-right:8px; border-radius:3px;"></i> Mất sóng (< -105)</div>';
                            }
                            div.innerHTML = html;
                            return div;
                        };
                        legend.addTo(map);
                    }
                });
            </script>

        {% elif active_page == 'kpi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/kpi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-2"><label class="form-label fw-bold small text-muted">CÔNG NGHỆ</label><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option></select></div>
                        <div class="col-md-4"><label class="form-label fw-bold small text-muted">TÌM THEO POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                        <div class="col-md-3"><label class="form-label fw-bold small text-muted">NHẬP CELL/SITE</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="Site code, Cell list..." value="{{ cell_name_input }}"></div>
                        <div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm">Visualize</button></div>
                    </form>
                </div>
            </div>
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
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/qoe-qos" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-6"><label class="form-label fw-bold small text-muted">NHẬP CELL NAME 4G</label><input type="text" name="cell_name" class="form-control border-0 shadow-sm" placeholder="VD: THA001_1" value="{{ cell_name_input }}" required></div>
                        <div class="col-md-2 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm"><i class="fa-solid fa-search me-2"></i>Tra cứu</button></div>
                        {% if has_data %}<div class="col-md-3 align-self-end"><a href="/kpi?tech=4g&cell_name={{ cell_name_input }}" class="btn btn-success w-100 shadow-sm"><i class="fa-solid fa-link me-2"></i>Link tới KPI Cell</a></div>{% endif %}
                    </form>
                </div>
            </div>
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
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/poi" class="row g-3 align-items-center bg-light p-3 rounded-3 border">
                        <div class="col-md-8"><label class="form-label fw-bold small text-muted">CHỌN POI</label><input type="text" name="poi_name" list="poi_list_kpi" class="form-control border-0 shadow-sm" placeholder="Chọn POI..." value="{{ selected_poi }}"><datalist id="poi_list_kpi">{% for p in poi_list %}<option value="{{ p }}">{% endfor %}</datalist></div>
                        <div class="col-md-4 align-self-end"><button type="submit" class="btn btn-primary w-100 shadow-sm">Xem Báo Cáo</button></div>
                    </form>
                </div>
            </div>
            {% if poi_charts %}
                <div class="row">
                    {% for chart_id, c in poi_charts.items() %}
                    <div class="col-md-6 mb-4"><div class="card h-100 border-0 shadow-sm"><div class="card-body p-4"><h6 class="card-title text-secondary fw-bold mb-3">{{ c.title }}</h6><div class="chart-container" style="position: relative; height:35vh; width:100%"><canvas id="{{ chart_id }}"></canvas></div></div></div></div>
                    <script>(function(){ const cd={{ c | tojson }}; new Chart(document.getElementById('{{ chart_id }}').getContext('2d'),{type:'line',data:cd,options:{responsive:true,maintainAspectRatio:false,spanGaps:true,elements:{line:{tension:0.3}},interaction:{mode:'nearest',intersect:false,axis:'x'},onClick:(e,el)=>{if(el.length>0)showDetailModal(cd.datasets[el[0].datasetIndex].label,cd.labels[el[0].index],cd.datasets[el[0].datasetIndex].data[el[0].index],'{{ c.title }}',cd.datasets,cd.labels)}}});})();</script>
                    {% endfor %}
                </div>
            {% elif selected_poi %}
                <div class="alert alert-warning border-0 shadow-sm">Không có dữ liệu cho POI: <strong>{{ selected_poi }}</strong></div>
            {% else %}
                <div class="text-center text-muted py-5"><i class="fa-solid fa-map-location-dot fa-3x mb-3"></i><p>Chọn địa điểm POI để xem báo cáo.</p></div>
            {% endif %}

        {% elif active_page == 'worst_cell' %}
            <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/worst-cell" class="row g-3 align-items-center bg-light p-3 rounded-3 border"><div class="col-auto"><label class="col-form-label fw-bold text-muted">THỜI GIAN</label></div><div class="col-auto"><select name="duration" class="form-select border-0 shadow-sm"><option value="1" {% if duration == 1 %}selected{% endif %}>1 ngày mới nhất</option><option value="3" {% if duration == 3 %}selected{% endif %}>3 ngày liên tiếp</option><option value="7" {% if duration == 7 %}selected{% endif %}>7 ngày liên tiếp</option><option value="15" {% if duration == 15 %}selected{% endif %}>15 ngày liên tiếp</option><option value="30" {% if duration == 30 %}selected{% endif %}>30 ngày liên tiếp</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-danger shadow-sm">Lọc Worst Cell</button></div><div class="col-auto"><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel me-2"></i>Export Excel</button></div></form></div></div>
            {% if dates %}<div class="alert alert-info border-0 shadow-sm mb-4"><i class="fa-solid fa-calendar-days me-2"></i><strong>Xét duyệt:</strong> {% for d in dates %}<span class="badge bg-white text-info border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 70vh;">
                <table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light position-sticky top-0" style="z-index: 10;"><tr><th>Cell Name</th><th class="text-center">Avg Thput</th><th class="text-center">Avg PRB</th><th class="text-center">Avg CQI</th><th class="text-center">Avg Drop Rate</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in worst_cells %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_thput < 7000 }}">{{ r.avg_thput }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_res_blk > 20 }}">{{ r.avg_res_blk }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_cqi < 93 }}">{{ r.avg_cqi }}</td><td class="text-center {{ 'text-danger fw-bold' if r.avg_drop > 0.3 }}">{{ r.avg_drop }}</td><td class="text-center"><a href="/kpi?tech=4g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted">Nhấn "Lọc Worst Cell" để xem dữ liệu</td></tr>{% endfor %}</tbody></table>
            </div>

        {% elif active_page == 'traffic_down' %}
             <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/traffic-down" class="row g-3 align-items-center bg-light p-3 rounded-3 border"><div class="col-auto"><label class="col-form-label fw-bold text-muted">CÔNG NGHỆ:</label></div><div class="col-auto"><select name="tech" class="form-select border-0 shadow-sm"><option value="3g" {% if tech == '3g' %}selected{% endif %}>3G</option><option value="4g" {% if tech == '4g' %}selected{% endif %}>4G</option><option value="5g" {% if tech == '5g' %}selected{% endif %}>5G</option></select></div><div class="col-auto"><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm">Thực hiện</button><button type="submit" name="action" value="export_zero" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> Zero</button><button type="submit" name="action" value="export_degraded" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> Degraded</button><button type="submit" name="action" value="export_poi_degraded" class="btn btn-warning shadow-sm ms-2"><i class="fa-solid fa-file-excel"></i> POI Degraded</button></div><div class="col-auto ms-auto"><span class="badge bg-info text-dark">Ngày phân tích: {{ analysis_date }}</span></div></form></div></div>
            <div class="row g-4">
                <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-danger text-white fw-bold">Cell Không Lưu Lượng (< 0.1 GB)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Avg (7 Days)</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in zero_traffic %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.avg_last_7 }}</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">Cell Suy Giảm (> 30%)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>Cell Name</th><th class="text-end">Today</th><th class="text-end">Last Week</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded %}<tr><td class="fw-bold">{{ r.cell_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/kpi?tech={{ tech }}&cell_name={{ r.cell_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
                 <div class="col-md-4"><div class="card h-100 border-0 shadow-sm"><div class="card-header bg-warning text-dark fw-bold">POI Suy Giảm (> 30%)</div><div class="card-body p-0 table-responsive"><table class="table table-striped mb-0 small"><thead class="table-light"><tr><th>POI Name</th><th class="text-end">Today</th><th class="text-end">Last Week</th><th class="text-end">Degrade %</th><th class="text-center">Action</th></tr></thead><tbody>{% for r in degraded_pois %}<tr><td class="fw-bold">{{ r.poi_name }}</td><td class="text-end text-danger">{{ r.traffic_today }}</td><td class="text-end">{{ r.traffic_last_week }}</td><td class="text-end text-danger fw-bold">-{{ r.degrade_percent }}%</td><td class="text-center"><a href="/poi?poi_name={{ r.poi_name }}" class="btn btn-xs btn-outline-primary"><i class="fa-solid fa-chart-line"></i></a></td></tr>{% endfor %}</tbody></table></div></div></div>
            </div>

        {% elif active_page == 'conges_3g' %}
            <div class="row mb-4"><div class="col-md-12"><form method="GET" action="/conges-3g" class="d-flex align-items-center"><div class="alert alert-info border-0 shadow-sm bg-soft-primary text-primary mb-0 flex-grow-1"><strong>Điều kiện:</strong> (CS_CONG > 2% & CS_ATT > 100) OR (PS_CONG > 2% & PS_ATT > 500) (3 ngày liên tiếp)</div><button type="submit" name="action" value="execute" class="btn btn-primary shadow-sm ms-3">Thực hiện</button><button type="submit" name="action" value="export" class="btn btn-success shadow-sm ms-2"><i class="fa-solid fa-file-excel me-2"></i>Export</button></form></div></div>
            {% if dates %}<div class="mb-3 text-muted small"><i class="fa-solid fa-calendar me-2"></i>Xét duyệt: {% for d in dates %}<span class="badge bg-light text-dark border ms-1">{{ d }}</span>{% endfor %}</div>{% endif %}
            <div class="table-responsive bg-white rounded shadow-sm border"><table class="table table-hover mb-0" style="font-size: 0.9rem;"><thead class="bg-light"><tr><th>Cell Name</th><th>Avg CS Traffic</th><th>Avg CS Conges (%)</th><th>Avg PS Traffic</th><th>Avg PS Conges (%)</th><th class="text-center">Hành động</th></tr></thead><tbody>{% for r in conges_data %}<tr><td class="fw-bold text-primary">{{ r.cell_name }}</td><td>{{ r.avg_cs_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_cs_conges > 2 }}">{{ r.avg_cs_conges }}</td><td>{{ r.avg_ps_traffic }}</td><td class="{{ 'text-danger fw-bold' if r.avg_ps_conges > 2 }}">{{ r.avg_ps_conges }}</td><td class="text-center"><a href="/kpi?tech=3g&cell_name={{ r.cell_name }}" class="btn btn-sm btn-success text-white shadow-sm">View</a></td></tr>{% else %}<tr><td colspan="6" class="text-center py-5 text-muted opacity-50">Nhấn nút "Thực hiện" để xem kết quả</td></tr>{% endfor %}</tbody></table></div>

        {% elif active_page == 'rf' %}
             <div class="d-flex flex-wrap justify-content-between align-items-center mb-4 bg-white p-3 rounded shadow-sm border gap-3">
                 <div class="btn-group shadow-sm">
                     <a href="/rf?tech=3g" class="btn {{ 'btn-primary' if current_tech == '3g' else 'btn-outline-primary' }}">3G</a>
                     <a href="/rf?tech=4g" class="btn {{ 'btn-primary' if current_tech == '4g' else 'btn-outline-primary' }}">4G</a>
                     <a href="/rf?tech=5g" class="btn {{ 'btn-primary' if current_tech == '5g' else 'btn-outline-primary' }}">5G</a>
                 </div>
                 
                 <form method="GET" action="/rf" class="d-flex flex-grow-1 mx-lg-4">
                     <input type="hidden" name="tech" value="{{ current_tech }}">
                     <div class="input-group shadow-sm">
                         <span class="input-group-text bg-light border-end-0"><i class="fa-solid fa-search text-muted"></i></span>
                         <input type="text" name="cell_search" class="form-control border-start-0 ps-0" placeholder="Nhập Cell Code hoặc Site Code để tìm nhanh..." value="{{ search_query }}">
                         <button type="submit" class="btn btn-primary px-4 fw-bold">Tìm kiếm</button>
                     </div>
                 </form>

                 <div class="d-flex gap-2">
                     <form method="GET" action="/rf" class="m-0">
                         <input type="hidden" name="tech" value="{{ current_tech }}">
                         <input type="hidden" name="cell_search" value="{{ search_query }}">
                         <button type="submit" name="action" value="export" class="btn btn-success shadow-sm text-white fw-bold"><i class="fa-solid fa-file-excel me-2"></i>Export</button>
                     </form>
                     {% if current_user.role == 'admin' %}
                     <a href="/rf/add?tech={{ current_tech }}" class="btn btn-warning shadow-sm fw-bold"><i class="fa-solid fa-plus me-1"></i>New</a>
                     {% endif %}
                 </div>
             </div>
             
             <div class="table-responsive bg-white rounded shadow-sm border" style="max-height: 65vh;">
                 <table class="table table-hover mb-0" style="font-size: 0.85rem; white-space: nowrap;">
                     <thead class="table-light position-sticky top-0" style="z-index: 10;">
                         <tr>
                             <th class="text-center border-bottom bg-light" style="position: sticky; left: 0; z-index: 20;">Action</th>
                             {% for col in rf_columns %}<th>{{ col | replace('_', ' ') | upper }}</th>{% endfor %}
                         </tr>
                     </thead>
                     <tbody>
                         {% for row in rf_data %}
                         <tr>
                             <td class="text-center bg-white border-end shadow-sm" style="position: sticky; left: 0; z-index: 5;">
                                 <a href="/rf/detail/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-primary py-0"><i class="fa-solid fa-eye"></i></a>
                                 {% if current_user.role == 'admin' %}
                                 <a href="/rf/edit/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-warning py-0"><i class="fa-solid fa-pen"></i></a>
                                 <a href="/rf/delete/{{ current_tech }}/{{ row['id'] }}" class="btn btn-sm btn-outline-danger py-0" onclick="return confirm('Xóa?')"><i class="fa-solid fa-trash"></i></a>
                                 {% endif %}
                             </td>
                             {% for col in rf_columns %}<td>{{ row[col] }}</td>{% endfor %}
                         </tr>
                         {% else %}
                         <tr><td colspan="100%" class="text-center py-4 text-muted"><i class="fa-solid fa-magnifying-glass fa-2x mb-2 d-block opacity-50"></i>Không tìm thấy trạm nào.</td></tr>
                         {% endfor %}
                     </tbody>
                 </table>
             </div>

        {% elif active_page == 'import' %}
             <div class="row">
                 <div class="col-md-8">
                     <div class="tab-content bg-white p-4 rounded-3 shadow-sm border">
                         <h5 class="mb-3 text-primary"><i class="fa-solid fa-cloud-arrow-up me-2"></i>Data Import</h5>
                         <ul class="nav nav-tabs mb-4" id="importTabs" role="tablist">
                             <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tabRF" type="button">Import RF</button></li>
                             <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabPOI" type="button">Import POI</button></li>
                             <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tabKPI" type="button">Import KPI</button></li>
                             <li class="nav-item"><button class="nav-link text-primary fw-bold" data-bs-toggle="tab" data-bs-target="#tabQoE" type="button">Import QoE/QoS</button></li>
                             <li class="nav-item"><button class="nav-link text-danger fw-bold" data-bs-toggle="tab" data-bs-target="#tabReset" type="button">Reset Data</button></li>
                         </ul>
                         <div class="tab-content">
                             <div class="tab-pane fade show active" id="tabRF">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu RF</label><select name="type" class="form-select"><option value="3g">RF 3G</option><option value="4g">RF 4G</option><option value="5g">RF 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload RF Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabPOI">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu POI</label><select name="type" class="form-select"><option value="poi4g">POI 4G</option><option value="poi5g">POI 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload POI Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabKPI">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn Loại Dữ Liệu KPI</label><select name="type" class="form-select"><option value="kpi3g">KPI 3G</option><option value="kpi4g">KPI 4G</option><option value="kpi5g">KPI 5G</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload KPI Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabQoE">
                                 <form action="/import" method="POST" enctype="multipart/form-data">
                                     <div class="mb-3"><label class="form-label fw-bold text-primary">Chọn Loại Dữ Liệu QoE/QoS</label><select name="type" class="form-select border-primary"><option value="qoe4g">QoE 4G (Hàng Tuần)</option><option value="qos4g">QoS 4G (Hàng Tuần)</option></select></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Tên Tuần (Quan trọng)</label><input type="text" name="week_name" class="form-control" value="{{ default_week_name }}" required></div>
                                     <div class="mb-3"><label class="form-label fw-bold">Chọn File (.xlsx, .csv)</label><input type="file" name="file" class="form-control" multiple required></div>
                                     <button class="btn btn-primary w-100"><i class="fa-solid fa-upload me-2"></i>Upload Data</button>
                                 </form>
                             </div>
                             <div class="tab-pane fade" id="tabReset">
                                 <div class="alert alert-warning border-0 shadow-sm mb-4">
                                     <i class="fa-solid fa-triangle-exclamation me-2"></i><strong>Cảnh báo:</strong> Hành động này sẽ xóa sạch dữ liệu khỏi cơ sở dữ liệu. Không thể hoàn tác!
                                 </div>
                                 <div class="d-flex flex-column gap-3">
                                     <form action="/reset-data" method="POST" onsubmit="return confirm('CẢNH BÁO TỐI KHẨN: Bạn có CHẮC CHẮN muốn xóa sạch toàn bộ dữ liệu cấu hình RF của 3G, 4G, 5G?');">
                                         <input type="hidden" name="target" value="rf">
                                         <button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu RF</button>
                                     </form>
                                     <form action="/reset-data" method="POST" onsubmit="return confirm('CẢNH BÁO TỐI KHẨN: Bạn có CHẮC CHẮN muốn xóa sạch toàn bộ dữ liệu địa điểm POI của 4G, 5G?');">
                                         <input type="hidden" name="target" value="poi">
                                         <button type="submit" class="btn btn-danger w-100 shadow-sm fw-bold"><i class="fa-solid fa-trash-can me-2"></i>Reset Toàn Bộ Dữ Liệu POI</button>
                                     </form>
                                 </div>
                             </div>
                         </div>
                     </div>
                 </div>
                 <div class="col-md-4">
                     <div class="card h-100 border-0 shadow-sm"><div class="card-header bg-white fw-bold text-success border-bottom">Data History</div><div class="card-body p-0 overflow-auto" style="max-height: 400px;"><table class="table table-sm table-striped mb-0 text-center"><thead class="table-light sticky-top"><tr><th>3G</th><th>4G</th><th>5G</th></tr></thead><tbody>{% for r3, r4, r5 in kpi_rows %}<tr><td>{{ r3 or '-' }}</td><td>{{ r4 or '-' }}</td><td>{{ r5 or '-' }}</td></tr>{% endfor %}</tbody></table></div></div>
                 </div>
             </div>
        
        {% elif active_page == 'script' %}
             <div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Generate Script</div><div class="card-body">
                <ul class="nav nav-tabs mb-3" id="scriptTabs" role="tablist">
                    <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab3g900" type="button">3G 900</button></li>
                    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab4g" type="button">4G</button></li>
                    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab3g2100" type="button">3G 2100</button></li>
                </ul>
                <div class="tab-content">
                    <div class="tab-pane fade show active" id="tab3g900">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="3g900"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g900"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="70"></td><td><input type="number" name="hsn[]" class="form-control" value="2"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g900')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                    </div>
                     <div class="tab-pane fade" id="tab4g">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="4g"><div class="table-responsive"><table class="table table-bordered" id="rruTable_4g"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="60"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="4"></td><td><input type="number" name="txnum[]" class="form-control" value="4"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('4g')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                     </div>
                     <div class="tab-pane fade" id="tab3g2100">
                        <form method="POST" action="/script"><input type="hidden" name="tech" value="3g2100"><div class="table-responsive"><table class="table table-bordered" id="rruTable_3g2100"><thead class="table-light"><tr><th>RRU Name</th><th>SRN</th><th>Slot</th><th>Port</th><th>RCN</th><th>SectorID</th><th>RX</th><th>TX</th><th>Action</th></tr></thead><tbody><tr><td><input type="text" name="rn[]" class="form-control" value="RRU1"></td><td><input type="number" name="srn[]" class="form-control" value="80"></td><td><input type="number" name="hsn[]" class="form-control" value="3"></td><td><input type="number" name="hpn[]" class="form-control" value="0"></td><td><input type="number" name="rcn[]" class="form-control" value="0"></td><td><input type="number" name="sectorid[]" class="form-control" value="0"></td><td><input type="number" name="rxnum[]" class="form-control" value="2"></td><td><input type="number" name="txnum[]" class="form-control" value="1"></td><td><button type="button" class="btn btn-danger btn-sm" onclick="this.closest('tr').remove()">X</button></td></tr></tbody></table></div><button type="button" class="btn btn-success mb-3" onclick="addRow('3g2100')">+ Add RRU</button><br><button class="btn btn-primary shadow-sm">Generate Script</button></form>
                     </div>
                </div>
                {% if script_result %}
                <div class="mt-4"><h5 class="fw-bold text-primary">Result:</h5><textarea class="form-control font-monospace bg-light border-0" rows="12" readonly>{{ script_result }}</textarea></div>
                {% endif %}
             </div></div>

        {% endif %}
    </div>
</div>
{% endblock %}
"""

USER_MANAGEMENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row">
    <div class="col-md-4"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Add User</div><div class="card-body"><form method="POST" action="/users/add"><input name="username" class="form-control mb-2" placeholder="Username" required><input name="password" type="password" class="form-control mb-2" placeholder="Password" required><select name="role" class="form-select mb-3"><option value="user">User</option><option value="admin">Admin</option></select><button class="btn btn-success w-100 shadow-sm">Create</button></form></div></div></div>
    <div class="col-md-8"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Users</div><div class="table-responsive"><table class="table table-hover mb-0"><thead class="table-light"><tr><th>ID</th><th>User</th><th>Role</th><th>Action</th></tr></thead><tbody>{% for u in users %}<tr><td>{{ u.id }}</td><td class="fw-bold">{{ u.username }}</td><td><span class="badge bg-secondary">{{ u.role }}</span></td><td>{% if u.username!='admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-danger shadow-sm">Del</a>{% endif %}</td></tr>{% endfor %}</tbody></table></div></div></div>
</div>
{% endblock %}
"""

PROFILE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row justify-content-center"><div class="col-md-6"><div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">Change Password</div><div class="card-body"><form method="POST" action="/change-password"><input type="password" name="current_password" class="form-control mb-3" placeholder="Current Password" required><input type="password" name="new_password" class="form-control mb-3" placeholder="New Password" required><button class="btn btn-primary w-100 shadow-sm">Save Changes</button></form></div></div></div></div>
{% endblock %}
"""

BACKUP_RESTORE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="container py-4">
    <div class="row g-4">
        <div class="col-md-6">
            <div class="card h-100 shadow-sm border-primary">
                <div class="card-header bg-primary text-white"><h5 class="mb-0"><i class="fa-solid fa-download me-2"></i>Backup Database</h5></div>
                <div class="card-body">
                    <form action="/backup" method="POST">
                        <div class="mb-3"><label class="form-label fw-bold">Select Tables to Backup:</label>
                        <div class="form-check"><input class="form-check-input" type="checkbox" id="selectAll" onclick="toggleCheckboxes(this)"><label class="form-check-label fw-bold" for="selectAll">Select All</label></div><hr class="my-2">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="users.csv"> Users</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="rf3g.csv"> RF 3G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="rf4g.csv"> RF 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="rf5g.csv"> RF 5G</div>
                            </div>
                            <div class="col-md-6">
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="poi4g.csv"> POI 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="poi5g.csv"> POI 5G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="kpi3g.csv"> KPI 3G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="kpi4g.csv"> KPI 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="kpi5g.csv"> KPI 5G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="qoe_4g.csv"> QoE 4G</div>
                                <div class="form-check"><input class="form-check-input" type="checkbox" name="tables" value="qos_4g.csv"> QoS 4G</div>
                            </div>
                        </div></div>
                        <button type="submit" class="btn btn-primary w-100 shadow-sm"><i class="fa-solid fa-file-zipper me-2"></i>Download Selected</button>
                    </form>
                </div>
            </div>
        </div>
        <div class="col-md-6">
            <div class="card h-100 shadow-sm border-warning">
                <div class="card-header bg-warning text-dark"><h5 class="mb-0"><i class="fa-solid fa-upload me-2"></i>Restore Database</h5></div>
                <div class="card-body">
                    <div class="alert alert-danger border-0 shadow-sm"><i class="fa-solid fa-triangle-exclamation me-2"></i><strong>WARNING:</strong> This will OVERWRITE existing data for the tables found in the zip file.</div>
                    <form action="/restore" method="POST" enctype="multipart/form-data">
                        <div class="mb-3"><label class="form-label fw-bold">Select Backup File (.zip)</label><input class="form-control border-0 shadow-sm" type="file" name="file" accept=".zip" required></div>
                        <button type="submit" class="btn btn-warning w-100 shadow-sm" onclick="return confirm('Are you sure you want to restore? This action cannot be undone.')"><i class="fa-solid fa-rotate-left me-2"></i>Restore Data</button>
                    </form>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

RF_FORM_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold">{{ title }}</div><div class="card-body"><form method="POST"><div class="row">{% for col in columns %}<div class="col-md-4 mb-3"><label class="small text-muted fw-bold">{{ col }}</label><input type="text" name="{{ col }}" class="form-control" value="{{ obj[col] if obj else '' }}"></div>{% endfor %}</div><button type="submit" class="btn btn-primary shadow-sm">Save</button><a href="/rf?tech={{ tech }}" class="btn btn-secondary shadow-sm ms-2">Cancel</a></form></div></div>
{% endblock %}
"""

RF_DETAIL_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card border-0 shadow-sm"><div class="card-header bg-white fw-bold d-flex justify-content-between align-items-center"><span>Detail</span><a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm shadow-sm">Quay lại</a></div><div class="card-body p-0 table-responsive"><table class="table table-bordered mb-0 table-striped">{% for k,v in obj.items() %}<tr><th class="w-25 text-end text-muted">{{ k }}</th><td class="fw-bold">{{ v }}</td></tr>{% endfor %}</table></div></div>
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
# 5. ROUTES
# ==============================================================================

def send_telegram_message(chat_id, text_content):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": chat_id, "text": text_content, "parse_mode": "HTML"})

def send_telegram_photo(chat_id, photo_url, caption=""):
    if not TELEGRAM_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    requests.post(url, json={"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"})

def process_bot_command(text):
    text = str(text).strip().upper()
    parts = text.split()
    if not parts: return "🤖 <b>Lỗi cú pháp!</b> Gõ <code>HELP</code> để xem hướng dẫn."
        
    cmd = parts[0]
    
    # 0. Lệnh HELP: Hiển thị hướng dẫn sử dụng
    if cmd == 'HELP':
        return """🤖 <b>HƯỚNG DẪN SỬ DỤNG BOT TRA CỨU NETOPS</b>
Vui lòng gõ theo các cú pháp sau (không phân biệt hoa/thường):

👉 <code>DASHBOARD</code>: Tổng quan mạng 4G toàn hệ thống (7 ngày).
👉 <code>KPI [Mã_Cell]</code>: Tra cứu thông số KPI ngày mới nhất (VD: KPI THA001_1).
👉 <code>CHARTKPI [Mã_Cell]</code>: Tra cứu biểu đồ KPI 7 ngày gần nhất.
👉 <code>RF [Mã_Cell]</code>: Tra cứu tất cả thông số cấu hình trạm.
👉 <code>CTS [Mã_Cell]</code>: Tra cứu thông số QoE, QoS tuần mới nhất.
👉 <code>CHARTCTS [Mã_Cell]</code>: Tra cứu biểu đồ QoE, QoS 4 tuần mới nhất.

<i>*Lưu ý: Mặc định tra cứu mạng 4G. Có thể thêm 3G/5G vào giữa câu lệnh (VD: KPI 3G THA001).</i>"""
    
    with app.app_context():
        # 1. Lệnh DASHBOARD: Trả về 4 biểu đồ tổng quan mạng 4G
        if cmd == 'DASHBOARD':
            records = db.session.query(
                KPI4G.thoi_gian,
                func.sum(KPI4G.traffic).label('traffic'),
                func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput'),
                func.avg(KPI4G.res_blk_dl).label('res_blk_dl'),
                func.avg(KPI4G.cqi_4g).label('cqi_4g')
            ).group_by(KPI4G.thoi_gian).order_by(KPI4G.thoi_gian.desc()).limit(7).all()

            if not records:
                return "❌ Chưa có dữ liệu hệ thống 4G."

            records.reverse()
            labels = [r[0] for r in records if r[0]]

            def create_dash_url(label, data, color, title):
                cfg = {
                    "type": "line",
                    "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]},
                    "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}
                }
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"

            charts_to_send = []
            metrics = [
                ("Total Traffic (GB)", [round(r[1] or 0, 2) for r in records], "#0078d4", "Tổng Traffic 4G (7 Ngày)"),
                ("Avg Thput (Mbps)", [round(r[2] or 0, 2) for r in records], "#107c10", "Trung bình Tốc độ DL (7 Ngày)"),
                ("Avg PRB (%)", [round(r[3] or 0, 2) for r in records], "#ffaa44", "Trung bình Tải PRB (7 Ngày)"),
                ("Avg CQI", [round(r[4] or 0, 2) for r in records], "#00bcf2", "Trung bình CQI 4G (7 Ngày)")
            ]

            for label, data, color, title in metrics:
                charts_to_send.append({
                    "type": "photo",
                    "url": create_dash_url(label, data, color, title),
                    "caption": f"📈 <b>{title}</b> toàn mạng."
                })
            return charts_to_send

        # Bắt lỗi thiếu tham số cho các lệnh cần Mã Cell
        if len(parts) < 2:
            return "🤖 <b>Lỗi cú pháp!</b> Vui lòng nhập đúng mẫu. (VD: <code>KPI THA001</code>)"

        target = parts[-1] # Lấy mã cell (từ cuối cùng)
        
        # 2. Lệnh CTS: Tra cứu Điểm số QoE/QoS tuần mới nhất
        if cmd == 'CTS':
            qoe = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).first()
            qos = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{target}%")).order_by(QoS4G.id.desc()).first()

            if not qoe and not qos:
                return f"❌ Không tìm thấy dữ liệu QoE/QoS cho Cell: <b>{target}</b>"

            msg = f"🌟 <b>THÔNG SỐ QoE / QoS - {target}</b>\n\n"
            if qoe:
                msg += f"📅 <b>{qoe.week_name}</b>\n- Điểm QoE: {qoe.qoe_score} ⭐\n- Tỷ lệ QoE: {qoe.qoe_percent} %\n\n"
            if qos:
                if not qoe or qoe.week_name != qos.week_name:
                    msg += f"📅 <b>{qos.week_name}</b>\n"
                msg += f"- Điểm QoS: {qos.qos_score} ⭐\n- Tỷ lệ QoS: {qos.qos_percent} %\n"
            return msg

        # 3. Lệnh CHARTCTS: Biểu đồ QoE/QoS 4 tuần gần nhất
        if cmd == 'CHARTCTS':
            qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{target}%")).order_by(QoE4G.id.desc()).limit(4).all()
            qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{target}%")).order_by(QoS4G.id.desc()).limit(4).all()

            if not qoe_records and not qos_records:
                return f"❌ Không tìm thấy dữ liệu QoE/QoS cho Cell: <b>{target}</b>"

            # Gom nhóm tuần và sắp xếp từ cũ tới mới
            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))
            all_weeks = all_weeks[-4:]

            def create_cts_url(label, data, color, title):
                cfg = {
                    "type": "line",
                    "data": {"labels": all_weeks, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]},
                    "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}
                }
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(cfg))}&w=600&h=350&bkg=white"

            charts_to_send = []
            c_name = (qoe_records[0].cell_name if qoe_records else qos_records[0].cell_name)

            if qoe_records:
                qmap = {r.week_name: r.qoe_score for r in qoe_records}
                pmap = {r.week_name: r.qoe_percent for r in qoe_records}
                charts_to_send.append({"type": "photo", "url": create_cts_url("Điểm QoE", [qmap.get(w, 0) for w in all_weeks], "#0078d4", f"Điểm QoE (4 Tuần) - {c_name}"), "caption": f"📈 Điểm QoE của {c_name}"})
                charts_to_send.append({"type": "photo", "url": create_cts_url("% QoE", [pmap.get(w, 0) for w in all_weeks], "#107c10", f"% QoE (4 Tuần) - {c_name}"), "caption": f"📈 Tỷ lệ % QoE của {c_name}"})

            if qos_records:
                qmap = {r.week_name: r.qos_score for r in qos_records}
                pmap = {r.week_name: r.qos_percent for r in qos_records}
                charts_to_send.append({"type": "photo", "url": create_cts_url("Điểm QoS", [qmap.get(w, 0) for w in all_weeks], "#ffaa44", f"Điểm QoS (4 Tuần) - {c_name}"), "caption": f"📈 Điểm QoS của {c_name}"})
                charts_to_send.append({"type": "photo", "url": create_cts_url("% QoS", [pmap.get(w, 0) for w in all_weeks], "#e3008c", f"% QoS (4 Tuần) - {c_name}"), "caption": f"📈 Tỷ lệ % QoS của {c_name}"})

            return charts_to_send

        # 4. Các lệnh truyền thống: KPI, RF, CHARTKPI
        tech = '4g' # Mặc định công nghệ 4G nếu người dùng không gõ
        if len(parts) >= 3 and parts[1].lower() in ['3g', '4g', '5g']:
            tech = parts[1].lower()

        if cmd == 'KPI':
            Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
            if not Model: return "❌ Công nghệ không hợp lệ"
            record = Model.query.filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).first()
            if record:
                if tech == '4g': return f"📊 <b>KPI 4G - {record.ten_cell}</b>\n📅 Ngày: {record.thoi_gian}\n- Traffic: {record.traffic} GB\n- Avg Thput: {record.user_dl_avg_thput} Mbps\n- PRB: {record.res_blk_dl}%\n- CQI: {record.cqi_4g}\n- Drop Rate: {record.service_drop_all}%"
                elif tech == '3g': return f"📊 <b>KPI 3G - {record.ten_cell}</b>\n📅 Ngày: {record.thoi_gian}\n- CS Traffic: {record.traffic} Erl\n- PS Traffic: {record.pstraffic} GB\n- CS Conges: {record.csconges}%\n- PS Conges: {record.psconges}%"
                else: return f"📊 <b>KPI 5G - {record.ten_cell}</b>\n📅 Ngày: {record.thoi_gian}\n- Traffic: {record.traffic} GB\n- Avg Thput: {record.user_dl_avg_throughput} Mbps\n- CQI 5G: {record.cqi_5g}"
            return f"❌ Không tìm thấy dữ liệu KPI cho Cell: <b>{target}</b>"
            
        elif cmd == 'RF':
            Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
            if not Model: return "❌ Công nghệ không hợp lệ"
            record = Model.query.filter(Model.cell_code.ilike(f"%{target}%")).first()
            if record:
                if tech == '4g': return f"📡 <b>RF 4G - {record.cell_code}</b>\n📍 Trạm: {record.site_code}\n- Tọa độ: {record.latitude}, {record.longitude}\n- Azimuth: {record.azimuth}\n- Tilt: {record.total_tilt}\n- Tần số: {record.frequency}\n- ENodeB: {record.enodeb_id}\n- LCRID: {record.lcrid}"
                elif tech == '3g': return f"📡 <b>RF 3G - {record.cell_code}</b>\n📍 Trạm: {record.site_code}\n- Tọa độ: {record.latitude}, {record.longitude}\n- Azimuth: {record.azimuth}\n- Tần số: {record.frequency}\n- BSC_LAC: {record.bsc_lac}\n- CI: {record.ci}"
                elif tech == '5g': return f"📡 <b>RF 5G - {record.cell_code}</b>\n📍 Trạm: {record.site_code}\n- Tọa độ: {record.latitude}, {record.longitude}\n- Azimuth: {record.azimuth}\n- Tần số: {record.frequency}\n- GNodeB: {record.gnodeb_id}\n- LCRID: {record.lcrid}"
            return f"❌ Không tìm thấy cấu hình RF cho Cell: <b>{target}</b>"
            
        elif cmd in ['CHARTKPI', 'CHART', 'BIEUDO']:
            Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(tech)
            if not Model: return "❌ Công nghệ không hợp lệ"
            records = db.session.query(Model).filter(Model.ten_cell.ilike(f"%{target}%")).order_by(Model.id.desc()).limit(7).all()
            if not records: return f"❌ Không tìm thấy dữ liệu KPI cho Cell: <b>{target}</b>"
            
            records.reverse()
            labels = [r.thoi_gian for r in records if r.thoi_gian]
            charts_to_send = []
            
            def create_chart_url(label, data, color, title):
                chart_config = {
                    "type": "line",
                    "data": {"labels": labels, "datasets": [{"label": label, "data": data, "borderColor": color, "backgroundColor": "transparent", "borderWidth": 3}]},
                    "options": {"title": {"display": True, "text": title, "fontSize": 16}, "elements": {"line": {"tension": 0.3}}}
                }
                return f"https://quickchart.io/chart?c={urllib.parse.quote(json.dumps(chart_config))}&w=600&h=350&bkg=white"

            cell_name = records[0].ten_cell
            if tech == '4g':
                kpis = [("Traffic (GB)", [r.traffic or 0 for r in records], "#0078d4"), ("Avg Thput (Mbps)", [r.user_dl_avg_thput or 0 for r in records], "#107c10"), ("PRB DL (%)", [r.res_blk_dl or 0 for r in records], "#ffaa44"), ("CQI", [r.cqi_4g or 0 for r in records], "#00bcf2")]
            elif tech == '3g':
                kpis = [("CS Traffic (Erl)", [r.traffic or 0 for r in records], "#0078d4"), ("PS Traffic (GB)", [r.pstraffic or 0 for r in records], "#107c10"), ("CS Congestion (%)", [r.csconges or 0 for r in records], "#d13438"), ("PS Congestion (%)", [r.psconges or 0 for r in records], "#e3008c")]
            else:
                kpis = [("Traffic (GB)", [r.traffic or 0 for r in records], "#0078d4"), ("Avg Thput (Mbps)", [r.user_dl_avg_throughput or 0 for r in records], "#107c10"), ("CQI 5G", [r.cqi_5g or 0 for r in records], "#00bcf2")]
                
            for label, data, color in kpis:
                charts_to_send.append({"type": "photo", "url": create_chart_url(label, data, color, f"Biểu đồ {label} 7 Ngày - {cell_name}"), "caption": f"📈 <b>{label}</b> của {cell_name}"})
            return charts_to_send
            
    return "🤖 Cú pháp không được hỗ trợ. Gõ <code>HELP</code> để xem hướng dẫn."

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    data = request.json
    if data and 'message' in data:
        chat_id = data['message']['chat']['id']
        text = data['message'].get('text', '')
        if text:
            reply_data = process_bot_command(text)
            if isinstance(reply_data, list):
                for item in reply_data:
                    if item.get('type') == 'photo': send_telegram_photo(chat_id, item['url'], item.get('caption', ''))
            elif isinstance(reply_data, dict) and reply_data.get('type') == 'photo':
                send_telegram_photo(chat_id, reply_data['url'], reply_data.get('caption', ''))
            else:
                send_telegram_message(chat_id, str(reply_data))
    return jsonify({"status": "success"}), 200

@app.route('/telegram/set_webhook')
def set_telegram_webhook():
    if not TELEGRAM_BOT_TOKEN: return "Missing Bot Token", 400
    webhook_url = request.host_url.rstrip('/') + url_for('telegram_webhook')
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook?url={webhook_url}")
        return jsonify(r.json())
    except Exception as e: return str(e), 500

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
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    dashboard_data = {'labels': [], 'traffic': [], 'thput': [], 'prb': [], 'cqi': []}
    try:
        records = db.session.query(KPI4G.thoi_gian, func.sum(KPI4G.traffic).label('traffic'), func.avg(KPI4G.user_dl_avg_thput).label('user_dl_avg_thput'), func.avg(KPI4G.res_blk_dl).label('res_blk_dl'), func.avg(KPI4G.cqi_4g).label('cqi_4g')).group_by(KPI4G.thoi_gian).all()
        if records:
            agg_data = {}
            for r in records:
                if not r[0]: continue
                agg_data[r[0]] = {'traffic_sum': r[1] or 0, 'thput_avg': r[2] or 0, 'prb_avg': r[3] or 0, 'cqi_avg': r[4] or 0}
            sorted_dates = sorted(agg_data.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
            dashboard_data['labels'] = sorted_dates
            for d in sorted_dates:
                dashboard_data['traffic'].append(round(agg_data[d]['traffic_sum'], 2))
                dashboard_data['thput'].append(round(agg_data[d]['thput_avg'], 2))
                dashboard_data['prb'].append(round(agg_data[d]['prb_avg'], 2))
                dashboard_data['cqi'].append(round(agg_data[d]['cqi_avg'], 2))
    except Exception as e: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard', dashboard_data=dashboard_data)

@app.route('/azimuth')
@login_required
def azimuth():
    return render_page(CONTENT_TEMPLATE, title="Tính toán và Vẽ Azimuth", active_page='azimuth')

@app.route('/optimize')
@login_required
def optimize():
    action = request.args.get('action')
    qoe_weeks = [r[0] for r in db.session.query(QoE4G.week_name).distinct().all()]
    qos_weeks = [r[0] for r in db.session.query(QoS4G.week_name).distinct().all()]
    all_weeks = sorted(list(set([w for w in qoe_weeks + qos_weeks if w])), reverse=True)
    
    selected_week = request.args.get('week_name')
    if not selected_week and all_weeks:
        selected_week = all_weeks[0]
    
    bad_cells_dict = {}
    
    if selected_week:
        l900_cells = {c[0] for c in db.session.query(RF4G.cell_code).filter(RF4G.frequency.ilike('%L900%')).all()}

        qoe_bad = QoE4G.query.filter((QoE4G.week_name == selected_week) & ((QoE4G.qoe_score <= 2) | (QoE4G.qoe_percent < 80))).all()
        qos_bad = QoS4G.query.filter((QoS4G.week_name == selected_week) & ((QoS4G.qos_score <= 3) | (QoS4G.qos_percent < 90))).all()
        
        # Hàm kiểm tra nội bộ để khử dòng rác
        def is_trash(c_name):
            c_str = str(c_name).strip().upper()
            if not c_str or c_str in ['NAN', 'NONE', 'NULL']: return True
            if len(c_str) < 5: return True
            if c_str.replace('.', '', 1).isdigit(): return True # Chặn '6', '6.0', '123'
            if c_str in l900_cells: return True
            if c_str.startswith('VNP-4G') or c_str.startswith('MBF_TH'): return True
            return False

        for r in qoe_bad:
            if is_trash(r.cell_name): continue
            bad_cells_dict[r.cell_name] = {'qoe_score': r.qoe_score, 'qoe_percent': r.qoe_percent, 'qos_score': '-', 'qos_percent': '-'}
            
        for r in qos_bad:
            if is_trash(r.cell_name): continue
            if r.cell_name not in bad_cells_dict:
                bad_cells_dict[r.cell_name] = {'qoe_score': '-', 'qoe_percent': '-', 'qos_score': r.qos_score, 'qos_percent': r.qos_percent}
            else:
                bad_cells_dict[r.cell_name]['qos_score'] = r.qos_score
                bad_cells_dict[r.cell_name]['qos_percent'] = r.qos_percent
        
        if bad_cells_dict:
            cell_names = list(bad_cells_dict.keys())
            
            latest_dates = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).limit(3).all()]
            
            if latest_dates:
                kpi_records = db.session.query(
                    KPI4G.ten_cell,
                    func.avg(KPI4G.res_blk_dl).label('avg_prb'),
                    func.avg(KPI4G.user_dl_avg_thput).label('avg_thput'),
                    func.avg(KPI4G.cqi_4g).label('avg_cqi'),
                    func.avg(KPI4G.service_drop_all).label('avg_drop')
                ).filter(
                    KPI4G.ten_cell.in_(cell_names),
                    KPI4G.thoi_gian.in_(latest_dates)
                ).group_by(KPI4G.ten_cell).all()
                
                for r in kpi_records:
                    c = r.ten_cell
                    if c in bad_cells_dict:
                        prb = r.avg_prb or 0
                        thput = r.avg_thput or 0
                        cqi = r.avg_cqi or 0
                        drop = r.avg_drop or 0
                        
                        issues = []
                        actions = []
                        
                        if prb > 20 and thput < 10:
                            issues.append("Nghẽn (Congestion)")
                            actions.append("Cân bằng tải L1800->L2100 / Thêm Carrier")
                        if cqi < 93:
                            issues.append("Vô tuyến kém / Nhiễu")
                            actions.append("Chỉnh Tx Power / Tối ưu Tilt, Azimuth")
                        if drop > 0.3 and prb <= 20:
                            issues.append("Lỗi Thiết bị / Truyền dẫn")
                            actions.append("NOC reset Card / UCTT đo kiểm Quang, VSWR")
                            
                        if not issues:
                            issues.append("Chưa rõ nguyên nhân")
                            actions.append("Theo dõi sâu / Phân tích tham số")
                            
                        bad_cells_dict[c].update({
                            'prb': round(prb, 2),
                            'thput': round(thput, 2),
                            'cqi': round(cqi, 2),
                            'drop': round(drop, 2),
                            'issues': issues,
                            'actions': actions
                        })
                    
    optimized_data = []
    for cell, data in bad_cells_dict.items():
        data['cell_name'] = cell
        if 'issues' not in data:
             data.update({'prb': '-', 'thput': '-', 'cqi': '-', 'drop': '-', 'issues': ['Thiếu dữ liệu KPI ngày'], 'actions': ['Cần Import KPI']})
        optimized_data.append(data)
        
    if action == 'export':
        export_list = []
        for data in optimized_data:
            export_list.append({
                'Cell Name': data.get('cell_name', ''),
                'QoE Score': data.get('qoe_score', ''),
                'QoE %': data.get('qoe_percent', ''),
                'QoS Score': data.get('qos_score', ''),
                'QoS %': data.get('qos_percent', ''),
                'PRB (%)': data.get('prb', ''),
                'Thput (Mbps)': data.get('thput', ''),
                'CQI (%)': data.get('cqi', ''),
                'Drop (%)': data.get('drop', ''),
                'Chẩn đoán': " | ".join(data.get('issues', [])),
                'Giải pháp': " | ".join(data.get('actions', []))
            })
        df = pd.DataFrame(export_list)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Toi_Uu')
        output.seek(0)
        safe_week_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', selected_week) if selected_week else 'Week'
        return send_file(output, download_name=f'ToiUu_{safe_week_name}.xlsx', as_attachment=True)
        
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Tối ưu QoE/QoS (NPO)", active_page='optimize', optimized_data=optimized_data, latest_week=selected_week, all_weeks=all_weeks)

@app.route('/gis', methods=['GET', 'POST'])
@login_required
def gis():
    action_type = request.form.get('action', 'search') if request.method == 'POST' else 'search'
    tech = request.form.get('tech', '4g') if request.method == 'POST' else request.args.get('tech', '4g')
    site_code_input = request.form.get('site_code', '').strip() if request.method == 'POST' else request.args.get('site_code', '').strip()
    cell_name_input = request.form.get('cell_name', '').strip() if request.method == 'POST' else request.args.get('cell_name', '').strip()
    
    show_its = False
    its_data = []
    matched_sites = set()
    gis_data = []

    def clean_val(v):
        if v is None: return None
        s = str(v).strip()
        if s == '-' or s == '' or s.lower() in ['nan', 'null', 'none']: return None
        try:
            f = float(s)
            if f.is_integer(): return str(int(f))
            return str(f)
        except ValueError: return s.upper()

    def safe_float(val, default=0.0):
        if val is None: return default
        s = str(val).strip()
        if not s or s == '-': return default
        try: return float(s)
        except ValueError: return default

    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db_mapping = {}
    
    if Model and action_type == 'show_log':
        if tech == '4g' and hasattr(Model, 'enodeb_id') and hasattr(Model, 'lcrid'):
            res = db.session.query(Model.site_code, Model.enodeb_id, Model.lcrid).all()
            for sc, en, lc in res:
                c_en, c_lc = clean_val(en), clean_val(lc)
                if sc and c_en and c_lc: db_mapping[f"{c_en}_{c_lc}"] = sc
        elif tech == '3g' and hasattr(Model, 'ci'):
            res = db.session.query(Model.site_code, Model.ci).all()
            for sc, ci in res:
                c_ci = clean_val(ci)
                if sc and c_ci: db_mapping[c_ci] = sc
        elif tech == '5g' and hasattr(Model, 'gnodeb_id') and hasattr(Model, 'lcrid'):
            res = db.session.query(Model.site_code, Model.gnodeb_id, Model.lcrid).all()
            for sc, gn, lc in res:
                c_gn, c_lc = clean_val(gn), clean_val(lc)
                if sc and c_gn and c_lc: db_mapping[f"{c_gn}_{c_lc}"] = sc
    
    if request.method == 'POST' and 'its_file' in request.files:
        files = request.files.getlist('its_file')
        for file in files:
            if file and file.filename:
                show_its = True
                try:
                    file_bytes = file.read()
                    if not file_bytes:
                        flash(f'Lỗi: File {file.filename} tải lên trống.', 'danger')
                        continue
                    
                    content = file_bytes.decode('utf-8-sig', errors='ignore')
                    lines = content.splitlines()
                    if len(lines) > 1:
                        header_line = lines[0]
                        sep = '|' if '|' in header_line else (',' if ',' in header_line else '\t')
                        headers = [h.strip().lower() for h in header_line.split(sep)]
                        
                        try:
                            lat_idx = next(i for i, h in enumerate(headers) if h in ['latitude', 'lat'])
                            lon_idx = next(i for i, h in enumerate(headers) if h in ['longitude', 'lon', 'long'])
                        except StopIteration:
                            lat_idx, lon_idx = -1, -1
                            
                        node_idx = next((i for i, h in enumerate(headers) if h in ['node', 'enodebid', 'enodeb_id']), -1)
                        cell_idx = next((i for i, h in enumerate(headers) if h in ['cellid', 'ci', 'cell_id']), -1)
                        level_idx = next((i for i, h in enumerate(headers) if h in ['level', 'rsrp', 'rscp', 'rxlev']), -1)
                        tech_idx = next((i for i, h in enumerate(headers) if h in ['networktech', 'tech', 'network_tech']), -1)
                        qual_idx = next((i for i, h in enumerate(headers) if h in ['qual', 'ecno', 'sinr', 'snr', 'rsrq']), -1)

                        if lat_idx == -1 or lon_idx == -1:
                            flash(f'Lỗi: Không tìm thấy cột Tọa độ trong file {file.filename}.', 'danger')
                        else:
                            data_lines = lines[1:]
                            for line in data_lines:
                                if not line.strip(): continue
                                parts = line.split(sep)
                                if len(parts) <= max(lat_idx, lon_idx): continue
                                
                                try:
                                    lat_str, lon_str = parts[lat_idx].strip(), parts[lon_idx].strip()
                                    if not lat_str or lat_str == '-' or not lon_str or lon_str == '-': continue
                                    lat, lon = float(lat_str), float(lon_str)
                                    
                                    n = clean_val(parts[node_idx]) if node_idx >= 0 and len(parts) > node_idx else None
                                    c = clean_val(parts[cell_idx]) if cell_idx >= 0 and len(parts) > cell_idx else None
                                    
                                    if action_type == 'show_log':
                                        if tech == '4g' and n and c:
                                            if f"{n}_{c}" in db_mapping: matched_sites.add(db_mapping[f"{n}_{c}"])
                                        elif tech == '3g' and c:
                                            if c in db_mapping: matched_sites.add(db_mapping[c])
                                        elif tech == '5g' and n and c:
                                            if f"{n}_{c}" in db_mapping: matched_sites.add(db_mapping[f"{n}_{c}"])
                                                
                                    lvl = safe_float(parts[level_idx] if level_idx >= 0 and len(parts) > level_idx else '')
                                    qual_str = parts[qual_idx].strip() if qual_idx >= 0 and len(parts) > qual_idx else ''
                                    tech_str = parts[tech_idx].strip().upper() if tech_idx >= 0 and len(parts) > tech_idx else tech.upper()
                                    
                                    its_data.append({'lat': lat, 'lon': lon, 'level': lvl, 'qual': qual_str, 'tech': tech_str, 'cellid': c or '', 'node': n or ''})
                                except ValueError: pass
                except Exception as e: flash(f'Lỗi xử lý file {file.filename}: {e}', 'danger')
        
        if len(its_data) > 20000:
            its_data = random.sample(its_data, 20000)
            flash(f'Đã giới hạn hiển thị ngẫu nhiên 20,000 điểm đo từ tổng số để chống treo trình duyệt.', 'warning')
            
    if Model:
        if action_type == 'show_log' and show_its:
            query = db.session.query(Model)
            if matched_sites: flash(f'Đã tải {len(its_data)} điểm Log từ các file. Tìm thấy {len(matched_sites)} trạm khớp trong DB.', 'success')
            else: flash('Không có điểm Log nào khớp với trạm trong DB.', 'danger')
            
            if matched_sites: query = query.filter(Model.site_code.in_(list(matched_sites)[:900]))
            else: query = query.filter(text("1=0"))
            records = query.all()
        else:
            records_dict = {}
            target_lat, target_lon = None, None
            found_search = False
            
            if site_code_input or cell_name_input:
                search_q = db.session.query(Model)
                if site_code_input: search_q = search_q.filter(Model.site_code.ilike(f"%{site_code_input}%"))
                if cell_name_input:
                    filters = [Model.cell_code.ilike(f"%{cell_name_input}%")]
                    if hasattr(Model, 'cell_name'): filters.append(Model.cell_name.ilike(f"%{cell_name_input}%"))
                    search_q = search_q.filter(or_(*filters))
                
                for r in search_q.limit(50).all():
                    records_dict[r.id] = r
                    if not found_search and r.latitude and r.longitude:
                        try:
                            target_lat, target_lon = float(r.latitude), float(r.longitude)
                            found_search = True
                        except: pass
            
            if found_search and target_lat and target_lon:
                lat_min, lat_max = target_lat - 0.15, target_lat + 0.15
                lon_min, lon_max = target_lon - 0.15, target_lon + 0.15
                nearby = db.session.query(Model).filter(
                    Model.latitude >= lat_min, Model.latitude <= lat_max,
                    Model.longitude >= lon_min, Model.longitude <= lon_max
                ).limit(2000).all()
                for r in nearby: records_dict[r.id] = r
            else:
                for r in db.session.query(Model).limit(3000).all(): records_dict[r.id] = r
                
            records = list(records_dict.values())

        cols = [c.key for c in Model.__table__.columns if c.key not in ['id']]
        for r in records:
            try:
                lat, lon = float(r.latitude), float(r.longitude)
                azi = int(r.azimuth) if getattr(r, 'azimuth', None) is not None else 0
                if 8 <= lat <= 24 and 102 <= lon <= 110:
                    gis_data.append({'cell_name': getattr(r, 'cell_name', getattr(r, 'site_name', str(r.cell_code))), 'site_code': r.site_code, 'lat': lat, 'lon': lon, 'azi': azi, 'tech': tech, 'info': {c: getattr(r, c) or '' for c in cols}})
            except: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Bản đồ Trực quan (GIS)", active_page='gis', selected_tech=tech, site_code_input=site_code_input, cell_name_input=cell_name_input, gis_data=gis_data, its_data=its_data, show_its=show_its, action_type=action_type)

@app.route('/kpi')
@login_required
def kpi():
    selected_tech = request.args.get('tech', '4g')
    cell_name_input = request.args.get('cell_name', '').strip()
    poi_input = request.args.get('poi_name', '').strip()
    charts = {} 

    colors = generate_colors(20)
    target_cells = []
    KPI_Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)
    RF_Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(selected_tech)

    if poi_input:
        POI_Model = {'4g': POI4G, '5g': POI5G}.get(selected_tech)
        if POI_Model: target_cells = [r.cell_code for r in POI_Model.query.filter(POI_Model.poi_name == poi_input).all()]
    elif cell_name_input:
        if RF_Model:
            matched_rf = RF_Model.query.filter(or_(RF_Model.site_code.ilike(f"%{cell_name_input}%"), RF_Model.cell_code.ilike(f"%{cell_name_input}%"))).all()
            if matched_rf: target_cells.extend([r.cell_code for r in matched_rf])
        if KPI_Model:
            matched_kpi = KPI_Model.query.filter(KPI_Model.ten_cell.ilike(f"%{cell_name_input}%")).with_entities(KPI_Model.ten_cell).distinct().all()
            if matched_kpi: target_cells.extend([r[0] for r in matched_kpi])
        if not target_cells: target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]
            
    if target_cells:
        unique_cells = []
        seen = set()
        for c in target_cells:
            if not c: continue
            c_clean = str(c).strip().upper()
            if c_clean not in seen:
                seen.add(c_clean)
                unique_cells.append(str(c).strip())
        target_cells = unique_cells

    if target_cells and KPI_Model:
        data = KPI_Model.query.filter(KPI_Model.ten_cell.in_(target_cells)).all()
        try: data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
        except: pass 

        if data:
            all_labels = sorted(list(set([x.thoi_gian for x in data])), key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
            data_by_cell = defaultdict(list)
            for x in data: data_by_cell[str(x.ten_cell).strip().upper()].append(x)

            metrics_config = {
                '3g': [{'key': 'pstraffic', 'label': 'PSTRAFFIC (GB)'}, {'key': 'traffic', 'label': 'TRAFFIC (Erl)'}, {'key': 'psconges', 'label': 'PS CONGESTION (%)'}, {'key': 'csconges', 'label': 'CS CONGESTION (%)'}],
                '4g': [{'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'}, {'key': 'user_dl_avg_thput', 'label': 'USER DL AVG THPUT (Mbps)'}, {'key': 'res_blk_dl', 'label': 'RES BLOCK DL (%)'}, {'key': 'cqi_4g', 'label': 'CQI 4G'}],
                '5g': [{'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'}, {'key': 'user_dl_avg_throughput', 'label': 'USER DL AVG THPUT (Mbps)'}, {'key': 'cqi_5g', 'label': 'CQI 5G'}]
            }
            
            current_metrics = metrics_config.get(selected_tech, [])
            for metric in current_metrics:
                datasets = []
                for i, cell_code in enumerate(target_cells):
                    cell_data = data_by_cell.get(cell_code.upper(), [])
                    data_map = {item.thoi_gian: (getattr(item, metric['key'], 0) or 0) for item in cell_data}
                    datasets.append({'label': cell_code, 'data': [data_map.get(lbl, None) for lbl in all_labels], 'borderColor': colors[i % len(colors)], 'fill': False, 'spanGaps': True})
                charts[f"chart_{metric['key']}"] = {'title': metric['label'], 'labels': all_labels, 'datasets': datasets}

    poi_list = []
    with app.app_context():
        try:
            p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
            p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
            poi_list = sorted(list(set(p4 + p5)))
        except: pass

    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi', selected_tech=selected_tech, cell_name_input=cell_name_input, selected_poi=poi_input, poi_list=poi_list, charts=charts)

@app.route('/qoe-qos')
@login_required
def qoe_qos():
    cell_name_input = request.args.get('cell_name', '').strip()
    charts = {}
    has_data = False
    qoe_details, qos_details, qoe_headers, qos_headers = [], [], [], []
    
    if cell_name_input:
        qoe_records = QoE4G.query.filter(QoE4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoE4G.id.asc()).all()
        qos_records = QoS4G.query.filter(QoS4G.cell_name.ilike(f"%{cell_name_input}%")).order_by(QoS4G.id.asc()).all()
        
        if qoe_records or qos_records:
            has_data = True
            all_weeks = sorted(list(set([r.week_name for r in qoe_records] + [r.week_name for r in qos_records])))
            
            if qoe_records:
                qoe_score_map = {r.week_name: (r.qoe_score or 0) for r in qoe_records}
                qoe_percent_map = {r.week_name: (r.qoe_percent or 0) for r in qoe_records}
                charts['qoe_score_chart'] = {'title': 'Biểu đồ Điểm QoE', 'labels': all_weeks, 'datasets': [{'label': 'Điểm QoE (1-5)', 'data': [qoe_score_map.get(w, None) for w in all_weeks], 'borderColor': '#0078d4', 'fill': False, 'borderWidth': 3}]}
                charts['qoe_percent_chart'] = {'title': 'Biểu đồ Tỷ lệ QoE (%)', 'labels': all_weeks, 'datasets': [{'label': '% QoE', 'data': [qoe_percent_map.get(w, None) for w in all_weeks], 'borderColor': '#107c10', 'fill': False, 'borderWidth': 3}]}
                for r in qoe_records:
                    if r.details:
                        try:
                            d = json.loads(r.details)
                            if not qoe_headers: qoe_headers = list(d.keys())
                            qoe_details.append({'week': r.week_name, 'data': d})
                        except: pass
            
            if qos_records:
                qos_score_map = {r.week_name: (r.qos_score or 0) for r in qos_records}
                qos_percent_map = {r.week_name: (r.qos_percent or 0) for r in qos_records}
                charts['qos_score_chart'] = {'title': 'Biểu đồ Điểm QoS', 'labels': all_weeks, 'datasets': [{'label': 'Điểm QoS (1-5)', 'data': [qos_score_map.get(w, None) for w in all_weeks], 'borderColor': '#ffaa44', 'fill': False, 'borderWidth': 3}]}
                charts['qos_percent_chart'] = {'title': 'Biểu đồ Tỷ lệ QoS (%)', 'labels': all_weeks, 'datasets': [{'label': '% QoS', 'data': [qos_percent_map.get(w, None) for w in all_weeks], 'borderColor': '#e3008c', 'fill': False, 'borderWidth': 3}]}
                for r in qos_records:
                    if r.details:
                        try:
                            d = json.loads(r.details)
                            if not qos_headers: qos_headers = list(d.keys())
                            qos_details.append({'week': r.week_name, 'data': d})
                        except: pass
    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="QoE & QoS Analytics", active_page='qoe_qos', cell_name_input=cell_name_input, charts=charts, has_data=has_data, qoe_details=qoe_details, qos_details=qos_details, qoe_headers=qoe_headers, qos_headers=qos_headers)

@app.route('/poi')
@login_required
def poi():
    pname = request.args.get('poi_name', '').strip()
    charts = {}
    pois = []
    try:
        p4 = [r[0] for r in db.session.query(POI4G.poi_name).distinct()]
        p5 = [r[0] for r in db.session.query(POI5G.poi_name).distinct()]
        pois = sorted(list(set(p4 + p5)))
    except: pass
    
    if pname:
        c4 = [r[0] for r in db.session.query(POI4G.cell_code).filter_by(poi_name=pname).all()]
        c5 = [r[0] for r in db.session.query(POI5G.cell_code).filter_by(poi_name=pname).all()]
        
        if c4:
            k4 = db.session.query(KPI4G.thoi_gian, KPI4G.traffic, KPI4G.user_dl_avg_thput).filter(KPI4G.ten_cell.in_(c4)).all()
            if k4:
                try: k4.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
                except: pass
                dates4 = sorted(list(set(x.thoi_gian for x in k4)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                agg_traf, agg_thput = defaultdict(float), defaultdict(list)
                for r in k4:
                    if r.thoi_gian in dates4:
                        agg_traf[r.thoi_gian] += (r.traffic or 0)
                        if r.user_dl_avg_thput is not None: agg_thput[r.thoi_gian].append(r.user_dl_avg_thput)

                ds_traf_agg = [{'label': 'Total 4G Traffic (GB)', 'data': [agg_traf[d] for d in dates4], 'borderColor': 'blue', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]
                ds_thput_agg = [{'label': 'Avg 4G Thput (Mbps)', 'data': [(sum(agg_thput[d])/len(agg_thput[d])) if agg_thput[d] else 0 for d in dates4], 'borderColor': 'green', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]

                charts['4g_traf'] = {'title': 'Total 4G Traffic (GB)', 'labels': dates4, 'datasets': ds_traf_agg}
                charts['4g_thp'] = {'title': 'Avg 4G Thput (Mbps)', 'labels': dates4, 'datasets': ds_thput_agg}
        
        if c5:
            k5 = db.session.query(KPI5G.thoi_gian, KPI5G.traffic, KPI5G.user_dl_avg_throughput).filter(KPI5G.ten_cell.in_(c5)).all()
            if k5:
                try: k5.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
                except: pass
                dates5 = sorted(list(set(x.thoi_gian for x in k5)), key=lambda x: datetime.strptime(x, '%d/%m/%Y'))
                agg_traf5, agg_thput5 = defaultdict(float), defaultdict(list)
                for r in k5:
                    if r.thoi_gian in dates5:
                        agg_traf5[r.thoi_gian] += (r.traffic or 0)
                        if r.user_dl_avg_throughput is not None: agg_thput5[r.thoi_gian].append(r.user_dl_avg_throughput)
                
                ds_traf_agg5 = [{'label': 'Total 5G Traffic (GB)', 'data': [agg_traf5[d] for d in dates5], 'borderColor': 'orange', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]
                ds_thput_agg5 = [{'label': 'Avg 5G Thput (Mbps)', 'data': [(sum(agg_thput5[d])/len(agg_thput5[d])) if agg_thput5[d] else 0 for d in dates5], 'borderColor': 'purple', 'fill': False, 'borderWidth': 3, 'spanGaps': True}]
                
                charts['5g_traf'] = {'title': 'Total 5G Traffic (GB)', 'labels': dates5, 'datasets': ds_traf_agg5}
                charts['5g_thp'] = {'title': 'Avg 5G Thput (Mbps)', 'labels': dates5, 'datasets': ds_thput_agg5}

    gc.collect()
    return render_page(CONTENT_TEMPLATE, title="POI Report", active_page='poi', poi_list=pois, selected_poi=pname, poi_charts=charts)

@app.route('/conges-3g')
@login_required
def conges_3g():
    conges_data, target_dates = [], []
    action = request.args.get('action')
    if action in ['execute', 'export']:
        try:
            all_dates = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().all()]
            date_objs = sorted([datetime.strptime(d, '%d/%m/%Y') for d in all_dates if d], reverse=True)
            if len(date_objs) >= 3:
                target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:3]]
                records = db.session.query(KPI3G.ten_cell, KPI3G.traffic, KPI3G.csconges, KPI3G.pstraffic, KPI3G.psconges).filter(
                    KPI3G.thoi_gian.in_(target_dates),
                    ((KPI3G.csconges > 2) & (KPI3G.cs_so_att > 100)) | ((KPI3G.psconges > 2) & (KPI3G.ps_so_att > 500))
                ).all()
                groups = defaultdict(list)
                for r in records: groups[r.ten_cell].append(r)
                for cell, rows in groups.items():
                    if len(rows) == 3:
                        conges_data.append({
                            'cell_name': cell,
                            'avg_cs_traffic': round(sum(r.traffic or 0 for r in rows)/3, 2),
                            'avg_cs_conges': round(sum(r.csconges or 0 for r in rows)/3, 2),
                            'avg_ps_traffic': round(sum(r.pstraffic or 0 for r in rows)/3, 2),
                            'avg_ps_conges': round(sum(r.psconges or 0 for r in rows)/3, 2)
                        })
        except: pass
    gc.collect()

    if action == 'export':
        df = pd.DataFrame(conges_data)
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False, sheet_name='Congestion 3G')
        output.seek(0)
        return send_file(output, download_name='Congestion3G.xlsx', as_attachment=True)
    return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g', conges_data=conges_data, dates=target_dates)

@app.route('/worst-cell')
@login_required
def worst_cell():
    duration = int(request.args.get('duration', 1))
    action = request.args.get('action')
    all_dates = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().all()]
    date_objs = sorted([datetime.strptime(d, '%d/%m/%Y') for d in all_dates if d], reverse=True)
    target_dates = [d.strftime('%d/%m/%Y') for d in date_objs[:duration]]
    
    results = []
    if target_dates:
        latest_date = target_dates[0]
        l900_cells = {c[0] for c in db.session.query(RF4G.cell_code).filter(RF4G.frequency.ilike('%L900%')).all()}
        active_latest_cells = {c[0] for c in db.session.query(KPI4G.ten_cell).filter(KPI4G.thoi_gian == latest_date).all()}

        records = db.session.query(KPI4G.ten_cell, KPI4G.user_dl_avg_thput, KPI4G.res_blk_dl, KPI4G.cqi_4g, KPI4G.service_drop_all).filter(
            KPI4G.thoi_gian.in_(target_dates),
            ~KPI4G.ten_cell.startswith('MBF_TH'), ~KPI4G.ten_cell.startswith('VNP-4G'),
            ((KPI4G.user_dl_avg_thput < 7000) | (KPI4G.res_blk_dl > 20) | (KPI4G.cqi_4g < 93) | (KPI4G.service_drop_all > 0.3))
        ).all()
    
        groups = defaultdict(list)
        for r in records: 
            if r.ten_cell in active_latest_cells and r.ten_cell not in l900_cells:
                groups[r.ten_cell].append(r)
        
        for cell, rows in groups.items():
            if len(rows) == duration:
                results.append({
                    'cell_name': cell,
                    'avg_thput': round(sum(r.user_dl_avg_thput or 0 for r in rows)/duration, 2),
                    'avg_res_blk': round(sum(r.res_blk_dl or 0 for r in rows)/duration, 2),
                    'avg_cqi': round(sum(r.cqi_4g or 0 for r in rows)/duration, 2),
                    'avg_drop': round(sum(r.service_drop_all or 0 for r in rows)/duration, 2)
                })
                
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
            dates_raw = [d[0] for d in db.session.query(Model.thoi_gian).distinct().all()]
            dates_obj = sorted([datetime.strptime(d, '%d/%m/%Y') for d in dates_raw if d], reverse=True)
            if dates_obj:
                latest = dates_obj[0]
                analysis_date = latest.strftime('%d/%m/%Y')
                needed = [latest] + [latest - timedelta(days=i) for i in range(1, 8)]
                needed_str = [d.strftime('%d/%m/%Y') for d in needed]
                
                records = db.session.query(Model.ten_cell, Model.thoi_gian, Model.traffic).filter(Model.thoi_gian.in_(needed_str)).all()
                data_map = defaultdict(dict)
                for r in records:
                    if r.ten_cell.startswith('MBF_TH') or r.ten_cell.startswith('VNP-4G'): continue
                    try: data_map[r.ten_cell][datetime.strptime(r.thoi_gian, '%d/%m/%Y')] = r.traffic or 0
                    except: pass
                
                last_week = latest - timedelta(days=7)
                for cell, d_map in data_map.items():
                    t0 = d_map.get(latest, 0)
                    t_last = d_map.get(last_week, 0)
                    if t0 < 0.1:
                        avg7 = sum(d_map.get(latest - timedelta(days=i), 0) for i in range(1,8)) / 7
                        if avg7 > 2: zero_traffic.append({'cell_name': cell, 'traffic_today': round(t0,3), 'avg_last_7': round(avg7,3)})
                    if t_last > 1 and t0 < 0.7 * t_last:
                        degraded.append({'cell_name': cell, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})
                
                if POI_Model:
                    poi_map = {r.cell_code: r.poi_name for r in db.session.query(POI_Model).all()}
                    poi_traffic = defaultdict(lambda: {'today': 0, 'last_week': 0})
                    for cell, d_map in data_map.items():
                        if cell in poi_map:
                            p_name = poi_map[cell]
                            poi_traffic[p_name]['today'] += d_map.get(latest, 0)
                            poi_traffic[p_name]['last_week'] += d_map.get(last_week, 0)
                    for pname, traf in poi_traffic.items():
                        t0 = traf['today']
                        t_last = traf['last_week']
                        if t_last > 5 and t0 < 0.7 * t_last:
                             degraded_pois.append({'poi_name': pname, 'traffic_today': round(t0,3), 'traffic_last_week': round(t_last,3), 'degrade_percent': round((1-t0/t_last)*100, 1)})

        gc.collect()

        if action == 'export_zero':
            df = pd.DataFrame(zero_traffic)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f'ZeroTraffic_{tech}.xlsx', as_attachment=True)
        elif action == 'export_degraded':
            df = pd.DataFrame(degraded)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f'DegradedTraffic_{tech}.xlsx', as_attachment=True)
        elif action == 'export_poi_degraded':
            df = pd.DataFrame(degraded_pois)
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, index=False)
            output.seek(0)
            return send_file(output, download_name=f'POIDegraded_{tech}.xlsx', as_attachment=True)

    return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down', zero_traffic=zero_traffic, degraded=degraded, degraded_pois=degraded_pois, tech=tech, analysis_date=analysis_date)

@app.route('/script', methods=['GET', 'POST'])
@login_required
def script():
    script_result = ""
    if request.method == 'POST':
        tech = request.form.get('tech')
        rns = request.form.getlist('rn[]')
        srns = request.form.getlist('srn[]')
        hsns = request.form.getlist('hsn[]')
        hpns = request.form.getlist('hpn[]')
        rcns = request.form.getlist('rcn[]')
        secids = request.form.getlist('sectorid[]')
        rxnums = request.form.getlist('rxnum[]')
        txnums = request.form.getlist('txnum[]')

        lines = []
        for i in range(len(rns)):
            lines.append(f"ADD RRUCHAIN: RCN={rcns[i]}, TT=CHAIN, BM=COLD, AT=LOCALPORT, HSRN=0, HSN={hsns[i]}, HPN={hpns[i]}, CR=AUTO, USERDEFRATENEGOSW=OFF;")
            rs_mode = "GU" if "900" in tech else "UO" if "2100" in tech else "LO"
            if tech == '4g': rs_mode = "LO"
            lines.append(f"ADD RRU: CN=0, SRN={srns[i]}, SN=0, TP=TRUNK, RCN={rcns[i]}, PS=0, RT=MRRU, RS={rs_mode}, RN={rns[i]}, RXNUM={rxnums[i]}, TXNUM={txnums[i]}, MNTMODE=NORMAL, RFDCPWROFFALMDETECTSW=OFF, RFTXSIGNDETECTSW=OFF;")
            
            ant_num = rxnums[i]
            ant_str = f"ANT1CN=0, ANT1SRN={srns[i]}, ANT1SN=0, ANT1N=R0A"
            if int(ant_num) >= 2: ant_str += f", ANT2CN=0, ANT2SRN={srns[i]}, ANT2SN=0, ANT2N=R0B"
            if int(ant_num) >= 4: ant_str += f", ANT3CN=0, ANT3SRN={srns[i]}, ANT3SN=0, ANT3N=R0C, ANT4CN=0, ANT4SRN={srns[i]}, ANT4SN=0, ANT4N=R0D"
            lines.append(f"ADD SECTOR: SECTORID={secids[i]}, ANTNUM={ant_num}, {ant_str}, CREATESECTOREQM=FALSE;")
            
            ant_type_str = "ANTTYPE1=RXTX_MODE"
            if int(ant_num) >= 2: ant_type_str += ", ANTTYPE2=RXTX_MODE"
            if int(ant_num) >= 4: ant_type_str += ", ANTTYPE3=RXTX_MODE, ANTTYPE4=RXTX_MODE"
            lines.append(f"ADD SECTOREQM: SECTOREQMID={secids[i]}, SECTORID={secids[i]}, ANTCFGMODE=ANTENNAPORT, ANTNUM={ant_num}, {ant_str.replace(f'ANT1SRN={srns[i]}', 'ANT1SRN=0').replace(f'ANT2SRN={srns[i]}', 'ANT2SRN=0').replace(f'ANT3SRN={srns[i]}', 'ANT3SRN=0').replace(f'ANT4SRN={srns[i]}', 'ANT4SRN=0')}, {ant_type_str};")
            lines.append("") 

        script_result = "\n".join(lines)

    return render_page(CONTENT_TEMPLATE, title="Generate Script", active_page='script', script_result=script_result)

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '4g')
    action = request.args.get('action')
    search_query = request.args.get('cell_search', '').strip()
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    
    query = Model.query
    if search_query:
        query = query.filter(or_(Model.cell_code.ilike(f"%{search_query}%"), Model.site_code.ilike(f"%{search_query}%")))
        
    if action == 'export':
        def generate():
            yield '\ufeff'.encode('utf-8')
            cols = [c.key for c in Model.__table__.columns if c.key != 'id']
            yield (','.join(cols) + '\n').encode('utf-8')
            seen_export = set()
            for row in query.all():
                if row.cell_code not in seen_export:
                    seen_export.add(row.cell_code)
                    yield (','.join([str(getattr(row, c) or '').replace(',', ';') for c in cols]) + '\n').encode('utf-8')
        return Response(stream_with_context(generate()), mimetype='text/csv', headers={"Content-Disposition": f"attachment; filename=RF_{tech}.csv"})

    rows = query.all()
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    data = []
    seen_cells = set()
    
    for r in rows:
        if r.cell_code not in seen_cells:
            seen_cells.add(r.cell_code)
            data.append({c: getattr(r, c) for c in cols} | {'id': r.id})
            # Nếu không tìm kiếm cụ thể, chỉ hiển thị tối đa 100 trạm để web không bị lag
            if not search_query and len(data) >= 100:
                break
                
    return render_page(CONTENT_TEMPLATE, title="RF Database", active_page='rf', current_tech=tech, rf_columns=cols, rf_data=data, search_query=search_query)

@app.route('/rf/delete/<tech>/<int:id>')
@login_required
def rf_delete(tech, id):
    if current_user.role != 'admin': return redirect(url_for('rf', tech=tech))
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    db.session.delete(db.session.get(Model, id))
    db.session.commit()
    flash('Đã xóa', 'success')
    return redirect(url_for('rf', tech=tech))

@app.route('/rf/detail/<tech>/<int:id>')
@login_required
def rf_detail(tech, id):
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    return render_page(RF_DETAIL_TEMPLATE, obj=obj.__dict__, tech=tech)

@app.route('/rf/add', methods=['GET', 'POST'])
@login_required
def rf_add():
    if current_user.role != 'admin': return redirect(url_for('rf', tech=request.args.get('tech', '3g')))
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
    if current_user.role != 'admin': return redirect(url_for('rf', tech=tech))
    Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    obj = db.session.get(Model, id)
    if request.method == 'POST':
        for k,v in request.form.items(): setattr(obj, k, v)
        db.session.commit(); flash('Updated', 'success'); return redirect(url_for('rf', tech=tech))
    cols = [c.key for c in Model.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Edit RF {tech}", columns=cols, tech=tech, obj=obj.__dict__)

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
            for file in files:
                try:
                    df = pd.read_excel(file, header=None) if file.filename.endswith('.xlsx') else pd.read_csv(file, header=None)
                    header_row_idx, cell_col_idx = -1, -1
                    for i, row in df.iterrows():
                        for j, val in enumerate(row):
                            if str(val).lower().strip() in ['cell name', 'tên cell', 'cell_name']:
                                header_row_idx, cell_col_idx = i, j
                                break
                        if header_row_idx != -1: break
                        
                    if header_row_idx != -1 and cell_col_idx != -1:
                        headers = [" - ".join([str(df.iloc[i, j]).strip() for i in range(header_row_idx + 1) if str(df.iloc[i, j]).strip() not in ['nan', 'None', '']]) or f"Col_{j}" for j in range(len(df.columns))]
                        records = []
                        for i in range(header_row_idx + 1, len(df)):
                            row_data = df.iloc[i]
                            c_name = str(row_data[cell_col_idx]).strip()
                            
                            # Cải tiến logic lọc lỗi rác từ Excel
                            if not c_name or str(c_name).lower() in ['nan', 'none', 'null'] or len(str(c_name)) < 5 or str(c_name).isdigit(): 
                                continue
                            
                            try: val1 = float(row_data[cell_col_idx + 2])
                            except: val1 = 0.0
                            if math.isnan(val1): val1 = 0.0
                                
                            try: val2 = float(row_data[cell_col_idx + 3])
                            except: val2 = 0.0
                            if math.isnan(val2): val2 = 0.0
                                
                            percent, score = max(val1, val2), min(val1, val2)
                            
                            details_dict = {headers[j]: str(row_data[j]).strip() for j in range(len(headers)) if pd.notna(row_data[j]) and str(row_data[j]).strip() != 'nan'}
                            details_json = json.dumps(details_dict, ensure_ascii=False)
                            
                            records.append({'cell_name': c_name, 'week_name': week_name, 'qoe_score' if itype == 'qoe4g' else 'qos_score': score, 'qoe_percent' if itype == 'qoe4g' else 'qos_percent': percent, 'details': details_json})
                        if records:
                            db.session.bulk_insert_mappings(TargetModel, records)
                            db.session.commit()
                            flash(f'Import thành công {len(records)} dòng.', 'success')
                except Exception as e: flash(f'Lỗi: {e}', 'danger')
        else:
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
                                if itype == 'kpi4g' and 'traffic' not in clean_row and 'traffic_vol_dl' in clean_row:
                                    clean_row['traffic'] = clean_row['traffic_vol_dl']
                                records.append(clean_row)
                            if records: db.session.bulk_insert_mappings(Model, records); db.session.commit()
                        flash(f'Imported {file.filename}', 'success')
                    except Exception as e: flash(f'Error {file.filename}: {e}', 'danger')
        return redirect(url_for('import_data'))
        
    d3 = [d[0] for d in db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()]
    d4 = [d[0] for d in db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()]
    d5 = [d[0] for d in db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()]
    
    today = datetime.now()
    year, week_num, weekday = today.isocalendar()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    default_week_name = f"Tuần {week_num:02d} ({start_of_week.strftime('%d/%m')}-{end_of_week.strftime('%d/%m')})"
    
    return render_page(CONTENT_TEMPLATE, title="Data Import", active_page='import', kpi_rows=list(zip_longest(d3, d4, d5)), default_week_name=default_week_name)

@app.route('/reset-data', methods=['POST'])
@login_required
def reset_data():
    if current_user.role != 'admin':
        return redirect(url_for('index'))
    
    target = request.form.get('target')
    try:
        if target == 'rf':
            db.session.query(RF3G).delete()
            db.session.query(RF4G).delete()
            db.session.query(RF5G).delete()
            db.session.commit()
            flash('Đã reset thành công toàn bộ dữ liệu RF (3G, 4G, 5G)!', 'success')
        elif target == 'poi':
            db.session.query(POI4G).delete()
            db.session.query(POI5G).delete()
            db.session.commit()
            flash('Đã reset thành công toàn bộ dữ liệu POI (4G, 5G)!', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Đã xảy ra lỗi khi reset: {str(e)}', 'danger')
        
    return redirect(url_for('import_data'))

@app.route('/backup', methods=['POST'])
@login_required
def backup_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    selected_tables = request.form.getlist('tables')
    if not selected_tables:
        flash('No tables selected', 'warning')
        return redirect(url_for('backup_restore'))
        
    stream = BytesIO()
    models_map = {'users.csv': User, 'rf3g.csv': RF3G, 'rf4g.csv': RF4G, 'rf5g.csv': RF5G, 'poi4g.csv': POI4G, 'poi5g.csv': POI5G, 'kpi3g.csv': KPI3G, 'kpi4g.csv': KPI4G, 'kpi5g.csv': KPI5G, 'qoe_4g.csv': QoE4G, 'qos_4g.csv': QoS4G}
    
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname in selected_tables:
            if fname in models_map:
                Model = models_map[fname]
                cols = [c.key for c in Model.__table__.columns]
                data = db.session.query(Model).all()
                if not data: df = pd.DataFrame(columns=cols)
                else: df = pd.DataFrame([{c: getattr(row, c) for c in cols} for row in data])
                zf.writestr(fname, df.to_csv(index=False, encoding='utf-8-sig'))
    
    stream.seek(0)
    gc.collect()
    return send_file(stream, as_attachment=True, download_name=f'backup_{datetime.now().strftime("%Y%m%d")}.zip')

@app.route('/restore', methods=['POST'])
@login_required
def restore_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    file = request.files['file']
    if file:
        try:
            file_bytes = BytesIO(file.read())
            with zipfile.ZipFile(file_bytes) as zf:
                models = {'users.csv': User, 'rf3g.csv': RF3G, 'rf4g.csv': RF4G, 'rf5g.csv': RF5G, 'poi4g.csv': POI4G, 'poi5g.csv': POI5G, 'kpi3g.csv': KPI3G, 'kpi4g.csv': KPI4G, 'kpi5g.csv': KPI5G, 'qoe_4g.csv': QoE4G, 'qos_4g.csv': QoS4G}
                for fname in zf.namelist():
                    if fname in models:
                        Model = models[fname]
                        with zf.open(fname) as f: df = pd.read_csv(f, encoding='utf-8-sig')
                        db.session.query(Model).delete()
                        records = df.to_dict('records')
                        clean_records = [{k: (v if not pd.isna(v) else None) for k, v in r.items() if k in [c.key for c in Model.__table__.columns]} for r in records]
                        if clean_records: db.session.bulk_insert_mappings(Model, clean_records)
                db.session.commit()
                flash('Restore Success', 'success')
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
def add_user(): 
    u = User(username=request.form['username'], role=request.form['role']); u.set_password(request.form['password'])
    db.session.add(u); db.session.commit(); return redirect(url_for('manage_users'))

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
    app.run(debug=True)
